#!/usr/bin/env python3
# shorts_pipeline.py — DarkHistoryMind Shorts Automation
# Format: 1080x1920 | 9:16 | 30 FPS | 20-40 seconds
# Style: Big karaoke captions | Fast cuts | Dark cinematic grade
import os, sys, json, re, random, time, math
import requests, subprocess, pickle, datetime, logging
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("shorts_pipeline.log")]
)
log = logging.getLogger(__name__)

GROQ_KEY          = os.environ["GROQ_KEY"]
PEXELS_KEY        = os.environ["PEXELS_KEY"]
PIXABAY_KEY       = os.environ["PIXABAY_KEY"]
GDRIVE_SECRETS_ID = os.environ["GDRIVE_SECRETS_ID"]
GDRIVE_TOKEN_ID   = os.environ["GDRIVE_TOKEN_ID"]
SHEET_ID          = os.environ["SHEET_ID"]
LONG_VIDEO_TOPIC  = os.environ.get("LONG_VIDEO_TOPIC", "")
TEST_MODE         = os.environ.get("TEST_MODE", "false").lower() == "true"
CHANNEL_NAME      = "DarkHistoryMind"

SW, SH            = 1080, 1920
FPS               = 30
BITRATE           = "14M"
CAPTION_SIZE      = 105
HOOK_SIZE         = 118
STROKE_W          = 7
CENTER_Y          = 0.50
SAFE_TOP          = 0.10
SAFE_BOTTOM       = 0.25

_FONTS = [
    "/usr/share/fonts/truetype/montserrat/Montserrat-ExtraBold.ttf",
    "/usr/share/fonts/truetype/fonts-montserrat/Montserrat-ExtraBold.ttf",
    os.path.expanduser("~/.local/share/fonts/Montserrat-ExtraBold.ttf"),
    "Montserrat-ExtraBold.ttf",
    os.path.expanduser("~/.local/share/fonts/Anton-Regular.ttf"),
    "Anton-Regular.ttf",
    "/usr/share/fonts/truetype/msttcorefonts/Arial_Black.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
]
_FC = {}

def download_fonts():
    for fname, url in [
        ("Montserrat-ExtraBold.ttf","https://github.com/google/fonts/raw/main/ofl/montserrat/static/Montserrat-ExtraBold.ttf"),
        ("Anton-Regular.ttf","https://github.com/google/fonts/raw/main/ofl/anton/Anton-Regular.ttf"),
    ]:
        if os.path.exists(fname): continue
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200 and len(r.content) > 1000:
                open(fname,"wb").write(r.content)
                log.info(f"Font downloaded: {fname}")
        except Exception as e:
            log.warning(f"Font {fname}: {e}")

def get_font(size):
    from PIL import ImageFont
    if size in _FC: return _FC[size]
    for p in _FONTS:
        if os.path.exists(p):
            try:
                f = ImageFont.truetype(p, size)
                _FC[size] = f
                return f
            except: continue
    f = ImageFont.load_default()
    _FC[size] = f
    return f

def download_music():
    if os.path.exists("shorts_music.mp3"):
        log.info("Music exists")
        return True
    try:
        r    = requests.get(
            f"https://pixabay.com/api/?key={PIXABAY_KEY}&q=epic+cinematic+history&media_type=music&per_page=10",
            timeout=20
        )
        hits = r.json().get("hits", [])
        for hit in hits:
            au = hit.get("audio",{}).get("url","")
            if not au: continue
            ar = requests.get(au, timeout=30)
            if ar.status_code==200 and len(ar.content)>10000:
                open("shorts_music.mp3","wb").write(ar.content)
                log.info("Music downloaded")
                return True
    except Exception as e:
        log.warning(f"Music: {e}")
    return False

# ── SECTION 1: SCRIPT ──────────────────────────────────────────
def generate_script(topic):
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)
        prompt = f"""Write a viral YouTube Shorts dark history script.
Topic: {topic}
Length: 30-35 seconds spoken (80-90 words total)

Structure:
[HOOK] 2sec — ONE brutal statement. Max 8 words. No questions.
[FACT1] 4sec — First dark fact. 15-18 words.
[FACT2] 4sec — Second fact, escalate tension. 15-18 words.
[STORY] 8sec — Dark core. 30-35 words.
[CLIMAX] 7sec — Most shocking reveal. 25-30 words.
[CONCLUSION] 10sec — Haunting final fact. 25-30 words.

Rules: cold documentary tone, all TRUE facts, no filler, no Welcome/Today/In this video.

Return ONLY valid JSON:
{{"hook":"","fact1":"","fact2":"","story":"","climax":"","conclusion":"","full_script":""}}"""
        r    = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}],
            temperature=0.9, max_tokens=800
        )
        text = r.choices[0].message.content.strip()
        text = text.replace("```json","").replace("```","").strip()
        data = json.loads(text[text.find('{'):text.rfind('}')+1])
        log.info(f"Hook: {data.get('hook','')}")
        return data
    except Exception as e:
        log.error(f"Script: {e}")
        return None

# ── SECTION 2: VOICE ───────────────────────────────────────────
def generate_voice(script_data):
    try:
        import asyncio, nest_asyncio, edge_tts
        nest_asyncio.apply()
        async def _voice():
            c = edge_tts.Communicate(script_data["full_script"], voice="en-GB-ThomasNeural", rate="+5%", pitch="-8Hz")
            await c.save("shorts_raw_voice.mp3")
        asyncio.run(_voice())
        from pydub import AudioSegment
        audio = AudioSegment.from_mp3("shorts_raw_voice.mp3").set_frame_rate(48000)
        audio = audio.apply_gain(-audio.dBFS + (-14))
        dur   = len(audio) / 1000
        audio.export("shorts_voice.wav", format="wav")
        log.info(f"Voice: {dur:.1f}s")
        return dur
    except Exception as e:
        log.error(f"Voice: {e}")
        return 0.0

def get_audio_dur():
    try:
        from moviepy.editor import AudioFileClip
        c = AudioFileClip("shorts_voice.wav"); d = c.duration; c.close(); return d
    except: return 35.0

# ── SECTION 3: CAPTIONS ────────────────────────────────────────
KEYWORDS = {
    "empire","emperor","king","queen","pharaoh","caesar","blood","death",
    "war","battle","massacre","execution","torture","secret","hidden",
    "forbidden","truth","betrayal","vanished","destroyed","murdered",
    "million","billion","ancient","medieval","roman","greek","egypt",
    "persian","mongol","viking","spartan","never","only","first","last",
    "dark","evil","brutal","savage","ruthless","feared","powerful",
}

def build_captions(script_data, total_dur):
    words    = script_data.get("full_script","").split()
    hook     = script_data.get("hook","")
    if not words: return []
    word_dur = total_dur / max(len(words),1)
    captions = []
    hook_dur = min(2.0, len(hook.split()) * word_dur * 1.2)
    captions.append({"text":hook.upper(),"start":0.2,"end":hook_dur,"is_hook":True,"emphasis":True,"color":"yellow"})
    i = len(hook.split())
    t = hook_dur + 0.1
    while i < len(words) and t < total_dur - 0.5:
        gs    = random.choices([2,3,3,4], weights=[15,45,25,15])[0]
        group = words[i:i+gs]
        if not group: break
        cd    = max(0.8, min(1.5, len(group)*word_dur*random.uniform(0.95,1.15)))
        has_k = any(w.lower().strip(".,!?;:") in KEYWORDS for w in group)
        emph  = has_k and random.random() < 0.30
        captions.append({
            "text":" ".join(group).upper(),
            "start":t,"end":min(t+cd,total_dur-0.2),
            "is_hook":False,"emphasis":emph,"color":"yellow" if emph else "white"
        })
        i += gs; t += cd + 0.05
    log.info(f"Captions: {len(captions)}")
    return captions

# ── SECTION 4: ASSETS ──────────────────────────────────────────
SCENE_KW = {
    "hook":       ["ancient ruins dramatic dark","mysterious fortress medieval"],
    "fact1":      ["ancient manuscript historical sepia","old parchment candlelight"],
    "fact2":      ["roman ruins cinematic","greek statue dramatic shadow"],
    "story":      ["battle painting historical","medieval war dramatic"],
    "climax":     ["dramatic ancient ruins dark","mysterious shadows historical"],
    "conclusion": ["sunset ancient ruins melancholic","foggy ancient landscape"],
}

def clean_topic(t):
    for p in ["The Psychology of","The Dark Truth About","The Secret Life of",
              "The Truth Behind","The Real Story of","The Hidden Truth About",
              "The Dark History of","Why Did","The Real History of",
              "The Hidden History of","The Dark Story of"]:
        t = t.replace(p,"").strip()
    return t

def crop_916(img):
    from PIL import Image
    W,H = img.size
    tw  = int(H*9/16)
    if tw <= W:
        l = (W-tw)//2
        img = img.crop((l,0,l+tw,H))
    else:
        th  = int(W*16/9)
        ni  = Image.new("RGB",(W,th),(8,5,3))
        ni.paste(img,(0,(th-H)//2))
        img = ni
    return img.resize((SW,SH), Image.LANCZOS)

def fetch_image(query, idx):
    from PIL import Image
    import io
    for attempt in ["pexels","pixabay","ai"]:
        try:
            if attempt=="pexels":
                r = requests.get("https://api.pexels.com/v1/search",
                    headers={"Authorization":PEXELS_KEY},
                    params={"query":query,"per_page":3,"orientation":"landscape"},timeout=20)
                photos = r.json().get("photos",[])
                if photos:
                    url  = random.choice(photos)["src"]["large2x"]
                    data = requests.get(url,timeout=30).content
                    img  = Image.open(io.BytesIO(data)).convert("RGB")
                    path = f"shorts_assets/img_{idx}.jpg"
                    crop_916(img).save(path, quality=92)
                    return path
            elif attempt=="pixabay":
                r    = requests.get(
                    f"https://pixabay.com/api/?key={PIXABAY_KEY}&q={query.replace(' ','+')}"
                    f"&image_type=photo&orientation=horizontal&per_page=3&order=popular",timeout=20)
                hits = r.json().get("hits",[])
                if hits:
                    url  = random.choice(hits).get("largeImageURL","")
                    data = requests.get(url,timeout=30).content
                    img  = Image.open(io.BytesIO(data)).convert("RGB")
                    path = f"shorts_assets/img_{idx}.jpg"
                    crop_916(img).save(path, quality=92)
                    return path
            else:
                prompt = f"dark cinematic historical {query} dramatic sepia no text no watermark"
                url    = (f"https://image.pollinations.ai/prompt/{requests.utils.quote(prompt)}"
                          f"?width=1080&height=1920&nologo=true&seed={idx*7}")
                r = requests.get(url, timeout=90)
                if r.status_code==200 and len(r.content)>5000:
                    img  = Image.open(io.BytesIO(r.content)).convert("RGB").resize((SW,SH),Image.LANCZOS)
                    path = f"shorts_assets/img_{idx}.jpg"
                    img.save(path, quality=92)
                    return path
        except Exception as e:
            log.warning(f"{attempt}: {e}")
    return None

def fetch_assets(topic):
    os.makedirs("shorts_assets", exist_ok=True)
    assets  = []
    idx     = 0
    clean_t = clean_topic(topic)
    for section in ["hook","fact1","fact2","story","story","climax","climax","conclusion","conclusion"]:
        kw   = random.choice(SCENE_KW.get(section,SCENE_KW["hook"])) + f" {clean_t}"
        path = fetch_image(kw, idx)
        if path:
            assets.append(path)
            log.info(f"  ✅ {section}")
        idx += 1; time.sleep(0.3)
    while len(assets) < 6:
        kw   = random.choice(SCENE_KW["hook"]) + f" {clean_t}"
        path = fetch_image(kw, idx)
        if path: assets.append(path)
        idx += 1
    log.info(f"Assets: {len(assets)}")
    return assets

# ── SECTION 5: GRADE ───────────────────────────────────────────
def grade(frame):
    img = frame.astype(np.float32)/255.0
    img = np.clip(img*0.75, 0, 1)
    img = np.clip(img + 1.3*0.12*(img-0.5), 0, 1)
    img[:,:,0] = np.clip(img[:,:,0]+0.06,0,1)
    img[:,:,2] = np.clip(img[:,:,2]-0.06,0,1)
    gray  = 0.299*img[:,:,0]+0.587*img[:,:,1]+0.114*img[:,:,2]
    sepia = np.stack([np.clip(gray*1.08,0,1),np.clip(gray*0.86,0,1),np.clip(gray*0.67,0,1)],axis=2)
    img   = np.clip(img*0.65+sepia*0.35, 0, 1)
    H,W   = img.shape[:2]
    Y,X   = np.ogrid[:H,:W]
    dist  = np.sqrt(((X-W/2)/(W/2))**2+((Y-H/2)/(H/2))**2)
    img   = np.clip(img*(1-np.clip(dist*0.65,0,0.70))[:,:,np.newaxis], 0, 1)
    img   = np.clip(img+np.random.normal(0,0.015,img.shape).astype(np.float32), 0, 1)
    return (img*255).astype(np.uint8)

# ── SECTION 6: RENDER FRAME ────────────────────────────────────
def render_frame(frame_rgb, t, captions):
    from PIL import Image, ImageDraw
    img  = Image.fromarray(frame_rgb).convert("RGBA")
    W, H = img.size
    cap  = next((c for c in captions if c["start"]<=t<=c["end"]), None)
    if cap is None: return np.array(img.convert("RGB"))

    cap_t = t - cap["start"]
    ap    = min(1.0, cap_t/0.25)
    fp    = min(1.0, cap_t/0.15)
    scale = 0.85 + 0.15*(1-(1-ap)**3)
    alpha = int(255*fp)

    base  = HOOK_SIZE if cap.get("is_hook") else CAPTION_SIZE
    if cap.get("emphasis"): base = int(base*1.05)
    fsize = max(60, int(base*scale))
    font  = get_font(fsize)

    tcol  = (255,230,0,alpha) if (cap.get("is_hook") or cap.get("emphasis")) else (242,242,242,alpha)
    scol  = (0,0,0,alpha)

    text  = cap["text"]
    words = text.split()
    lines, cur = [], ""
    td = ImageDraw.Draw(Image.new("RGBA",(W,H)))
    for w in words:
        test = f"{cur} {w}".strip()
        bb   = td.textbbox((0,0),test,font=font)
        if bb[2]-bb[0] > W*0.68:
            if cur: lines.append(cur)
            cur = w
        else: cur = test
    if cur: lines.append(cur)

    lh    = fsize+12
    th    = len(lines)*lh
    ys    = int(H*CENTER_Y)-th//2
    ys    = max(int(H*SAFE_TOP), min(int(H*(1-SAFE_BOTTOM))-th, ys))

    ov    = Image.new("RGBA",(W,H),(0,0,0,0))
    od    = ImageDraw.Draw(ov)
    for li, line in enumerate(lines):
        bb = od.textbbox((0,0),line,font=font)
        tw = bb[2]-bb[0]
        x  = (W-tw)//2
        y  = ys+li*lh
        for dx in range(-STROKE_W, STROKE_W+1, 2):
            for dy in range(-STROKE_W, STROKE_W+1, 2):
                if dx==0 and dy==0: continue
                od.text((x+dx,y+dy),line,font=font,fill=scol)
        od.text((x,y),line,font=font,fill=tcol)
    img = Image.alpha_composite(img,ov)
    return np.array(img.convert("RGB"))

# ── SECTION 7: ASSEMBLY ────────────────────────────────────────
def zoom_frame(frame, t, cd):
    from PIL import Image
    sc  = 1.0 + 0.08*(t/cd)
    img = Image.fromarray(frame).resize((int(SW*sc),int(SH*sc)),Image.LANCZOS)
    x,y = (img.width-SW)//2,(img.height-SH)//2
    return np.array(img.crop((x,y,x+SW,y+SH)))

def dust(frame, seed=0):
    import cv2
    np.random.seed(seed%500)
    ov = np.zeros((SH,SW),dtype=np.float32)
    for _ in range(6):
        cv2.circle(ov,(np.random.randint(0,SW),np.random.randint(0,SH)),
                   np.random.randint(1,3),float(np.random.uniform(0.2,0.5)),-1)
    ov = cv2.GaussianBlur(ov,(5,5),0)
    ff = frame.astype(np.float32)/255.0
    return (np.clip(ff+np.stack([ov]*3,axis=2)*0.10,0,1)*255).astype(np.uint8)

def xfade(f1,f2,p):
    s = p*p*(3-2*p)
    return np.clip(f1.astype(np.float32)*(1-s)+f2.astype(np.float32)*s,0,255).astype(np.uint8)

def assemble(assets, captions, total_dur, output_file):
    import cv2
    log.info("Assembling...")
    frames_data = []
    for path in assets:
        img = cv2.imread(path)
        if img is None: continue
        img = cv2.cvtColor(img,cv2.COLOR_BGR2RGB)
        img = cv2.resize(img,(SW,SH))
        frames_data.append(grade(img))
    if not frames_data:
        log.error("No frames"); return False

    cd     = max(1.5, min(4.0, total_dur/len(frames_data)))
    n_clip = math.ceil(total_dur/cd)
    log.info(f"{n_clip} clips x {cd:.1f}s")

    writer = cv2.VideoWriter("shorts_temp.mp4",cv2.VideoWriter_fourcc(*'mp4v'),FPS,(SW,SH))
    gf     = 0
    for ci in range(n_clip):
        if gf/FPS >= total_dur: break
        curr   = frames_data[ci%len(frames_data)]
        next_i = frames_data[(ci+1)%len(frames_data)]
        for fi in range(int(cd*FPS)):
            tl = fi/FPS; tg = gf/FPS
            if tg >= total_dur: break
            f = zoom_frame(curr, tl, cd)
            f = dust(f, seed=gf)
            f = render_frame(f, tg, captions)
            if tl > cd-0.4 and ci<n_clip-1 and random.random()<0.20:
                p = (tl-(cd-0.4))/0.4
                f = xfade(f, zoom_frame(next_i,0.0,cd), min(1.0,p))
            if ci==0 and fi<int(0.3*FPS):
                f = (f.astype(np.float32)*(fi/(0.3*FPS))).astype(np.uint8)
            if tg > total_dur-0.5:
                f = (f.astype(np.float32)*max(0,(total_dur-tg)/0.5)).astype(np.uint8)
            writer.write(cv2.cvtColor(f,cv2.COLOR_RGB2BGR))
            gf += 1
    writer.release()
    log.info("Frames done")

    vf    = "shorts_voice.wav"
    mf    = "shorts_music.mp3" if os.path.exists("shorts_music.mp3") else None
    if mf:
        af  = "[0:a]volume=1.0[v];[1:a]volume=0.18,aloop=0:size=2e+09[m];[v][m]amix=inputs=2:duration=first[aout]"
        cmd = ["ffmpeg","-y","-i","shorts_temp.mp4","-i",vf,"-i",mf,
               "-filter_complex",af,"-map","0:v","-map","[aout]",
               "-c:v","libx264","-preset","fast","-b:v",BITRATE,
               "-c:a","aac","-b:a","192k","-t",str(total_dur),"-movflags","+faststart",output_file]
    else:
        cmd = ["ffmpeg","-y","-i","shorts_temp.mp4","-i",vf,
               "-c:v","libx264","-preset","fast","-b:v",BITRATE,
               "-c:a","aac","-b:a","192k","-t",str(total_dur),"-movflags","+faststart",output_file]
    res = subprocess.run(cmd,capture_output=True,text=True)
    if res.returncode != 0:
        log.error(f"FFmpeg: {res.stderr[-300:]}")
        import shutil; shutil.copy("shorts_temp.mp4",output_file)
    else:
        log.info(f"Muxed: {output_file}")
    if os.path.exists("shorts_temp.mp4"): os.remove("shorts_temp.mp4")
    return True

# ── SECTION 8: THUMBNAIL ───────────────────────────────────────
def gen_thumbnail(topic, hook):
    from PIL import Image, ImageDraw
    import io
    log.info("Thumbnail...")
    thumb = None
    clean_t = clean_topic(topic)
    for seed in [42,123,777]:
        try:
            p   = f"ultra dramatic dark cinematic {clean_t} ancient historical sepia shadows vertical portrait no text no watermark"
            url = f"https://image.pollinations.ai/prompt/{requests.utils.quote(p)}?width=1080&height=1920&nologo=true&seed={seed}"
            r   = requests.get(url,timeout=90)
            if r.status_code==200 and len(r.content)>5000:
                thumb = Image.open(io.BytesIO(r.content)).convert("RGB").resize((SW,SH),Image.LANCZOS)
                break
        except: continue
    if thumb is None: thumb = Image.new("RGB",(SW,SH),(8,5,3))
    ov = Image.new("RGBA",(SW,SH),(0,0,0,0))
    od = ImageDraw.Draw(ov)
    for y in range(SH//2,SH):
        od.line([(0,y),(SW,y)],fill=(0,0,0,int(200*(y-SH//2)/(SH//2))))
    thumb = Image.alpha_composite(thumb.convert("RGBA"),ov).convert("RGB")
    draw  = ImageDraw.Draw(thumb)
    font  = get_font(90)
    words = hook.upper().split()
    lines, cur = [], ""
    for w in words:
        test = f"{cur} {w}".strip()
        bb   = draw.textbbox((0,0),test,font=font)
        if bb[2]-bb[0]>SW*0.85:
            if cur: lines.append(cur)
            cur=w
        else: cur=test
    if cur: lines.append(cur)
    ys = SH-380-len(lines)*100//2
    for li,line in enumerate(lines):
        bb = draw.textbbox((0,0),line,font=font)
        x  = (SW-(bb[2]-bb[0]))//2
        y  = ys+li*100
        for dx in range(-6,7,3):
            for dy in range(-6,7,3):
                draw.text((x+dx,y+dy),line,font=font,fill=(0,0,0,255))
        draw.text((x,y),line,font=font,fill=(255,230,0,255))
    draw.text((40,60),CHANNEL_NAME,font=get_font(42),fill=(212,175,55,255))
    draw.line([(0,0),(SW,0)],fill=(212,175,55),width=5)
    thumb.save("shorts_thumbnail.jpg",quality=95)
    return "shorts_thumbnail.jpg"

# ── SECTION 9: SEO ─────────────────────────────────────────────
def gen_seo(topic, script_data, hook):
    try:
        from groq import Groq
        c = Groq(api_key=GROQ_KEY)
        ct = clean_topic(topic)
        prompt = f"""Viral YouTube Shorts SEO for dark history.
Topic: {topic} | Hook: {hook}
Facts: {script_data.get('fact1','')} {script_data.get('fact2','')}

Return ONLY valid JSON:
{{"title":"max 60 chars, shocking, historical name, one emoji, ends #Shorts","description":"Line1 hook. Line2 fact tease. Line3 subscribe CTA.","tags":["15 tags mix broad+specific"]}}
Title example: "THIS Empire Vanished In ONE Night 💀 #Shorts" """
        r    = c.chat.completions.create(model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}],temperature=0.85,max_tokens=500)
        text = r.choices[0].message.content.strip().replace("```json","").replace("```","").strip()
        data = json.loads(text[text.find('{'):text.rfind('}')+1])
        slug = ct.replace(' ','').lower()
        data["description"] += (
            f"\n\n#Shorts #darkhistory #hiddenhistory #historyshorts #darkfacts "
            f"#{slug} #historyfacts #secrethistory #ancienthistory #historymystery "
            f"#darkhistorymind #untoldhistory #historydocumentary #viralhistory #mindblowing"
            f"\n\n🔔 Subscribe to {CHANNEL_NAME}\n👍 Like if this shocked you\n💬 Comment below"
        )
        log.info(f"Title: {data.get('title','')}")
        return data
    except Exception as e:
        log.warning(f"SEO: {e}")
        return {"title":f"🔴 {hook[:50]} #Shorts",
                "description":f"{hook}\n\n#Shorts #darkhistory #history",
                "tags":["Shorts","darkhistory","history","hiddenhistory","historyfacts",
                        "darkfacts","ancienthistory","secrethistory","historymystery",
                        "documentary","mindblowing","viral","historybuff","darkhistorymind","shorts"]}

# ── SECTION 10: UPLOAD ─────────────────────────────────────────
PEAK_UTC = [(1,30),(7,30),(13,30),(14,30)]  # 7AM/1PM/7PM/8PM IST

def get_schedule(offset=0):
    now  = datetime.datetime.utcnow()
    cands = []
    for h,m in PEAK_UTC:
        t = now.replace(hour=h,minute=m,second=0,microsecond=0)
        if t <= now: t += datetime.timedelta(days=1)
        cands.append(t)
    cands.sort()
    return cands[offset%len(cands)].strftime("%Y-%m-%dT%H:%M:%S.000Z")

def get_yt():
    from googleapiclient.discovery import build
    import google.auth.transport.requests
    creds = None
    if os.path.exists("youtube_token.pkl"):
        with open("youtube_token.pkl","rb") as f: creds = pickle.load(f)
    if creds and creds.expired and creds.refresh_token:
        try: creds.refresh(google.auth.transport.requests.Request())
        except: creds = None
    if not creds or not creds.valid:
        log.error("Token invalid"); return None
    return build("youtube","v3",credentials=creds,cache_discovery=False)

def upload(video_file, seo, thumbnail, offset=0):
    from googleapiclient.http import MediaFileUpload
    yt = get_yt()
    if not yt: return None,None

    if TEST_MODE:
        # TEST MODE: upload as private, no schedule, instant review
        log.info("TEST MODE — uploading as Private (no schedule)")
        body = {
            "snippet":{"title":f"[TEST] {seo['title']}","description":seo["description"],
                       "tags":seo.get("tags",[]),"categoryId":"27","defaultLanguage":"en"},
            "status":{"privacyStatus":"private","selfDeclaredMadeForKids":False}
        }
    else:
        pub  = get_schedule(offset)
        log.info(f"Schedule: {pub}")
        body = {
            "snippet":{"title":seo["title"],"description":seo["description"],
                       "tags":seo.get("tags",[]),"categoryId":"27","defaultLanguage":"en"},
            "status":{"privacyStatus":"private","publishAt":pub,"selfDeclaredMadeForKids":False}
        }
    try:
        media = MediaFileUpload(video_file,mimetype="video/mp4",resumable=True,chunksize=5*1024*1024)
        req   = yt.videos().insert(part="snippet,status",body=body,media_body=media)
        resp  = None
        while resp is None:
            st,resp = req.next_chunk()
            if st: log.info(f"  {int(st.progress()*100)}%")
        vid = resp["id"]; url = f"https://youtube.com/watch?v={vid}"
        log.info(f"Uploaded: {url}")
        if TEST_MODE:
            log.info("TEST MODE — review at: https://studio.youtube.com")
        try:
            yt.thumbnails().set(videoId=vid,media_body=MediaFileUpload(thumbnail)).execute()
            log.info("Thumbnail set")
        except Exception as e: log.warning(f"Thumb: {e}")
        return vid,url
    except Exception as e:
        log.error(f"Upload: {e}"); return None,None

def dl_drive(fid, path):
    try:
        url = f"https://drive.google.com/uc?export=download&id={fid}"
        s   = requests.Session(); r = s.get(url,stream=True,timeout=60)
        tok = next((v for k,v in r.cookies.items() if "download_warning" in k), None)
        if tok: r = s.get(f"{url}&confirm={tok}",stream=True,timeout=60)
        with open(path,"wb") as f:
            for chunk in r.iter_content(32768):
                if chunk: f.write(chunk)
        return os.path.getsize(path)>=100
    except Exception as e:
        log.error(f"Drive: {e}"); return False

def setup_auth():
    for name,fid in [("client_secrets.json",GDRIVE_SECRETS_ID),("youtube_token.pkl",GDRIVE_TOKEN_ID)]:
        if not os.path.exists(name):
            log.info(f"Downloading {name}..."); dl_drive(fid,name)

def update_sheet(topic, url, title, num):
    try:
        import gspread
        from google.oauth2.service_account import ServiceAccountCredentials
        creds = ServiceAccountCredentials.from_json_keyfile_name("client_secrets.json",
            ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"])
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)
        try: ws = sh.get_worksheet(1)
        except:
            ws = sh.add_worksheet(title="Shorts",rows=1000,cols=6)
            ws.append_row(["Topic","Short#","Title","URL","Uploaded","Status"])
        ws.append_row([topic,num,title,url,datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),"scheduled"])
        log.info("Sheet updated")
    except Exception as e: log.warning(f"Sheet: {e}")

def get_related_topics(long_topic, n=2):
    try:
        from groq import Groq
        c = Groq(api_key=GROQ_KEY)
        prompt = f"""Main long-form topic: {long_topic}
Generate {n} related YouTube Shorts topics — different dark angles of same topic.
Each: shocking, specific, 5-10 words max.
Return ONLY JSON array of {n} strings: ["Topic 1","Topic 2"]"""
        r    = c.chat.completions.create(model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}],temperature=0.9,max_tokens=150)
        text = r.choices[0].message.content.strip().replace("```json","").replace("```","").strip()
        tops = json.loads(text[text.find('['):text.rfind(']')+1])
        log.info(f"Topics: {tops}")
        return tops[:n]
    except Exception as e:
        log.warning(f"Topics: {e}")
        ct = clean_topic(long_topic)
        return [f"The Secret Death of {ct}", f"The Dark Side of {ct}"]

# ── MAIN ───────────────────────────────────────────────────────
def run_short(topic, num, offset):
    log.info(f"\n{'='*50}\nSHORT #{num} — {topic}\n{'='*50}")

    script = generate_script(topic)
    if not script: return None
    hook   = script.get("hook", f"The truth about {topic} was buried.")

    dur = generate_voice(script)
    if dur < 5: dur = get_audio_dur()
    dur = max(20.0, min(40.0, dur))
    log.info(f"Duration: {dur:.1f}s")

    captions = build_captions(script, dur)
    assets   = fetch_assets(topic)
    if not assets: return None

    safe = re.sub(r'[^\w\s]','',topic).replace(' ','_')[:40]
    out  = f"short_{num}_{safe}.mp4"
    ok   = assemble(assets, captions, dur, out)
    if not ok or not os.path.exists(out): return None
    log.info(f"Video: {out} ({os.path.getsize(out)//1024//1024}MB)")

    thumb = gen_thumbnail(topic, hook)
    seo   = gen_seo(topic, script, hook)
    _,url = upload(out, seo, thumb, offset)

    if url:
        update_sheet(topic, url, seo["title"], num)
        log.info(f"Short #{num}: {url}")
        log.info(f"Title: {seo['title']}")

    # In TEST_MODE keep the video file so GitHub Actions can upload it as artifact
    if TEST_MODE:
        log.info(f"TEST MODE — video kept for artifact: {out}")
        for f in ["shorts_thumbnail.jpg","shorts_raw_voice.mp3","shorts_voice.wav"]:
            if os.path.exists(f): os.remove(f)
    else:
        for f in [out,"shorts_thumbnail.jpg","shorts_raw_voice.mp3","shorts_voice.wav"]:
            if os.path.exists(f): os.remove(f)
    for f in os.listdir("shorts_assets"):
        try: os.remove(f"shorts_assets/{f}")
        except: pass

    return url

def main():
    log.info("="*50+"\nDarkHistoryMind SHORTS Pipeline\n"+"="*50)
    if TEST_MODE:
        log.info("*** TEST MODE — 1 short, private upload, no schedule ***")

    download_fonts()
    setup_auth()
    download_music()
    os.makedirs("shorts_assets", exist_ok=True)

    long_topic = LONG_VIDEO_TOPIC.strip() or "The Dark Truth About The Roman Empire"
    log.info(f"Long topic: {long_topic}")

    # TEST_MODE: produce only 1 short; production: 2
    n_shorts = 1 if TEST_MODE else 2
    topics   = get_related_topics(long_topic, n=n_shorts)

    results = []
    for i, topic in enumerate(topics, 1):
        url = run_short(topic, num=i, offset=i-1)
        results.append(url)
        if i < len(topics): time.sleep(10)

    log.info("="*50+"\nSHORTS COMPLETE")
    for i,url in enumerate(results,1):
        log.info(f"Short #{i}: {url or 'FAILED'}")
    if TEST_MODE:
        log.info("Review your Short at: https://studio.youtube.com (Content > Videos > Private)")
    if not any(results): sys.exit(1)

if __name__ == "__main__":
    main()
