#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════
# DarkHistoryMind — Shorts Pipeline (Final Clean)
# Follows long-form process exactly, differs only in:
# - 9:16 vertical format
# - Short duration (30-40s)
# - Big karaoke captions
# ═══════════════════════════════════════════════════════
import os, sys, json, re, random, time, math
import requests, subprocess, pickle, datetime, logging
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("shorts_pipeline.log")
    ]
)
log = logging.getLogger(__name__)

# ── ENV ──────────────────────────────────────────────
GROQ_KEY          = os.environ["GROQ_KEY"]
PEXELS_KEY        = os.environ["PEXELS_KEY"]
PIXABAY_KEY       = os.environ["PIXABAY_KEY"]
GDRIVE_SECRETS_ID = os.environ["GDRIVE_SECRETS_ID"]
GDRIVE_TOKEN_ID   = os.environ["GDRIVE_TOKEN_ID"]
GDRIVE_MUSIC_ID   = os.environ.get("GDRIVE_MUSIC_ID", "")
SHEET_ID          = os.environ["SHEET_ID"]
LONG_VIDEO_TOPIC  = os.environ.get("LONG_VIDEO_TOPIC", "")
CHANNEL_NAME      = "DarkHistoryMind"

# ── VIDEO SPECS ──────────────────────────────────────
SW, SH = 1080, 1920   # 9:16 vertical
FPS    = 30

# ── CAPTION STYLE ────────────────────────────────────
CAPTION_SIZE = 100
HOOK_SIZE    = 115
STROKE_W     = 7
CENTER_Y     = 0.50
SAFE_TOP     = 0.10
SAFE_BOTTOM  = 0.25

# ── FONTS ────────────────────────────────────────────
_FONTS = [
    "/usr/share/fonts/truetype/custom/Montserrat-ExtraBold.ttf",
    "/usr/share/fonts/truetype/montserrat/Montserrat-ExtraBold.ttf",
    "/usr/share/fonts/truetype/fonts-montserrat/Montserrat-ExtraBold.ttf",
    os.path.expanduser("~/.local/share/fonts/Montserrat-ExtraBold.ttf"),
    "Montserrat-ExtraBold.ttf",
    "/usr/share/fonts/truetype/custom/Anton-Regular.ttf",
    os.path.expanduser("~/.local/share/fonts/Anton-Regular.ttf"),
    "Anton-Regular.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
]
_FC = {}

def download_fonts():
    for fname, url in [
        ("Montserrat-ExtraBold.ttf",
         "https://github.com/google/fonts/raw/main/ofl/montserrat/static/Montserrat-ExtraBold.ttf"),
        ("Anton-Regular.ttf",
         "https://github.com/google/fonts/raw/main/ofl/anton/Anton-Regular.ttf"),
    ]:
        if os.path.exists(fname):
            continue
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200 and len(r.content) > 1000:
                open(fname, "wb").write(r.content)
                log.info(f"Font: {fname}")
        except Exception as e:
            log.warning(f"Font {fname}: {e}")

def get_font(size):
    from PIL import ImageFont
    if size in _FC:
        return _FC[size]
    for p in _FONTS:
        if os.path.exists(p):
            try:
                f = ImageFont.truetype(p, size)
                _FC[size] = f
                return f
            except:
                continue
    f = ImageFont.load_default()
    _FC[size] = f
    return f

# ══════════════════════════════════════════════════════
# SECTION 1 — SCRIPT
# Clean 100-word script. No padding loops.
# ══════════════════════════════════════════════════════
def generate_script(topic):
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)

        prompt = f"""Write a YouTube Shorts dark history script.
Topic: {topic}

EXACT REQUIREMENT: full_script must be 90-110 words. Count every word.

Structure:
- hook: ONE brutal shocking statement. 8-12 words. No question.
- fact1: First dark fact. 18-22 words.
- fact2: Second darker fact. 18-22 words.
- story: The shocking core truth. 28-35 words.
- conclusion: Haunting final statement. 18-22 words.

IMPORTANT: full_script = hook + fact1 + fact2 + story + conclusion combined.
Each section must be UNIQUE — no repeated sentences anywhere.
Cold documentary tone. TRUE historical facts only.
Never start with Welcome, Today, In this video.

Return ONLY valid JSON — nothing else:
{{"hook":"","fact1":"","fact2":"","story":"","conclusion":"","full_script":""}}"""

        r    = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.85,
            max_tokens=700
        )
        text = r.choices[0].message.content.strip()
        text = text.replace("```json","").replace("```","").strip()
        data = json.loads(text[text.find('{'):text.rfind('}')+1])

        # Always rebuild full_script by joining sections — prevents duplicates
        sections = ["hook","fact1","fact2","story","conclusion"]
        built    = " ".join(data.get(s,"").strip() for s in sections if data.get(s,"").strip())
        data["full_script"] = built
        wc = len(built.split())
        log.info(f"Script: {wc} words | Hook: {data.get('hook','')[:60]}")

        # Retry if too short — up to 3 times
        for attempt in range(3):
            if wc >= 80:
                break
            log.warning(f"Script {wc} words — retry {attempt+1}")
            r2   = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role":"user","content":prompt}],
                temperature=0.9 + attempt*0.05,
                max_tokens=700
            )
            text2 = r2.choices[0].message.content.strip()
            text2 = text2.replace("```json","").replace("```","").strip()
            try:
                d2    = json.loads(text2[text2.find('{'):text2.rfind('}')+1])
                built2 = " ".join(d2.get(s,"").strip() for s in sections if d2.get(s,"").strip())
                d2["full_script"] = built2
                wc2    = len(built2.split())
                if wc2 > wc:
                    data = d2
                    wc   = wc2
                    log.info(f"Retry {attempt+1}: {wc} words")
            except Exception as e:
                log.warning(f"Retry parse: {e}")

        log.info(f"Final script: {wc} words")
        return data

    except Exception as e:
        log.error(f"Script failed: {e}")
        return None

# ══════════════════════════════════════════════════════
# SECTION 2 — VOICE
# Exactly same as long-form: communicate.save()
# Duration from ffprobe — ground truth
# ══════════════════════════════════════════════════════
def generate_voice(script_data):
    try:
        import asyncio, nest_asyncio, edge_tts
        nest_asyncio.apply()

        full_script = script_data.get("full_script", "").strip()
        if not full_script:
            log.error("Empty script")
            return False

        log.info(f"Voice: {len(full_script.split())} words, {len(full_script)} chars")

        async def _save():
            communicate = edge_tts.Communicate(
                full_script,
                voice="en-GB-ThomasNeural",
                rate="-18%",
                pitch="-10Hz"
            )
            await communicate.save("shorts_raw_voice.mp3")

        asyncio.run(_save())

        if not os.path.exists("shorts_raw_voice.mp3") or \
           os.path.getsize("shorts_raw_voice.mp3") < 1000:
            log.error("Voice file missing or too small")
            return False

        # Normalize audio — same as long-form
        from pydub import AudioSegment
        audio = AudioSegment.from_mp3("shorts_raw_voice.mp3")
        audio = audio.set_frame_rate(48000).set_sample_width(3)
        audio.export("shorts_voice.wav", format="wav")
        log.info(f"Voice saved: {len(audio)/1000:.2f}s")
        return True

    except Exception as e:
        log.error(f"Voice failed: {e}")
        return False

def get_voice_duration():
    """Get exact voice duration using ffprobe — ground truth."""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", "shorts_voice.wav"],
            capture_output=True, text=True, timeout=10
        )
        d = float(r.stdout.strip())
        log.info(f"Voice duration: {d:.3f}s")
        return d
    except Exception as e:
        log.warning(f"ffprobe failed: {e}")
    try:
        try:
            from moviepy.editor import AudioFileClip
        except ImportError:
            from moviepy import AudioFileClip
        c = AudioFileClip("shorts_voice.wav")
        d = c.duration; c.close()
        return d
    except:
        return 35.0

# ══════════════════════════════════════════════════════
# SECTION 3 — CAPTIONS (Whisper exact sync)
# Same approach as long-form but 2-3 words per group
# ══════════════════════════════════════════════════════
KEYWORDS = {
    "empire","emperor","king","queen","pharaoh","caesar","blood","death",
    "war","battle","massacre","execution","torture","secret","hidden",
    "forbidden","truth","betrayal","vanished","destroyed","murdered",
    "million","billion","ancient","medieval","roman","greek","egypt",
    "persian","mongol","viking","spartan","never","only","first","last",
    "dark","evil","brutal","savage","ruthless","feared","powerful",
    "night","centuries","single","entire","buried","erased","real",
}

def transcribe_voice():
    """Whisper tiny — same method as long-form."""
    try:
        from faster_whisper import WhisperModel
        log.info("Whisper transcribing...")
        model = WhisperModel("tiny", device="cpu", compute_type="int8")
        segments, _ = model.transcribe(
            "shorts_voice.wav",
            word_timestamps=True,
            language="en",
            beam_size=1,
            vad_filter=True,
        )
        words = []
        for seg in segments:
            if seg.words:
                for w in seg.words:
                    word = w.word.strip()
                    if word:
                        words.append({
                            "word":  word,
                            "start": round(w.start, 3),
                            "end":   round(w.end, 3),
                        })
        log.info(f"Whisper: {len(words)} words")
        return words
    except ImportError:
        log.warning("faster-whisper not available")
        return []
    except Exception as e:
        log.warning(f"Whisper error: {e}")
        return []

def build_captions(script_data, total_dur):
    hook         = script_data.get("hook","").strip()
    word_timings = transcribe_voice()
    captions     = []

    if word_timings:
        log.info("Building captions from Whisper")
        n_hook = len(hook.split())

        # Hook caption — covers hook words exactly
        if n_hook > 0 and len(word_timings) >= n_hook:
            h_start = word_timings[0]["start"]
            h_end   = max(word_timings[min(n_hook-1,len(word_timings)-1)]["end"], h_start+1.5)
            captions.append({
                "text": hook.upper(), "start": h_start,
                "end": min(h_end, total_dur-0.1),
                "is_hook": True, "color": "yellow",
            })
            remaining = word_timings[n_hook:]
        else:
            remaining = word_timings

        # Body: 2-3 words per caption — pure Whisper timestamps
        i = 0
        while i < len(remaining):
            gs    = random.choices([2,3,3], weights=[25,50,25])[0]
            group = remaining[i:i+gs]
            if not group: break

            g_start = group[0]["start"]
            g_end   = group[-1]["end"]
            g_words = [w["word"] for w in group]

            if g_end - g_start < 0.4:
                g_end = g_start + 0.4
            if i + gs < len(remaining):
                g_end = min(g_end, remaining[i+gs]["start"] - 0.02)
            g_end = min(g_end, total_dur - 0.05)
            if g_end <= g_start:
                i += gs; continue

            has_kw = any(w.lower().strip(".,!?;:") in KEYWORDS for w in g_words)
            captions.append({
                "text":    " ".join(g_words).upper(),
                "start":   round(g_start, 3),
                "end":     round(g_end, 3),
                "is_hook": False,
                "color":   "yellow" if (has_kw and random.random()<0.35) else "white",
            })
            i += gs

    else:
        # Fallback: distribute script words evenly
        log.warning("Whisper unavailable — estimating captions")
        words    = script_data.get("full_script","").split()
        wdur     = total_dur / max(len(words),1)
        hook_end = len(hook.split()) * wdur
        captions.append({
            "text": hook.upper(), "start": 0.0,
            "end": min(hook_end, total_dur-0.1),
            "is_hook": True, "color": "yellow",
        })
        t = hook_end + 0.05
        i = len(hook.split())
        while i < len(words) and t < total_dur - 0.2:
            gs    = random.choices([2,3,3], weights=[25,50,25])[0]
            group = words[i:i+gs]
            if not group: break
            cd    = max(0.4, len(group)*wdur)
            has_kw= any(w.lower().strip(".,!?;:") in KEYWORDS for w in group)
            captions.append({
                "text":    " ".join(group).upper(),
                "start":   round(t,3),
                "end":     round(min(t+cd,total_dur-0.05),3),
                "is_hook": False,
                "color":   "yellow" if (has_kw and random.random()<0.35) else "white",
            })
            i += gs; t += cd + 0.02

    log.info(f"Captions: {len(captions)}")
    return captions

# ══════════════════════════════════════════════════════
# SECTION 4 — ASSETS
# ══════════════════════════════════════════════════════
SCENE_KW = {
    "hook":       ["ancient ruins dramatic dark","mysterious fortress medieval dark"],
    "fact1":      ["ancient manuscript historical sepia","old parchment candlelight"],
    "fact2":      ["roman ruins cinematic","greek statue dramatic shadow"],
    "story":      ["battle painting historical","medieval war scene dramatic"],
    "climax":     ["dramatic ancient empire dark","mysterious shadows historical"],
    "conclusion": ["sunset ancient ruins melancholic","foggy ancient landscape dark"],
}

def clean_topic(t):
    for p in ["The Psychology of","The Dark Truth About","The Secret Life of",
              "The Truth Behind","The Real Story of","The Hidden Truth About",
              "The Dark History of","Why Did","The Real History of",
              "The Hidden History of","The Dark Story of","The Real Reason"]:
        t = t.replace(p,"").strip()
    return t

def crop_916(img):
    from PIL import Image
    W, H  = img.size
    tw    = int(H * 9 / 16)
    if tw <= W:
        l   = (W-tw)//2
        img = img.crop((l,0,l+tw,H))
    else:
        th  = int(W*16/9)
        ni  = Image.new("RGB",(W,th),(8,5,3))
        ni.paste(img,(0,(th-H)//2))
        img = ni
    return img.resize((SW,SH),Image.LANCZOS)

def fetch_image(query, idx):
    from PIL import Image
    import io
    for source in ["pexels","pixabay","ai"]:
        try:
            if source == "pexels":
                r = requests.get("https://api.pexels.com/v1/search",
                    headers={"Authorization":PEXELS_KEY},
                    params={"query":query,"per_page":5,"orientation":"landscape"},timeout=20)
                photos = r.json().get("photos",[])
                if photos:
                    url  = random.choice(photos)["src"].get("original") or random.choice(photos)["src"]["large2x"]
                    data = requests.get(url,timeout=40).content
                    img  = Image.open(io.BytesIO(data)).convert("RGB")
                    if img.width >= 1000 or img.height >= 1000:
                        path = f"shorts_assets/img_{idx}.jpg"
                        crop_916(img).save(path,quality=95)
                        return path
            elif source == "pixabay":
                r    = requests.get(
                    f"https://pixabay.com/api/?key={PIXABAY_KEY}&q={query.replace(' ','+')}"
                    f"&image_type=photo&orientation=horizontal&per_page=5&order=popular&min_width=1280",
                    timeout=20)
                hits = r.json().get("hits",[])
                if hits:
                    url  = random.choice(hits).get("largeImageURL","")
                    data = requests.get(url,timeout=40).content
                    img  = Image.open(io.BytesIO(data)).convert("RGB")
                    path = f"shorts_assets/img_{idx}.jpg"
                    crop_916(img).save(path,quality=95)
                    return path
            else:
                prompt = f"ultra detailed dark cinematic historical {query} dramatic sepia no text no watermark"
                url    = (f"https://image.pollinations.ai/prompt/{requests.utils.quote(prompt)}"
                          f"?width=1080&height=1920&nologo=true&seed={idx*7}")
                r = requests.get(url,timeout=90)
                if r.status_code==200 and len(r.content)>10000:
                    img  = Image.open(io.BytesIO(r.content)).convert("RGB").resize((SW,SH),Image.LANCZOS)
                    path = f"shorts_assets/img_{idx}.jpg"
                    img.save(path,quality=95)
                    return path
        except Exception as e:
            log.warning(f"{source}: {e}")
    return None

def fetch_assets(topic):
    os.makedirs("shorts_assets",exist_ok=True)
    assets  = []
    idx     = 0
    clean_t = clean_topic(topic)
    for section in ["hook","fact1","fact2","story","story","climax","climax","conclusion"]:
        kw   = random.choice(SCENE_KW.get(section,SCENE_KW["hook"])) + f" {clean_t}"
        path = fetch_image(kw,idx)
        if path: assets.append(path)
        idx += 1; time.sleep(0.3)
    while len(assets) < 6:
        kw   = random.choice(SCENE_KW["story"]) + f" {clean_t}"
        path = fetch_image(kw,idx)
        if path: assets.append(path)
        idx += 1
    log.info(f"Assets: {len(assets)}")
    return assets

# ══════════════════════════════════════════════════════
# SECTION 5 — COLOR GRADING (same as long-form)
# ══════════════════════════════════════════════════════
def grade(frame):
    img = frame.astype(np.float32)/255.0
    img = np.clip(img*0.78,0,1)
    img = np.clip(img+1.25*0.10*(img-0.5),0,1)
    img[:,:,0] = np.clip(img[:,:,0]+0.05,0,1)
    img[:,:,2] = np.clip(img[:,:,2]-0.05,0,1)
    gray  = 0.299*img[:,:,0]+0.587*img[:,:,1]+0.114*img[:,:,2]
    sepia = np.stack([np.clip(gray*1.08,0,1),np.clip(gray*0.86,0,1),np.clip(gray*0.67,0,1)],axis=2)
    img   = np.clip(img*0.70+sepia*0.30,0,1)
    H,W   = img.shape[:2]
    Y,X   = np.ogrid[:H,:W]
    dist  = np.sqrt(((X-W/2)/(W/2))**2+((Y-H/2)/(H/2))**2)
    img   = np.clip(img*(1-np.clip(dist*0.60,0,0.65))[:,:,np.newaxis],0,1)
    img   = np.clip(img+np.random.normal(0,0.013,img.shape).astype(np.float32),0,1)
    return (img*255).astype(np.uint8)

# ══════════════════════════════════════════════════════
# SECTION 6 — RENDER CAPTIONS
# ══════════════════════════════════════════════════════
def render_caption(frame_rgb, t, captions):
    from PIL import Image, ImageDraw
    img  = Image.fromarray(frame_rgb).convert("RGBA")
    W, H = img.size

    cap = next((c for c in captions if c["start"] <= t <= c["end"]),None)
    if cap is None:
        return np.array(img.convert("RGB"))

    cap_t = t - cap["start"]
    ap    = min(1.0, cap_t/0.20)
    fp    = min(1.0, cap_t/0.12)
    scale = 0.85+0.15*(1-(1-ap)**3)
    alpha = int(255*fp)

    base  = HOOK_SIZE if cap.get("is_hook") else CAPTION_SIZE
    fsize = max(60, int(base*scale))
    font  = get_font(fsize)

    tcol  = (255,230,0,alpha) if (cap.get("is_hook") or cap.get("color")=="yellow") else (245,245,245,alpha)
    scol  = (0,0,0,alpha)

    text  = cap["text"]
    lines, cur = [], ""
    td = ImageDraw.Draw(Image.new("RGBA",(W,H)))
    for w in text.split():
        test = f"{cur} {w}".strip()
        if td.textbbox((0,0),test,font=font)[2] > W*0.68:
            if cur: lines.append(cur)
            cur = w
        else: cur = test
    if cur: lines.append(cur)

    lh  = fsize+14
    th  = len(lines)*lh
    ys  = int(H*CENTER_Y)-th//2
    ys  = max(int(H*SAFE_TOP),min(int(H*(1-SAFE_BOTTOM))-th,ys))

    ov = Image.new("RGBA",(W,H),(0,0,0,0))
    od = ImageDraw.Draw(ov)
    for li,line in enumerate(lines):
        bb = od.textbbox((0,0),line,font=font)
        x  = (W-(bb[2]-bb[0]))//2
        y  = ys+li*lh
        for dx in range(-STROKE_W,STROKE_W+1,2):
            for dy in range(-STROKE_W,STROKE_W+1,2):
                if dx==0 and dy==0: continue
                od.text((x+dx,y+dy),line,font=font,fill=scol)
        od.text((x,y),line,font=font,fill=tcol)
    img = Image.alpha_composite(img,ov)
    return np.array(img.convert("RGB"))

# ══════════════════════════════════════════════════════
# SECTION 7 — ASSEMBLE VIDEO
# Video duration = exact voice duration from ffprobe
# Audio mux: voice + music with explicit -t trim
# Loop bug fix: video frames and audio both trimmed to
# same exact duration — no extra frames, no loop
# ══════════════════════════════════════════════════════
def zoom_frame(frame,t,clip_dur):
    from PIL import Image
    scale  = 1.0+0.06*(t/max(clip_dur,0.1))
    img    = Image.fromarray(frame)
    nW,nH  = int(SW*scale),int(SH*scale)
    img    = img.resize((nW,nH),Image.LANCZOS)
    x,y    = (nW-SW)//2,(nH-SH)//2
    return np.array(img.crop((x,y,x+SW,y+SH)))

def dust(frame,seed=0):
    import cv2
    np.random.seed(seed%500)
    ov = np.zeros((SH,SW),dtype=np.float32)
    for _ in range(5):
        cv2.circle(ov,(np.random.randint(0,SW),np.random.randint(0,SH)),
                   np.random.randint(1,3),float(np.random.uniform(0.15,0.40)),-1)
    ov = cv2.GaussianBlur(ov,(5,5),0)
    ff = frame.astype(np.float32)/255.0
    return (np.clip(ff+np.stack([ov]*3,axis=2)*0.08,0,1)*255).astype(np.uint8)

def assemble(assets, captions, total_dur, output_file):
    import cv2
    log.info(f"Assembling {total_dur:.3f}s | {len(assets)} images")

    frames_data = []
    for path in assets:
        img = cv2.imread(path)
        if img is None: continue
        img = cv2.cvtColor(img,cv2.COLOR_BGR2RGB)
        img = cv2.resize(img,(SW,SH),interpolation=cv2.INTER_LANCZOS4)
        frames_data.append(grade(img))
    if not frames_data:
        log.error("No frames"); return False

    clip_dur = max(2.5,min(5.0,total_dur/len(frames_data)))
    n_clips  = math.ceil(total_dur/clip_dur)
    log.info(f"{n_clips} clips x {clip_dur:.2f}s")

    writer = cv2.VideoWriter("shorts_temp.mp4",cv2.VideoWriter_fourcc(*'mp4v'),FPS,(SW,SH))
    gf     = 0

    for ci in range(n_clips):
        if gf/FPS >= total_dur: break
        curr = frames_data[ci%len(frames_data)]
        for fi in range(int(clip_dur*FPS)):
            tg = gf/FPS
            if tg >= total_dur: break
            frame = zoom_frame(curr,fi/FPS,clip_dur)
            frame = dust(frame,seed=gf)
            frame = render_caption(frame,tg,captions)
            # Fade out last 0.5s only
            if tg > total_dur-0.5:
                fade  = max(0.0,(total_dur-tg)/0.5)
                frame = (frame.astype(np.float32)*fade).astype(np.uint8)
            writer.write(cv2.cvtColor(frame,cv2.COLOR_RGB2BGR))
            gf += 1
        # Hard cut between clips
        if ci < n_clips-1 and gf/FPS < total_dur:
            writer.write(np.zeros((SH,SW,3),dtype=np.uint8))
            gf += 1

    writer.release()
    log.info(f"Frames written: {gf} ({gf/FPS:.3f}s)")

    # ── AUDIO MUX ────────────────────────────────────
    # KEY: use -t total_dur on ALL inputs to prevent any loop
    # voice: play once, stop at total_dur
    # music: loop but hard stop at total_dur
    music = "background.mp3" if os.path.exists("background.mp3") else None

    if music:
        cmd = [
            "ffmpeg","-y",
            "-i","shorts_temp.mp4",
            "-stream_loop","-1","-i","shorts_voice.wav",  # -stream_loop -1 never loops WAV
            "-stream_loop","-1","-i",music,
            "-filter_complex",
            "[1:a]volume=1.0,atrim=0={dur},asetpts=PTS-STARTPTS[v];"
            "[2:a]volume=0.08,atrim=0={dur},asetpts=PTS-STARTPTS[m];"
            "[v][m]amix=inputs=2:duration=first[aout]".format(dur=total_dur),
            "-map","0:v","-map","[aout]",
            "-c:v","libx264","-crf","18","-preset","fast","-profile:v","high",
            "-c:a","aac","-b:a","192k","-ar","48000",
            "-t",str(total_dur),
            "-movflags","+faststart","-pix_fmt","yuv420p",
            output_file
        ]
    else:
        cmd = [
            "ffmpeg","-y",
            "-i","shorts_temp.mp4",
            "-i","shorts_voice.wav",
            "-c:v","libx264","-crf","18","-preset","fast","-profile:v","high",
            "-c:a","aac","-b:a","192k","-ar","48000",
            "-t",str(total_dur),
            "-movflags","+faststart","-pix_fmt","yuv420p",
            output_file
        ]

    result = subprocess.run(cmd,capture_output=True,text=True)
    if result.returncode != 0:
        log.error(f"FFmpeg failed:\n{result.stderr[-600:]}")
        import shutil; shutil.copy("shorts_temp.mp4",output_file)
        return False

    size_mb = os.path.getsize(output_file)/1024/1024
    log.info(f"Video: {output_file} ({size_mb:.1f}MB)")

    if os.path.exists("shorts_temp.mp4"):
        os.remove("shorts_temp.mp4")
    return True

# ══════════════════════════════════════════════════════
# SECTION 8 — THUMBNAIL
# ══════════════════════════════════════════════════════
def gen_thumbnail(topic,hook):
    from PIL import Image,ImageDraw
    import io
    log.info("Generating thumbnail...")
    clean_t = clean_topic(topic)
    thumb   = None
    for seed in [int(datetime.datetime.now().timestamp())%100000,
                 int(datetime.datetime.now().timestamp())%100000+3333]:
        try:
            p   = (f"ultra dramatic dark cinematic {clean_t} ancient historical "
                   f"sepia shadows vertical portrait no text no watermark")
            url = (f"https://image.pollinations.ai/prompt/{requests.utils.quote(p)}"
                   f"?width=1080&height=1920&nologo=true&seed={seed}")
            r   = requests.get(url,timeout=90)
            if r.status_code==200 and len(r.content)>5000:
                thumb = Image.open(io.BytesIO(r.content)).convert("RGB").resize((SW,SH),Image.LANCZOS)
                break
        except: continue
    if thumb is None: thumb = Image.new("RGB",(SW,SH),(8,5,3))
    ov = Image.new("RGBA",(SW,SH),(0,0,0,0))
    od = ImageDraw.Draw(ov)
    for y in range(SH//2,SH):
        od.line([(0,y),(SW,y)],fill=(0,0,0,int(210*(y-SH//2)/(SH//2))))
    thumb = Image.alpha_composite(thumb.convert("RGBA"),ov).convert("RGB")
    draw  = ImageDraw.Draw(thumb)
    font  = get_font(88)
    words = hook.upper().split()
    lines,cur = [],""
    for w in words:
        test = f"{cur} {w}".strip()
        if draw.textbbox((0,0),test,font=font)[2]>SW*0.85:
            if cur: lines.append(cur)
            cur=w
        else: cur=test
    if cur: lines.append(cur)
    ys = SH-400-len(lines)*100//2
    for li,line in enumerate(lines):
        bb = draw.textbbox((0,0),line,font=font)
        x  = (SW-(bb[2]-bb[0]))//2
        y  = ys+li*100
        for dx in range(-7,8,3):
            for dy in range(-7,8,3):
                draw.text((x+dx,y+dy),line,font=font,fill=(0,0,0,255))
        draw.text((x,y),line,font=font,fill=(255,230,0,255))
    draw.text((40,60),CHANNEL_NAME,font=get_font(42),fill=(212,175,55,255))
    draw.line([(0,0),(SW,0)],fill=(212,175,55),width=5)
    thumb.save("shorts_thumbnail.jpg",quality=95)
    return "shorts_thumbnail.jpg"

# ══════════════════════════════════════════════════════
# SECTION 9 — SEO
# ══════════════════════════════════════════════════════
def gen_seo(topic,script_data,hook):
    try:
        from groq import Groq
        client  = Groq(api_key=GROQ_KEY)
        clean_t = clean_topic(topic)
        prompt  = f"""Viral YouTube Shorts SEO for dark history.
Topic: {topic} | Hook: {hook}
Return ONLY valid JSON:
{{"title":"max 60 chars, shocking, historical name, one emoji, ends #Shorts",
  "description":"2-3 punchy lines",
  "tags":["15 tags"]}}"""
        r    = client.chat.completions.create(model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}],temperature=0.85,max_tokens=400)
        text = r.choices[0].message.content.strip().replace("```json","").replace("```","").strip()
        data = json.loads(text[text.find('{'):text.rfind('}')+1])
        slug = clean_t.replace(' ','').lower()
        data["description"] += (
            f"\n\n#Shorts #darkhistory #hiddenhistory #historyshorts "
            f"#{slug} #historyfacts #secrethistory #ancienthistory "
            f"#darkhistorymind #untoldhistory #historydocumentary #mindblowing"
            f"\n\n🔔 Subscribe to {CHANNEL_NAME}\n👍 Like if this shocked you"
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

# ══════════════════════════════════════════════════════
# SECTION 10 — UPLOAD + SCHEDULE
# ══════════════════════════════════════════════════════
# Pipeline runs ~16:00 UTC Day 0 (after long form finishes at 14:30 UTC)
# Short #1 → 19:30 UTC Day 0  = 1:00 AM IST gap day       (days_ahead=0)
# Short #2 → 14:30 UTC Day 1  = 8:00 PM IST gap day       (days_ahead=1)
# Short #3 → 19:00 UTC Day 1  = 12:30 AM IST new vid day  (days_ahead=1, later hour)
SLOT_SCHEDULE = {
    0: {"hour":19,"minute":30,"days_ahead":0,"label":"1:00 AM IST — gap day"},
    1: {"hour":14,"minute":30,"days_ahead":1,"label":"8:00 PM IST — gap day"},
    2: {"hour":19,"minute": 0,"days_ahead":1,"label":"12:30 AM IST — new video day"},
}

def get_schedule(slot):
    now  = datetime.datetime.utcnow()
    cfg  = SLOT_SCHEDULE.get(slot,SLOT_SCHEDULE[0])
    base = now.replace(hour=cfg["hour"],minute=cfg["minute"],second=0,microsecond=0)
    t    = base + datetime.timedelta(days=cfg["days_ahead"])
    if t <= now:
        t += datetime.timedelta(days=1)
    ist = t + datetime.timedelta(hours=5,minutes=30)
    log.info(f"Slot {slot}: {t.strftime('%Y-%m-%d %H:%M')} UTC = {ist.strftime('%d %b %I:%M %p')} IST | {cfg['label']}")
    return t.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def get_yt():
    from googleapiclient.discovery import build
    import google.auth.transport.requests
    creds = None
    if os.path.exists("youtube_token.pkl"):
        with open("youtube_token.pkl","rb") as f: creds = pickle.load(f)
    if creds and hasattr(creds,"expired") and creds.expired and hasattr(creds,"refresh_token") and creds.refresh_token:
        try: creds.refresh(google.auth.transport.requests.Request())
        except: creds = None
    if not creds or (hasattr(creds,"valid") and not creds.valid):
        log.error("YouTube token invalid"); return None
    return build("youtube","v3",credentials=creds,cache_discovery=False)

def upload(video_file,seo,thumbnail,offset=0):
    from googleapiclient.http import MediaFileUpload
    yt = get_yt()
    if not yt: return None,None
    pub  = get_schedule(offset)
    body = {
        "snippet":{"title":seo["title"],"description":seo["description"],
                   "tags":seo.get("tags",[]),"categoryId":"27","defaultLanguage":"en"},
        "status":{"privacyStatus":"private","publishAt":pub,"selfDeclaredMadeForKids":False}
    }
    try:
        media   = MediaFileUpload(video_file,mimetype="video/mp4",resumable=True,chunksize=5*1024*1024)
        request = yt.videos().insert(part="snippet,status",body=body,media_body=media)
        response = None
        while response is None:
            st,response = request.next_chunk()
            if st: log.info(f"  {int(st.progress()*100)}%")
        vid = response["id"]; url = f"https://youtube.com/watch?v={vid}"
        log.info(f"Uploaded: {url}")
        try:
            yt.thumbnails().set(videoId=vid,media_body=MediaFileUpload(thumbnail)).execute()
            log.info("Thumbnail set")
        except Exception as e: log.warning(f"Thumb: {e}")
        return vid,url
    except Exception as e:
        log.error(f"Upload failed: {e}"); return None,None

def dl_drive(fid,path):
    try:
        url = f"https://drive.google.com/uc?export=download&id={fid}"
        s   = requests.Session(); r = s.get(url,stream=True,timeout=60)
        tok = next((v for k,v in r.cookies.items() if "download_warning" in k),None)
        if tok: r = s.get(f"{url}&confirm={tok}",stream=True,timeout=60)
        with open(path,"wb") as f:
            for chunk in r.iter_content(32768):
                if chunk: f.write(chunk)
        return os.path.getsize(path)>=100
    except Exception as e:
        log.error(f"Drive: {e}"); return False

def setup_auth():
    for name,fid in [("client_secrets.json",GDRIVE_SECRETS_ID),
                     ("youtube_token.pkl",GDRIVE_TOKEN_ID)]:
        if not os.path.exists(name):
            log.info(f"Downloading {name}...")
            dl_drive(fid,name)
    # Always refresh music
    if GDRIVE_MUSIC_ID:
        log.info("Downloading background.mp3...")
        ok = dl_drive(GDRIVE_MUSIC_ID,"background.mp3")
        log.info(f"background.mp3: {'ready' if ok else 'failed'}")

def get_sheet_client():
    try:
        import gspread, google.auth.transport.requests
        creds = None
        if os.path.exists("youtube_token.pkl"):
            with open("youtube_token.pkl","rb") as f: creds = pickle.load(f)
        if not creds: return None
        if hasattr(creds,"expired") and creds.expired and hasattr(creds,"refresh_token") and creds.refresh_token:
            try: creds.refresh(google.auth.transport.requests.Request())
            except Exception as e: log.warning(f"Sheet refresh: {e}")
        if hasattr(creds,"valid") and not creds.valid: return None
        gc = gspread.authorize(creds)
        return gc
    except Exception as e:
        log.warning(f"Sheet client: {e}"); return None

def update_sheet(topic,url,title,num):
    try:
        gc = get_sheet_client()
        if not gc: log.warning("Sheet skipped — no auth"); return
        sh = gc.open_by_key(SHEET_ID)
        try: ws = sh.get_worksheet(1)
        except:
            ws = sh.add_worksheet(title="Shorts",rows=1000,cols=6)
            ws.append_row(["Topic","Short#","Title","URL","Uploaded","Status"])
        ws.append_row([topic,num,title,url,
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),"scheduled"])
        log.info(f"Sheet: short #{num} logged")
    except Exception as e:
        log.warning(f"Sheet: {e}")

def get_related_topics(long_topic,n=3):
    try:
        from groq import Groq
        c      = Groq(api_key=GROQ_KEY)
        prompt = f"""Long-form video: {long_topic}
Generate {n} related Shorts topics — different dark angles of same topic.
Each: shocking, specific, 5-10 words. No repeats. No teasers.
Return ONLY JSON array: ["Topic 1","Topic 2","Topic 3"]"""
        r    = c.chat.completions.create(model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}],temperature=0.9,max_tokens=200)
        text = r.choices[0].message.content.strip().replace("```json","").replace("```","").strip()
        tops = json.loads(text[text.find('['):text.rfind(']')+1])
        while len(tops) < n:
            tops.append(f"The Hidden Truth About {clean_topic(long_topic)}")
        log.info(f"Topics: {tops[:n]}")
        return tops[:n]
    except Exception as e:
        log.warning(f"Topics: {e}")
        ct = clean_topic(long_topic)
        return [f"The Dark Secret of {ct}",f"The Brutal Reality of {ct}",f"The Hidden Truth About {ct}"][:n]

# ══════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════
def run_short(topic,num,offset):
    log.info(f"\n{'='*50}\nSHORT #{num} — {topic}\n{'='*50}")

    # 1. Script
    script = generate_script(topic)
    if not script: return None
    hook = script.get("hook","The truth was buried for centuries.")
    log.info(f"Script OK: {len(script.get('full_script','').split())} words")

    # 2. Voice
    ok = generate_voice(script)
    if not ok or not os.path.exists("shorts_voice.wav"):
        log.error("ABORT: voice failed"); return None
    dur = get_voice_duration()
    log.info(f"Duration: {dur:.3f}s")

    # 3. Captions
    captions = build_captions(script,dur)
    log.info(f"Captions: {len(captions)}")

    # 4. Assets
    assets = fetch_assets(topic)
    if not assets: return None

    # 5. Assemble
    safe = re.sub(r'[^\w\s]','',topic).replace(' ','_')[:40]
    out  = f"short_{num}_{safe}.mp4"
    ok   = assemble(assets,captions,dur,out)
    if not ok or not os.path.exists(out):
        log.error("ABORT: assembly failed"); return None
    log.info(f"Video: {out} ({os.path.getsize(out)//1024//1024}MB)")

    # 6. Thumbnail + SEO + Upload
    thumb = gen_thumbnail(topic,hook)
    seo   = gen_seo(topic,script,hook)
    _,url = upload(out,seo,thumb,offset)

    if url:
        update_sheet(topic,url,seo["title"],num)
        log.info(f"Short #{num} done: {url}")

    # Cleanup
    for f in [out,"shorts_raw_voice.mp3","shorts_voice.wav","shorts_thumbnail.jpg"]:
        if os.path.exists(f): os.remove(f)
    for f in os.listdir("shorts_assets"):
        try: os.remove(f"shorts_assets/{f}")
        except: pass
    return url

def main():
    log.info("="*55+"\nDarkHistoryMind SHORTS\n"+"="*55)
    download_fonts()
    setup_auth()
    os.makedirs("shorts_assets",exist_ok=True)

    long_topic = LONG_VIDEO_TOPIC.strip() or "The Dark Truth About The Roman Empire"
    log.info(f"Long-form topic: {long_topic}")

    topics = get_related_topics(long_topic,n=3)
    results = []
    for i,(topic,slot) in enumerate(zip(topics,[0,1,2]),1):
        log.info(f"\n{'='*40}\nSHORT {i}/3 — {SLOT_SCHEDULE[slot]['label']}\n{'='*40}")
        url = run_short(topic,num=i,offset=slot)
        results.append(url)
        if i < 3: time.sleep(15)

    log.info("="*55+"\nSHORTS COMPLETE")
    for i,url in enumerate(results,1):
        log.info(f"Short #{i}: {url or 'FAILED'}")
    if not any(results): sys.exit(1)

if __name__ == "__main__":
    main()
