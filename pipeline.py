#!/usr/bin/env python3
# ═══════════════════════════════════════════════════
# DarkHistoryMind — Full YouTube Automation Pipeline
# Runs every 2 days via GitHub Actions
# Sections: Topic → Script → Voice → Captions →
#           Assets → Grade → Assemble → Upload
# ═══════════════════════════════════════════════════
import os, sys, json, re, random, time, requests
import subprocess, pickle, datetime, logging
import numpy as np

# ── LOGGING ──────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log")
    ]
)
log = logging.getLogger(__name__)

# ── ENV VARIABLES ─────────────────────────────────────
GROQ_KEY          = os.environ["GROQ_KEY"]
PEXELS_KEY        = os.environ["PEXELS_KEY"]
PIXABAY_KEY       = os.environ["PIXABAY_KEY"]
HF_TOKEN          = os.environ.get("HF_TOKEN","")
GDRIVE_MUSIC_ID   = os.environ["GDRIVE_MUSIC_ID"]
GDRIVE_SECRETS_ID = os.environ["GDRIVE_SECRETS_ID"]
GDRIVE_TOKEN_ID   = os.environ["GDRIVE_TOKEN_ID"]
SHEET_ID          = os.environ["SHEET_ID"]
CHANNEL_NAME      = "DarkHistoryMind"
SCENE_ORDER       = ["mystery","explanation","insight","reflection"]
FPS               = 20
W, H              = 1920, 1080
CLIP_DUR_VIDEO    = 10.0
CLIP_DUR_IMAGE    = 6.0
TRANS_DUR         = 1.2
SCHEDULE_HOUR_UTC = 14
SCHEDULE_MINUTE_UTC = 30

SCENE_TARGETS = {
    "mystery":     {"videos": 6, "images": 6},
    "explanation": {"videos": 5, "images": 5},
    "insight":     {"videos": 5, "images": 5},
    "reflection":  {"videos": 4, "images": 4},
}

TOPIC_BANK = [
    "The Psychology of Napoleon Bonaparte",
    "Why Did The Roman Empire Really Fall",
    "The Dark Truth About The Egyptian Pharaohs",
    "The Secret Life of Nikola Tesla",
    "The Hidden Truth About The Black Death",
    "The Real Story of Jack The Ripper",
    "The Dark History of The Vatican",
    "The Psychology of Adolf Hitler",
    "The Truth Behind The Bermuda Triangle",
    "The Secret Society of The Freemasons",
    "The Dark Truth About The Crusades",
    "The Real Reason Rome Burned",
    "The Psychology of Genghis Khan",
    "The Hidden History of The Silk Road",
    "The Dark Truth About Medieval Torture",
    "The Secret Wars of The Catholic Church",
    "The Real Story of Cleopatra",
    "The Dark Psychology of Julius Caesar",
    "The Hidden Truth About The Viking Age",
    "The Real History of The Ottoman Empire",
    "The Dark Truth About The Spanish Inquisition",
    "The Psychology of Alexander The Great",
    "The Secret History of Ancient Rome",
    "The Hidden Truth About The Mongol Empire",
    "The Real Story of The Knights Templar",
    "The Dark History of Ancient Egypt",
    "The Psychology of Vlad The Impaler",
    "The Hidden Truth About The Renaissance",
    "The Real Story of The Hundred Years War",
    "The Dark Truth About Ancient Greece",
    "The Secret History of The Byzantine Empire",
    "The Real Psychology of Nero",
    "The Hidden Truth About The Fall of Constantinople",
    "The Dark History of The Plague of Justinian",
    "The Psychology of Attila The Hun",
    "The Real Story of The Persian Empire",
    "The Hidden Truth About The Trojan War",
    "The Dark Psychology of Caligula",
    "The Real History of The Spartan Warriors",
    "The Hidden Truth About Ancient Babylon",
    "The Dark Story of The Library of Alexandria",
    "The Real History of The Aztec Empire",
    "The Psychology of Hannibal Barca",
    "The Hidden Truth About The Maya Civilization",
    "The Dark History of The Inca Empire",
    "The Real Story of Marco Polo",
    "The Hidden Truth About The Age of Exploration",
    "The Dark Psychology of King Henry VIII",
    "The Real History of The French Revolution",
    "The Hidden Truth About Napoleon Exile",
    "The Dark Truth About The Salem Witch Trials",
    "The Real Psychology of Rasputin",
    "The Hidden Truth About The Russian Revolution",
    "The Dark History of The Opium Wars",
    "The Real Story of The Titanic Conspiracy",
    "The Hidden Truth About World War One",
    "The Dark Psychology of Joseph Stalin",
    "The Real History of The CIA Mind Control",
    "The Hidden Truth About The Cold War",
    "The Dark Truth About Ancient Rome",
    "The Real Story of The Medici Family",
]

# ══════════════════════════════════════════════════════
# UTILITIES
# ══════════════════════════════════════════════════════
def download_from_drive(file_id, save_path):
    try:
        url     = f"https://drive.google.com/uc?export=download&id={file_id}"
        session = requests.Session()
        r       = session.get(url, stream=True, timeout=60)
        token   = None
        for k, v in r.cookies.items():
            if "download_warning" in k:
                token = v
        if token:
            r = session.get(
                f"{url}&confirm={token}",
                stream=True, timeout=60
            )
        with open(save_path,"wb") as f:
            for chunk in r.iter_content(32768):
                if chunk:
                    f.write(chunk)
        size = os.path.getsize(save_path)
        if size < 100:
            os.remove(save_path)
            return False
        return True
    except Exception as e:
        log.error(f"Drive download failed: {e}")
        return False

def setup_permanent_files():
    log.info("Setting up permanent files...")
    files = {
        "background.mp3":      GDRIVE_MUSIC_ID,
        "client_secrets.json": GDRIVE_SECRETS_ID,
        "youtube_token.pkl":   GDRIVE_TOKEN_ID,
    }
    for name, fid in files.items():
        if os.path.exists(name):
            log.info(f"  ✅ {name} exists")
            continue
        log.info(f"  Downloading {name}...")
        ok = download_from_drive(fid, name)
        log.info(f"  {'✅' if ok else '❌'} {name}")

def clean_topic(topic):
    for p in [
        "The Psychology of","The Dark Truth About",
        "The Secret Life of","The Truth Behind",
        "The Real Story of","The Hidden Truth About",
        "The Dark History of","Why Did",
        "The Real History of","The Hidden History of",
        "The Dark Story of","The Real Psychology of",
        "The Dark Psychology of","The Real Reason",
        "The Secret History of","The Secret Wars of",
        "The Secret Society of",
    ]:
        topic = topic.replace(p,"").strip()
    return topic

# ══════════════════════════════════════════════════════
# SECTION 1 — TOPIC RESEARCH
# ══════════════════════════════════════════════════════
def get_used_topics():
    try:
        url  = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:json"
        r    = requests.get(url, timeout=15)
        text = r.text
        data = json.loads(text[text.find('{'):text.rfind('}')+1])
        rows = data.get("table",{}).get("rows",[])
        used = []
        for row in rows[1:]:
            cells = row.get("c",[])
            if cells and cells[0] and cells[0].get("v"):
                used.append(cells[0]["v"].strip().lower())
        log.info(f"Used topics: {len(used)}")
        return used
    except Exception as e:
        log.warning(f"Sheet read failed: {e}")
        return []

def select_topic(used):
    available = [t for t in TOPIC_BANK
                 if t.strip().lower() not in used]
    if not available:
        available = TOPIC_BANK.copy()
    topic = random.choice(available)
    log.info(f"Topic: {topic}")
    return topic

def get_facts(topic):
    try:
        import wikipedia
        search  = clean_topic(topic)
        results = wikipedia.search(search, results=3)
        if not results:
            return ""
        page  = wikipedia.page(results[0])
        sents = page.summary.split('. ')
        return '. '.join(sents[:15])
    except Exception as e:
        log.warning(f"Wikipedia failed: {e}")
        return f"Historical facts about {topic}"

def generate_hook(topic, facts):
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)
        prompt = f"""Write ONE hook sentence for a dark history YouTube video.
Topic: {topic}
Facts: {facts[:300]}
Rules: max 12 words, shocking statement, no questions, dark tone.
Output only the sentence."""
        r    = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}],
            temperature=0.9, max_tokens=50
        )
        hook = r.choices[0].message.content.strip()
        hook = hook.replace('"','').replace("'","").strip()
        log.info(f"Hook: {hook}")
        return hook
    except Exception as e:
        log.warning(f"Hook failed: {e}")
        return f"The truth about {topic} was hidden for centuries."

def save_topic_to_sheet(topic):
    try:
        import gspread
        from google.oauth2.service_account import ServiceAccountCredentials
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            "client_secrets.json", scope
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)
        ws = sh.get_worksheet(0)
        ws.append_row([
            topic,
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
            "pending",
            "rendering"
        ])
        log.info("Topic saved to sheet")
    except Exception as e:
        log.warning(f"Sheet save failed: {e}")

# ══════════════════════════════════════════════════════
# SECTION 2 — SCRIPT GENERATION
# ══════════════════════════════════════════════════════
def generate_script(topic, facts):
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)
        prompt = f"""You are writing a dark history YouTube documentary script.
Topic: {topic}
Facts: {facts[:1000]}

Write a compelling 900-1000 word documentary script.
Rules:
- Cold, factual, documentary thriller tone
- First sentence must be a brutal shocking fact
- Never start with Welcome or Today
- No headers or sections
- Use the facts provided
- Build tension throughout
- End with a haunting conclusion

Output only the script."""
        r      = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}],
            temperature=0.9, max_tokens=2500
        )
        script = r.choices[0].message.content.strip()
        with open("script.txt","w") as f:
            f.write(script)
        log.info(f"Script: {len(script)} chars")
        return script
    except Exception as e:
        log.error(f"Script failed: {e}")
        return ""

# ══════════════════════════════════════════════════════
# SECTION 3 — VOICEOVER
# ══════════════════════════════════════════════════════
def generate_voice(script):
    try:
        import asyncio, nest_asyncio
        import edge_tts
        nest_asyncio.apply()

        async def make_voice():
            communicate = edge_tts.Communicate(
                script,
                voice="en-GB-ThomasNeural",
                rate="-18%",
                pitch="-10Hz"
            )
            await communicate.save("raw_voice.mp3")

        asyncio.run(make_voice())
        log.info("Raw voice saved")

        # Audio processing
        from pydub import AudioSegment
        import pyloudnorm as pyln

        audio = AudioSegment.from_mp3("raw_voice.mp3")
        audio = audio.set_frame_rate(48000).set_sample_width(3)

        # Export to WAV
        audio.export("voiceover.wav", format="wav")
        log.info("Voiceover saved")
        return True
    except Exception as e:
        log.error(f"Voice failed: {e}")
        return False

# ══════════════════════════════════════════════════════
# SECTION 4 — CAPTIONS
# ══════════════════════════════════════════════════════
def get_audio_duration():
    try:
        from moviepy.editor import AudioFileClip
        clip = AudioFileClip("voiceover.wav")
        dur  = clip.duration
        clip.close()
        return dur
    except:
        return 420.0

def generate_captions(script, total_duration, hook):
    try:
        from groq import Groq
        client      = Groq(api_key=GROQ_KEY)
        num_caps    = max(20, int(total_duration / 10))
        prompt = f"""Create {num_caps} caption moments for a dark history video.
Duration: {total_duration:.0f} seconds
Script: {script[:2000]}

Space evenly every 8-12 seconds from start to finish.
Rules: max 10 words each, dark dramatic tone, mix white(80%) and gold(20%).

Return ONLY JSON array:
[{{"text":"Caption","time":5.0,"color":"white","size":"large"}}]"""

        r    = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}],
            temperature=0.8, max_tokens=4000
        )
        text = r.choices[0].message.content.strip()
        text = text.replace("```json","").replace("```","").strip()
        data = json.loads(text[text.find('['):text.rfind(']')+1])

        captions = []
        for item in data:
            t = float(item.get("time",0))
            if item.get("text") and t < total_duration:
                captions.append({
                    "text":  item["text"].strip(),
                    "start": t,
                    "end":   min(t+4.5, total_duration-0.5),
                    "color": item.get("color","white"),
                    "size":  item.get("size","large")
                })

        captions.sort(key=lambda x: x["start"])

        # Add hook at start
        captions.insert(0,{
            "text":  hook,
            "start": 1.5,
            "end":   7.0,
            "color": "gold",
            "size":  "large"
        })

        log.info(f"Captions: {len(captions)}")
        return captions
    except Exception as e:
        log.warning(f"Captions failed: {e}")
        captions = []
        t = 5.0
        while t < total_duration - 5:
            captions.append({
                "text":  "The truth was hidden for centuries.",
                "start": t, "end": t+4.5,
                "color": "white", "size": "large"
            })
            t += 12.0
        return captions

# ── FONT SETUP ────────────────────────────────────────
def get_font(size, bold=True):
    from PIL import ImageFont
    path = (
        "/usr/share/fonts/truetype/custom/PlayfairDisplay-Bold.ttf"
        if bold else
        "/usr/share/fonts/truetype/custom/PlayfairDisplay-Regular.ttf"
    )
    try:
        return ImageFont.truetype(path, size)
    except:
        return ImageFont.load_default()

COLORS = {
    "white": (235,235,235),
    "gold":  (212,175,55),
    "cream": (255,245,220),
}

def render_caption(frame, text, color_name, size_name, progress):
    from PIL import Image, ImageDraw, ImageFont
    img  = Image.fromarray(frame)
    W,H  = img.size

    if size_name == "large":
        font_size = min(180, int(H * 0.068 * 2.5))
    elif size_name == "medium":
        font_size = min(160, int(H * 0.052 * 2.5))
    else:
        font_size = min(140, int(H * 0.042 * 2.5))
    font_size = max(font_size, 60)

    font  = get_font(font_size, bold=True)
    color = COLORS.get(color_name, COLORS["white"])

    fade  = min(1.0, progress*2.5) if progress < 0.5 \
            else min(1.0,(1.0-progress)*2.5)
    alpha = int(255 * fade)

    words   = text.split()
    lines   = []
    current = ""
    for word in words:
        test = f"{current} {word}".strip()
        if len(test) <= 28:
            current = test
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    lines = lines[:2]

    line_h   = font_size + 12
    total_h  = len(lines) * line_h
    y_start  = int(H * 0.45) - total_h // 2

    overlay = Image.new("RGBA",(W,H),(0,0,0,0))
    draw    = ImageDraw.Draw(overlay)

    for li, line in enumerate(lines):
        y    = y_start + li * line_h
        bbox = draw.textbbox((0,0),line,font=font)
        tw   = bbox[2] - bbox[0]
        x    = (W - tw) // 2

        shadow = (0,0,0,int(alpha*0.85))
        for dx,dy in [(-3,-3),(3,-3),(-3,3),(3,3),(0,4),(4,0)]:
            draw.text((x+dx,y+dy),line,font=font,fill=shadow)

        draw.text((x,y),line,font=font,fill=(*color,alpha))

    img = Image.alpha_composite(img.convert("RGBA"),overlay)
    return np.array(img.convert("RGB"))

def get_caption_at_time(captions, t):
    for cap in captions:
        if cap["start"] <= t <= cap["end"]:
            dur  = cap["end"] - cap["start"]
            prog = (t - cap["start"]) / dur if dur > 0 else 0.5
            return cap, prog
    return None, 0.0

# ══════════════════════════════════════════════════════
# SECTION 5 — VISUAL ASSETS
# ══════════════════════════════════════════════════════
def build_keywords(topic):
    t = clean_topic(topic)
    return {
        "mystery": [
            f"ancient ruins {t}","medieval castle dark",
            "roman ruins cinematic","ancient temple fog",
            "mysterious corridor dark","dark dungeon historical",
            "ancient fortress dramatic","ruins abandoned historical",
        ],
        "explanation": [
            f"ancient manuscript {t}","historical scrolls old books",
            "old map parchment antique","dusty books candlelight library",
            "medieval documents historical","ancient writing stone",
            "historical archive library dark","old letters parchment",
        ],
        "insight": [
            f"greek statue ancient {t}","philosopher statue rome",
            "roman sculpture dramatic","ancient monument historical",
            "classical statue dark","bronze statue historical",
            "ancient emperor bust","historical figure portrait",
        ],
        "reflection": [
            "sunset ancient ruins","foggy mountains historical",
            "candle flame dark room","old library candlelight",
            "misty ancient landscape","dark sky ruins dramatic",
            "empty ancient hall","moonlight historical ruins",
        ],
    }

AI_PROMPTS = {
    "mystery":     "dark cinematic ancient ruins mysterious sepia dramatic shadows no watermark",
    "explanation": "ancient manuscript scrolls candlelight dark library historical no watermark",
    "insight":     "ancient philosopher statue dramatic shadow gold cinematic no watermark",
    "reflection":  "sunset ancient ruins melancholic atmosphere cinematic dark no watermark",
}

def init_ocr():
    import easyocr
    return easyocr.Reader(['en'], gpu=False, verbose=False)

def clean_image(image_path, reader):
    import cv2
    try:
        img = cv2.imread(image_path)
        if img is None: return image_path
        H,W = img.shape[:2]
        results = reader.readtext(image_path)
        cleaned = False
        for (bbox,text,conf) in results:
            if conf < 0.4 or len(text.strip()) < 2: continue
            pts   = np.array(bbox,dtype=np.int32)
            x_min = max(0,pts[:,0].min()-10)
            y_min = max(0,pts[:,1].min()-10)
            x_max = min(W,pts[:,0].max()+10)
            y_max = min(H,pts[:,1].max()+10)
            if not (y_min > H*0.88 or
                    (y_max < H*0.08 and
                     (x_max < W*0.20 or x_min > W*0.80))):
                continue
            r = img[y_min:y_max,x_min:x_max]
            if r.size > 0:
                img[y_min:y_max,x_min:x_max] = \
                    cv2.GaussianBlur(r,(51,51),0)
            cleaned = True
        if cleaned:
            cv2.imwrite(image_path,img)
        return image_path
    except:
        return image_path

def fetch_video(query, scene, idx, reader):
    import cv2
    url = (
        f"https://pixabay.com/api/videos/?key={PIXABAY_KEY}"
        f"&q={query.replace(' ','+')}"
        f"&per_page=3&video_type=film&order=popular"
        f"&min_duration=5&max_duration=30"
    )
    try:
        r    = requests.get(url,timeout=30)
        hits = r.json().get("hits",[])
        for hit in hits[:1]:
            vurl = hit["videos"]["medium"]["url"]
            path = f"assets/videos/{scene}/vid_{idx}.mp4"
            v    = requests.get(vurl,timeout=60)
            with open(path,"wb") as f:
                f.write(v.content)
            log.info(f"  ✅ Video: {scene}/{query[:30]}")
            return path
    except Exception as e:
        log.warning(f"  Video failed: {e}")
    return None

def fetch_image(query, scene, idx, reader):
    headers = {"Authorization": PEXELS_KEY}
    params  = {"query":query,"per_page":1,"orientation":"landscape"}
    try:
        r      = requests.get(
            "https://api.pexels.com/v1/search",
            headers=headers,params=params,timeout=30
        )
        photos = r.json().get("photos",[])
        for photo in photos[:1]:
            url  = photo["src"]["large2x"]
            path = f"assets/images/{scene}/img_{idx}.jpg"
            data = requests.get(url,timeout=60).content
            with open(path,"wb") as f:
                f.write(data)
            clean_image(path, reader)
            log.info(f"  ✅ Image: {scene}/{query[:30]}")
            return path
    except Exception as e:
        log.warning(f"  Image failed: {e}")
    return None

def fetch_ai_image(topic, scene, idx):
    prompt  = AI_PROMPTS[scene] + f" {clean_topic(topic)}"
    encoded = requests.utils.quote(prompt)
    url     = (
        f"https://image.pollinations.ai/prompt/{encoded}"
        f"?width=1920&height=1080&nologo=true&seed={idx}"
    )
    try:
        r = requests.get(url,timeout=90)
        if r.status_code == 200 and len(r.content) > 5000:
            path = f"assets/images/{scene}/ai_{idx}.jpg"
            with open(path,"wb") as f:
                f.write(r.content)
            log.info(f"  ✅ AI image: {scene}")
            return path
    except Exception as e:
        log.warning(f"  AI image failed: {e}")
    return None

def fetch_all_assets(topic):
    log.info("Fetching assets...")
    for scene in SCENE_TARGETS:
        os.makedirs(f"assets/videos/{scene}",exist_ok=True)
        os.makedirs(f"assets/images/{scene}",exist_ok=True)

    reader     = init_ocr()
    keywords   = build_keywords(topic)
    all_videos = {s:[] for s in SCENE_TARGETS}
    all_images = {s:[] for s in SCENE_TARGETS}

    for scene in SCENE_TARGETS:
        tv  = SCENE_TARGETS[scene]["videos"]
        ti  = SCENE_TARGETS[scene]["images"]
        vi  = ii = 0
        kws = keywords[scene]

        for kw in kws:
            if len(all_videos[scene]) < tv:
                v = fetch_video(kw,scene,vi,reader)
                if v:
                    all_videos[scene].append(v)
                    vi += 1
                time.sleep(0.5)
            if len(all_images[scene]) < ti:
                i = fetch_image(kw,scene,ii,reader)
                if i:
                    all_images[scene].append(i)
                    ii += 1
                time.sleep(0.5)

        while len(all_images[scene]) < ti:
            ai = fetch_ai_image(topic,scene,ii)
            if ai:
                all_images[scene].append(ai)
                ii += 1
            else:
                break

    total_v = sum(len(v) for v in all_videos.values())
    total_i = sum(len(i) for i in all_images.values())
    log.info(f"Assets: {total_v} videos | {total_i} images")
    return all_videos, all_images

# ══════════════════════════════════════════════════════
# SECTION 6 — COLOR GRADING
# ══════════════════════════════════════════════════════
GRADE = {
    "brightness":0.68,"contrast":1.35,"saturation":0.35,
    "black_lift":0.06,"warm_temp":0.80,"sepia":0.45,
    "sepia_opacity":0.35,"vignette":0.70,"vignette_feather":0.72,
    "grain":0.15,"sharpness":0.55,"glow":0.20,
    "highlights":0.50,"shadows":0.80,"blue_shadows":0.05,
    "gold_highlights":0.35,"orange_midtones":0.50,
}

def grade_frame(frame):
    from PIL import Image, ImageEnhance, ImageFilter
    img = frame.astype(np.float32)/255.0
    # Exposure
    img = img + GRADE["black_lift"]*0.05
    img = img * GRADE["brightness"]
    img = np.clip(img,0,1)
    img = img + (GRADE["contrast"]-1.0)*0.15*(img-0.5)
    img = np.clip(img,0,1)
    # Color balance
    r = img[:,:,0]+GRADE["orange_midtones"]*0.06
    g = img[:,:,1]+GRADE["orange_midtones"]*0.03
    b = img[:,:,2]-GRADE["blue_shadows"]*0.08
    img = np.clip(np.stack([r,g,b],axis=2),0,1)
    # Temperature
    warm = (GRADE["warm_temp"]-0.5)*0.35
    r = img[:,:,0]+warm+0.02
    g = img[:,:,1]-0.01
    b = img[:,:,2]-warm*1.2+0.01
    img = np.clip(np.stack([r,g,b],axis=2),0,1)
    # Saturation
    uint8 = (img*255).astype(np.uint8)
    pil   = Image.fromarray(uint8)
    pil   = ImageEnhance.Color(pil).enhance(GRADE["saturation"])
    img   = np.array(pil).astype(np.float32)/255.0
    # Sepia
    uint8  = (img*255).astype(np.uint8)
    gray   = Image.fromarray(uint8).convert("L")
    sepia  = Image.new("RGB",gray.size)
    px     = gray.load()
    sp     = sepia.load()
    for y in range(gray.height):
        for x in range(gray.width):
            g = px[x,y]
            sp[x,y] = (
                min(255,int(g*1.08)),
                min(255,int(g*0.86)),
                min(255,int(g*0.67))
            )
    orig  = Image.fromarray(uint8)
    blend = Image.blend(orig,sepia,GRADE["sepia_opacity"])
    img   = np.array(blend).astype(np.float32)/255.0
    # Vignette
    Hf,Wf = img.shape[:2]
    Y,X   = np.ogrid[:Hf,:Wf]
    cx,cy = Wf/2,Hf/2
    dist  = np.sqrt(((X-cx)/cx)**2+((Y-cy)/cy)**2)
    vign  = 1-np.clip(dist*GRADE["vignette"]*0.85,0,GRADE["vignette"])
    img   = np.clip(img*vign[:,:,np.newaxis],0,1)
    # Sharpness
    uint8 = (img*255).astype(np.uint8)
    pil   = Image.fromarray(uint8)
    pil   = ImageEnhance.Sharpness(pil).enhance(1.0+GRADE["sharpness"]*0.5)
    img   = np.array(pil).astype(np.float32)/255.0
    # Grain
    noise = np.random.normal(0,GRADE["grain"]*0.03,img.shape).astype(np.float32)
    img   = np.clip(img+noise,0,1)
    return (img*255).astype(np.uint8)

def grade_video_ffmpeg(input_path, output_path):
    cmd = [
        "ffmpeg","-y","-i",input_path,
        "-vf",(
            "eq=contrast=1.4:brightness=-0.08:saturation=0.35,"
            "colorchannelmixer="
            "rr=0.393:rg=0.769:rb=0.189:"
            "gr=0.349:gg=0.686:gb=0.168:"
            "br=0.272:bg=0.534:bb=0.131,"
            "hue=s=0.4,scale=1920:1080,vignette=PI/4"
        ),
        "-c:v","libx264","-preset","ultrafast",
        "-crf","23","-an",output_path
    ]
    result = subprocess.run(cmd,capture_output=True,text=True)
    return result.returncode == 0

def grade_all(all_videos, all_images):
    import cv2
    log.info("Grading assets...")

    # Grade images
    graded_images = {s:[] for s in all_images}
    for scene,paths in all_images.items():
        for path in paths:
            try:
                img = cv2.imread(path)
                if img is None:
                    graded_images[scene].append(path)
                    continue
                img    = cv2.cvtColor(img,cv2.COLOR_BGR2RGB)
                img    = cv2.resize(img,(1920,1080))
                graded = grade_frame(img)
                out    = path.replace(".jpg","_graded.jpg")
                cv2.imwrite(out,cv2.cvtColor(graded,cv2.COLOR_RGB2BGR))
                if os.path.exists(path) and path != out:
                    os.remove(path)
                graded_images[scene].append(out)
            except Exception as e:
                log.warning(f"Image grade failed: {e}")
                graded_images[scene].append(path)

    # Grade videos with FFmpeg
    graded_videos = {s:[] for s in all_videos}
    for scene,paths in all_videos.items():
        for i,path in enumerate(paths):
            gpath = path.replace(".mp4","_graded.mp4")
            if os.path.exists(gpath):
                graded_videos[scene].append(gpath)
                continue
            log.info(f"  Grading video {scene}/{i}...")
            ok = grade_video_ffmpeg(path,gpath)
            if ok:
                if os.path.exists(path):
                    os.remove(path)
                graded_videos[scene].append(gpath)
            else:
                graded_videos[scene].append(path)

    tv = sum(len(v) for v in graded_videos.values())
    ti = sum(len(i) for i in graded_images.values())
    log.info(f"Graded: {tv} videos | {ti} images")
    return graded_videos, graded_images

# ══════════════════════════════════════════════════════
# SECTION 7 — VIDEO ASSEMBLY
# ══════════════════════════════════════════════════════
def build_media_list(graded_videos, graded_images, total_duration):
    video_pool = []
    image_pool = []
    for scene in SCENE_ORDER:
        for v in graded_videos.get(scene,[]):
            video_pool.append(("video",scene,v))
        for i in graded_images.get(scene,[]):
            image_pool.append(("image",scene,i))

    pattern = ["video","video","image",
               "video","video","image","video","image"]
    media   = []
    vi = ii = p = 0

    def total_dur(m):
        return sum(
            CLIP_DUR_VIDEO if x[0]=="video"
            else CLIP_DUR_IMAGE for x in m
        )

    while total_dur(media) < total_duration+30:
        slot = pattern[p%len(pattern)]
        if slot=="video" and video_pool:
            media.append(video_pool[vi%len(video_pool)])
            vi += 1
        elif slot=="image" and image_pool:
            media.append(image_pool[ii%len(image_pool)])
            ii += 1
        else:
            if video_pool:
                media.append(video_pool[vi%len(video_pool)])
                vi += 1
            elif image_pool:
                media.append(image_pool[ii%len(image_pool)])
                ii += 1
        p += 1

    log.info(f"Media: {len(media)} clips for {total_duration:.1f}s")
    return media

def add_dust(frame, seed=0):
    import cv2
    np.random.seed(seed%1000)
    overlay = np.zeros((H,W),dtype=np.float32)
    for _ in range(10):
        x = np.random.randint(0,W)
        y = np.random.randint(0,H)
        r = np.random.randint(1,3)
        cv2.circle(overlay,(x,y),r,float(np.random.uniform(0.3,0.6)),-1)
    overlay   = cv2.GaussianBlur(overlay,(5,5),0)
    frame_f   = frame.astype(np.float32)/255.0
    result    = np.clip(frame_f+np.stack([overlay]*3,axis=2)*0.18,0,1)
    return (result*255).astype(np.uint8)

def ken_burns(img_array, t, duration, effect="zoom_in"):
    from PIL import Image
    img = Image.fromarray(img_array)
    if effect=="zoom_in":   scale = 1.0+0.12*(t/duration)
    elif effect=="zoom_out": scale = 1.12-0.12*(t/duration)
    else:                    scale = 1.08
    nW,nH = int(W*scale),int(H*scale)
    img   = img.resize((nW,nH),Image.LANCZOS)
    if effect=="pan":
        x = int((nW-W)*(t/duration))
        y = (nH-H)//2
    else:
        x = (nW-W)//2
        y = (nH-H)//2
    return np.array(img.crop((x,y,x+W,y+H)))

def crossfade(f1,f2,p):
    s = p*p*(3-2*p)
    return np.clip(f1.astype(np.float32)*(1-s)+f2.astype(np.float32)*s,0,255).astype(np.uint8)

def blur_fade(f1,f2,p):
    import cv2
    s = p*p*(3-2*p)
    b = max(1,int(21*np.sin(p*np.pi)))
    if b%2==0: b+=1
    return np.clip(
        cv2.GaussianBlur(f1,(b,b),0).astype(np.float32)*(1-s)+
        cv2.GaussianBlur(f2,(b,b),0).astype(np.float32)*s,
        0,255
    ).astype(np.uint8)

def fade_black(frame,p,fade_in=False):
    a = p if fade_in else 1.0-p
    s = a*a*(3-2*a)
    return (frame.astype(np.float32)*s).astype(np.uint8)

def get_transition(i):
    if i%4==0 and i>0: return "fade_black"
    return "crossfade" if random.random()<0.75 else "blur_fade"

def get_kb():
    r = random.random()
    if r<0.60: return "zoom_in"
    elif r<0.85: return "zoom_out"
    else: return "pan"

def get_video_frame(cap, total_frames, frame_idx, clip_dur):
    import cv2
    actual = int(frame_idx*total_frames/(clip_dur*FPS))
    actual = min(actual,total_frames-1)
    cap.set(cv2.CAP_PROP_POS_FRAMES,actual)
    ret,frame = cap.read()
    if not ret: return np.zeros((H,W,3),dtype=np.uint8)
    frame = cv2.cvtColor(frame,cv2.COLOR_BGR2RGB)
    return cv2.resize(frame,(W,H))

def get_first_frame(mtype,path):
    import cv2
    try:
        if mtype=="video":
            cap = cv2.VideoCapture(path)
            ret,frame = cap.read()
            cap.release()
            if ret:
                frame = cv2.cvtColor(frame,cv2.COLOR_BGR2RGB)
                return cv2.resize(frame,(W,H))
        else:
            img = cv2.imread(path)
            if img is not None:
                img = cv2.cvtColor(img,cv2.COLOR_BGR2RGB)
                return cv2.resize(img,(W,H))
    except: pass
    return np.zeros((H,W,3),dtype=np.uint8)

def write_transition(writer,f1,f2,trans_type):
    import cv2
    n = int(TRANS_DUR*FPS)
    for i in range(n):
        p = i/n
        if trans_type=="crossfade":   frame = crossfade(f1,f2,p)
        elif trans_type=="blur_fade": frame = blur_fade(f1,f2,p)
        elif trans_type=="fade_black":
            if p<0.4:   frame = fade_black(f1,p/0.4)
            elif p<0.6: frame = np.zeros((H,W,3),dtype=np.uint8)
            else:       frame = fade_black(f2,(p-0.6)/0.4,fade_in=True)
        else: frame = crossfade(f1,f2,p)
        writer.write(cv2.cvtColor(frame,cv2.COLOR_RGB2BGR))

def write_clip_frames(writer,mtype,path,duration,
                      captions,time_offset,total_dur,first_clip=False):
    import cv2
    n_frames  = int(duration*FPS)
    kb_effect = get_kb()
    last      = None

    if mtype=="video":
        cap   = cv2.VideoCapture(path)
        total = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    else:
        img = cv2.imread(path)
        if img is None:
            return np.zeros((H,W,3),dtype=np.uint8)
        img = cv2.cvtColor(img,cv2.COLOR_BGR2RGB)
        img = cv2.resize(img,(W,H))

    for i in range(n_frames):
        t    = i/FPS
        curr = time_offset+t
        if curr > total_dur: break

        if mtype=="video": frame = get_video_frame(cap,total,i,duration)
        else:              frame = ken_burns(img,t,duration,kb_effect)

        if first_clip and i < int(1.5*FPS):
            frame = fade_black(frame,i/(1.5*FPS),fade_in=True)

        frame = add_dust(frame,seed=i)

        cap_data,cap_prog = get_caption_at_time(captions,curr)
        if cap_data:
            frame = render_caption(
                frame,cap_data["text"],
                cap_data["color"],cap_data["size"],cap_prog
            )

        writer.write(cv2.cvtColor(frame,cv2.COLOR_RGB2BGR))
        last = frame

    if mtype=="video": cap.release()
    return last if last is not None else np.zeros((H,W,3),dtype=np.uint8)

def assemble_video(graded_videos, graded_images,
                   captions, total_duration, output_file):
    import cv2
    log.info("Assembling video...")

    media  = build_media_list(graded_videos,graded_images,total_duration)
    writer = cv2.VideoWriter(
        "temp_video.mp4",
        cv2.VideoWriter_fourcc(*'mp4v'),
        FPS,(W,H)
    )

    time_offset = 0.0
    last_frame  = None

    for i,(mtype,scene,path) in enumerate(media):
        if time_offset >= total_duration: break

        clip_dur  = CLIP_DUR_VIDEO if mtype=="video" else CLIP_DUR_IMAGE
        clip_dur  = min(clip_dur,total_duration-time_offset)

        log.info(f"  [{i+1}/{len(media)}] {mtype}: {scene}/{os.path.basename(path)[:20]}")

        if last_frame is not None:
            next_f = get_first_frame(mtype,path)
            write_transition(writer,last_frame,next_f,get_transition(i))

        last_frame = write_clip_frames(
            writer,mtype,path,clip_dur,
            captions,time_offset,total_duration,
            first_clip=(i==0)
        )
        time_offset += clip_dur
        log.info(f"    [{time_offset:.1f}s / {total_duration:.1f}s]")

    # Fade out
    if last_frame is not None:
        for fi in range(int(2.0*FPS)):
            frame = fade_black(last_frame,fi/(2.0*FPS))
            writer.write(cv2.cvtColor(frame,cv2.COLOR_RGB2BGR))

    writer.release()
    log.info("Frames written")

    # Add audio
    from moviepy.editor import VideoFileClip, AudioFileClip, CompositeAudioClip
    video_clip = VideoFileClip("temp_video.mp4")
    voice      = AudioFileClip("voiceover.wav")

    if os.path.exists("background.mp3"):
        music = AudioFileClip("background.mp3").volumex(0.08)
        music = music.audio_loop(duration=total_duration) \
                if music.duration < total_duration \
                else music.subclip(0,total_duration)
        mixed = CompositeAudioClip([voice.volumex(1.0),music])
    else:
        mixed = voice

    video_clip = video_clip.set_audio(mixed).set_duration(total_duration)
    video_clip.write_videofile(
        output_file,fps=FPS,
        codec="libx264",audio_codec="aac",
        threads=2,preset="ultrafast",logger=None
    )

    if os.path.exists("temp_video.mp4"):
        os.remove("temp_video.mp4")
    video_clip.close()
    voice.close()

    log.info(f"Video saved: {output_file}")

    # Cleanup graded videos
    for scene,paths in graded_videos.items():
        for path in paths:
            if os.path.exists(path):
                os.remove(path)
    log.info("Graded videos cleaned up")

# ══════════════════════════════════════════════════════
# SECTION 8 — YOUTUBE UPLOAD
# ══════════════════════════════════════════════════════
def get_schedule_time():
    now       = datetime.datetime.utcnow()
    scheduled = now.replace(
        hour=SCHEDULE_HOUR_UTC,
        minute=SCHEDULE_MINUTE_UTC,
        second=0, microsecond=0
    )
    if scheduled <= now:
        scheduled += datetime.timedelta(days=1)
    return scheduled.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def get_youtube_service():
    from google_auth_oauthlib.flow import InstalledAppFlow
    from googleapiclient.discovery import build
    import google.auth.transport.requests

    SCOPES = [
        "https://www.googleapis.com/auth/youtube.upload",
        "https://www.googleapis.com/auth/youtube",
    ]
    creds = None

    if os.path.exists("youtube_token.pkl"):
        with open("youtube_token.pkl","rb") as f:
            creds = pickle.load(f)

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(google.auth.transport.requests.Request())
            log.info("Token refreshed")
        except:
            creds = None

    if not creds or not creds.valid:
        log.error("YouTube token invalid — re-authenticate manually once")
        return None

    return build("youtube","v3",credentials=creds,cache_discovery=False)

def generate_title(topic, script):
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)
        prompt = f"""Write ONE viral YouTube title for dark history channel.
Topic: {topic}
Script: {script[:500]}
Rules:
- Max 70 chars with emojis
- Start with: 🔴 ⚠️ 💀 🔥 🕵️
- Controversial and shocking
- End with emoji
Examples:
🔴 The Dark Secret CIA Buried For 50 Years 💀
⚠️ What The Government Never Wanted You To Know 🕵️
Output only the title."""
        r     = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}],
            temperature=0.9,max_tokens=80
        )
        title = r.choices[0].message.content.strip().replace('"','').strip()
        log.info(f"Title: {title}")
        return title
    except Exception as e:
        log.warning(f"Title failed: {e}")
        return f"🔴 {topic} 💀"

def generate_description(topic, script, title, duration):
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)
        prompt = f"""Write YouTube description for dark history video.
Title: {title}
Script: {script[:1000]}
Write 150 words with hook, summary, cliffhanger.
Output only description."""
        r       = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}],
            temperature=0.8,max_tokens=300
        )
        summary = r.choices[0].message.content.strip()
        mins    = int(duration//60)
        inter   = max(1,mins//5)
        labels  = ["The Hidden Truth","Dark History Revealed",
                   "The Real Story","Final Verdict"]
        ts = "📍 CHAPTERS\n0:00 - Introduction\n"
        for i in range(1,5):
            ts += f"{i*inter}:00 - {labels[i-1]}\n"
        search = clean_topic(topic)
        tags   = " ".join([f"#{w}" for w in search.lower().split() if len(w)>3])
        tags  += " #darkhistory #history #documentary #hidden #truth #mystery #india"
        cta    = (
            f"\n🔔 Subscribe to {CHANNEL_NAME} for dark history every week.\n"
            f"👍 Like if this changed how you see history.\n"
            f"💬 Comment what you want uncovered next.\n"
        )
        return f"{summary}\n\n{ts}\n{cta}\n{tags}"
    except Exception as e:
        log.warning(f"Description failed: {e}")
        return f"{topic}\n\n#darkhistory #history"

def generate_thumbnail(topic, title):
    from PIL import Image, ImageDraw, ImageFont
    log.info("Generating thumbnail...")
    search = clean_topic(topic)
    ai_ok  = False

    for seed in [42,123,777,999,555]:
        try:
            prompt  = (
                f"dark cinematic {search} dramatic lighting "
                f"sepia mysterious historical epic "
                f"no text no watermark youtube thumbnail"
            )
            encoded = requests.utils.quote(prompt)
            url     = (
                f"https://image.pollinations.ai/prompt/{encoded}"
                f"?width=1280&height=720&nologo=true&seed={seed}"
            )
            r = requests.get(url,timeout=120)
            if r.status_code==200 and len(r.content)>5000:
                with open("thumb_base.jpg","wb") as f:
                    f.write(r.content)
                ai_ok = True
                log.info("AI thumbnail generated")
                break
        except:
            continue

    if not ai_ok:
        for scene in ["mystery","explanation","insight","reflection"]:
            folder = f"assets/images/{scene}"
            if os.path.exists(folder):
                imgs = [f for f in os.listdir(folder) if f.endswith(".jpg")]
                if imgs:
                    import shutil
                    shutil.copy(f"{folder}/{imgs[0]}","thumb_base.jpg")
                    break

    try:
        img     = Image.open("thumb_base.jpg").convert("RGB")
        img     = img.resize((1280,720),Image.LANCZOS)
        overlay = Image.new("RGBA",(1280,720),(0,0,0,0))
        od      = ImageDraw.Draw(overlay)
        for y in range(300,720):
            alpha = int(220*(y-300)/420)
            od.line([(0,y),(1280,y)],fill=(0,0,0,alpha))
        for x in range(200):
            alpha = int(100*(1-x/200))
            od.line([(x,0),(x,720)],fill=(0,0,0,alpha))
            od.line([(1280-x,0),(1280-x,720)],fill=(0,0,0,alpha))
        img = Image.alpha_composite(img.convert("RGBA"),overlay).convert("RGB")

        try:
            font_big = ImageFont.truetype(
                "/usr/share/fonts/truetype/custom/PlayfairDisplay-Bold.ttf",82)
            font_sm  = ImageFont.truetype(
                "/usr/share/fonts/truetype/custom/PlayfairDisplay-Bold.ttf",32)
            font_tag = ImageFont.truetype(
                "/usr/share/fonts/truetype/custom/PlayfairDisplay-Regular.ttf",26)
        except:
            font_big = font_sm = font_tag = ImageFont.load_default()

        draw        = ImageDraw.Draw(img)
        clean_title = re.sub(r'[^\x00-\x7F]+','',title).strip()
        words       = clean_title.split()
        mid         = max(1,len(words)//2)
        lines       = [" ".join(words[:mid])," ".join(words[mid:])]

        draw.rectangle([(60,395),(420,403)],fill=(180,20,20))

        for line,y in zip(lines,[415,515]):
            if not line.strip(): continue
            bbox = draw.textbbox((0,0),line,font=font_big)
            tw   = bbox[2]-bbox[0]
            x    = (1280-tw)//2
            for dx,dy in [(-4,-4),(4,-4),(-4,4),(4,4),
                          (-6,0),(6,0),(0,-6),(0,6),
                          (-8,0),(8,0),(0,-8),(0,8)]:
                draw.text((x+dx,y+dy),line,font=font_big,fill=(0,0,0,255))
            draw.text((x,y),line,font=font_big,fill=(255,255,255,255))
            draw.line([(x,y+90),(x+tw,y+90)],fill=(212,175,55),width=3)

        draw.text((35,25),f"🎬 {CHANNEL_NAME}",font=font_sm,fill=(212,175,55))
        draw.rectangle([(0,672),(1280,720)],fill=(12,8,6))
        draw.text((35,683),"DARK HISTORY  •  HIDDEN TRUTH  •  CLASSIFIED",
                 font=font_tag,fill=(180,140,40))
        draw.line([(0,0),(1280,0)],fill=(212,175,55),width=5)

        img.save("thumbnail.jpg",quality=98)
        if os.path.exists("thumb_base.jpg"):
            os.remove("thumb_base.jpg")
        log.info("Thumbnail saved")
        return "thumbnail.jpg"
    except Exception as e:
        log.warning(f"Thumbnail compose failed: {e}")
        img  = Image.new("RGB",(1280,720),(10,8,6))
        draw = ImageDraw.Draw(img)
        try:
            font = ImageFont.truetype(
                "/usr/share/fonts/truetype/custom/PlayfairDisplay-Bold.ttf",75)
        except:
            font = ImageFont.load_default()
        clean = re.sub(r'[^\x00-\x7F]+','',title).strip()
        words = clean.split()
        mid   = len(words)//2
        for line,y in [(" ".join(words[:mid]),250),(" ".join(words[mid:]),370)]:
            bbox = draw.textbbox((0,0),line,font=font)
            tw   = bbox[2]-bbox[0]
            draw.text(((1280-tw)//2,y),line,font=font,fill=(255,255,255))
        draw.line([(0,690),(1280,690)],fill=(212,175,55),width=4)
        img.save("thumbnail.jpg",quality=98)
        return "thumbnail.jpg"

def upload_video(video_file, title, description, thumbnail):
    from googleapiclient.http import MediaFileUpload
    youtube = get_youtube_service()
    if not youtube:
        log.error("YouTube auth failed")
        return None, None

    publish_at = get_schedule_time()
    log.info(f"Scheduling: 8 PM IST ({publish_at} UTC)")

    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": [
                "dark history","history documentary",
                "hidden truth","ancient history","mystery",
                "historical facts","educational",
                "darkhistorymind","conspiracy","india"
            ],
            "categoryId": "27",
            "defaultLanguage": "en",
        },
        "status": {
            "privacyStatus": "private",
            "publishAt": publish_at,
            "selfDeclaredMadeForKids": False,
        }
    }

    try:
        media   = MediaFileUpload(
            video_file,mimetype="video/mp4",
            resumable=True,chunksize=5*1024*1024
        )
        request = youtube.videos().insert(
            part="snippet,status",body=body,media_body=media
        )
        response = None
        while response is None:
            status,response = request.next_chunk()
            if status:
                log.info(f"  Upload: {int(status.progress()*100)}%")

        video_id  = response["id"]
        video_url = f"https://youtube.com/watch?v={video_id}"
        log.info(f"Uploaded: {video_url}")

        try:
            youtube.thumbnails().set(
                videoId=video_id,
                media_body=MediaFileUpload(thumbnail)
            ).execute()
            log.info("Thumbnail set")
        except Exception as e:
            log.warning(f"Thumbnail failed: {e}")

        return video_id, video_url
    except Exception as e:
        log.error(f"Upload failed: {e}")
        return None, None

def update_sheet(topic, video_url, title):
    try:
        import gspread
        from google.oauth2.service_account import ServiceAccountCredentials
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            "client_secrets.json", scope
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)
        ws = sh.get_worksheet(0)
        try:
            cell = ws.find(topic)
            if cell:
                ws.update_cell(cell.row,2,
                    datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))
                ws.update_cell(cell.row,3,video_url)
                ws.update_cell(cell.row,4,"scheduled")
            else:
                raise Exception("not found")
        except:
            ws.append_row([
                topic,
                datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
                video_url,"scheduled"
            ])
        log.info("Sheet updated")
    except Exception as e:
        log.warning(f"Sheet update failed: {e}")

# ══════════════════════════════════════════════════════
# MAIN — RUN ALL SECTIONS
# ══════════════════════════════════════════════════════
def main():
    log.info("="*50)
    log.info("DarkHistoryMind Pipeline Starting")
    log.info("="*50)

    # Setup
    setup_permanent_files()

    # Section 1 — Topic
    log.info("── SECTION 1: TOPIC ──")
    used  = get_used_topics()
    topic = select_topic(used)
    facts = get_facts(topic)
    hook  = generate_hook(topic,facts)
    save_topic_to_sheet(topic)

    # Section 2 — Script
    log.info("── SECTION 2: SCRIPT ──")
    script = generate_script(topic,facts)
    if not script:
        log.error("Script generation failed")
        sys.exit(1)

    # Section 3 — Voice
    log.info("── SECTION 3: VOICE ──")
    ok = generate_voice(script)
    if not ok:
        log.error("Voice generation failed")
        sys.exit(1)

    # Section 4 — Captions
    log.info("── SECTION 4: CAPTIONS ──")
    total_duration = get_audio_duration()
    captions       = generate_captions(script,total_duration,hook)

    # Section 5 — Assets
    log.info("── SECTION 5: ASSETS ──")
    all_videos, all_images = fetch_all_assets(topic)

    # Section 6 — Grading
    log.info("── SECTION 6: GRADING ──")
    graded_videos, graded_images = grade_all(all_videos,all_images)

    # Section 7 — Assembly
    log.info("── SECTION 7: ASSEMBLY ──")
    output_file = (
        topic.replace(' ','_')
             .replace('—','').replace('?','')
             .replace(':','').strip('_') + ".mp4"
    )
    assemble_video(
        graded_videos,graded_images,
        captions,total_duration,output_file
    )

    # Section 8 — Upload
    log.info("── SECTION 8: UPLOAD ──")
    title       = generate_title(topic,script)
    description = generate_description(topic,script,title,total_duration)
    thumbnail   = generate_thumbnail(topic,title)
    video_id,video_url = upload_video(output_file,title,description,thumbnail)

    if video_url:
        update_sheet(topic,video_url,title)
        log.info("="*50)
        log.info("PIPELINE COMPLETE")
        log.info(f"Video: {video_url}")
        log.info(f"Title: {title}")
        log.info(f"Scheduled: 8 PM IST")
        log.info("="*50)
    else:
        log.error("Upload failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
