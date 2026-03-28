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
TEST_MODE         = os.environ.get("TEST_MODE", "false").lower() == "true"
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
    _download_playfair_if_missing()
    files = {
        "background.mp3":      GDRIVE_MUSIC_ID,
        "client_secrets.json": GDRIVE_SECRETS_ID,
        "youtube_token.pkl":    GDRIVE_TOKEN_ID,
    }
    for name, fid in files.items():
        if os.path.exists(name):
            log.info(f"  ✅ {name} exists")
            continue
        log.info(f"  Downloading {name}...")
        ok = download_from_drive(fid, name)
        log.info(f"  {'✅' if ok else '❌'} {name}")

def clean_topic(topic):
    prefixes = [
        "The Psychology of","The Dark Truth About",
        "The Secret Life of","The Truth Behind",
        "The Real Story of","The Hidden Truth About",
        "The Dark History of","Why Did",
        "The Real History of","The Hidden History of",
        "The Dark Story of","The Real Psychology of",
        "The Dark Psychology of","The Real Reason",
        "The Secret History of","The Secret Wars of",
        "The Secret Society of",
    ]
    for p in prefixes:
        topic = topic.replace(p,"").strip()
    return topic

# ══════════════════════════════════════════════════════
# SECTION 1 — TOPIC RESEARCH
# ══════════════════════════════════════════════════════
def get_sheet_client():
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        
        if not os.path.exists("client_secrets.json"):
            log.error("❌ client_secrets.json not found")
            return None
        
        try:
            creds = Credentials.from_service_account_file(
                "client_secrets.json",
                scopes=[
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"
                ]
            )
            gc = gspread.authorize(creds)
            log.info("✅ Sheet client: Using service account")
            return gc
        except Exception as sa_err:
            log.error(f"Service account auth failed: {sa_err}")
            return None
    except Exception as e:
        log.error(f"❌ Sheet client error: {e}")
        return None

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
        return used
    except Exception as e:
        log.warning(f"Sheet read failed: {e}")
        return []

def select_topic(used):
    available = [t for t in TOPIC_BANK if t.strip().lower() not in used]
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
        if not results: return ""
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
        prompt = f"""Write ONE hook sentence for a dark history YouTube video. Topic: {topic}. Rules: max 12 words, shocking statement, dark tone."""
        r = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}],
            temperature=0.9, max_tokens=50
        )
        return r.choices[0].message.content.strip().replace('"','')
    except:
        return f"The truth about {topic} was hidden for centuries."
        
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
# SECTION 4 — CAPTIONS (UPDATED: WHISPER SYNC)
# ══════════════════════════════════════════════════════
CAPTION_KEYWORDS = {
    "empire","emperor","king","queen","pharaoh","caesar","pope",
    "blood","death","war","battle","massacre","execution","torture",
    "secret","hidden","forbidden","truth","lie","betrayal","conspiracy",
    "vanished","destroyed","collapsed","fell","conquered","murdered","burned",
    "million","billion","century","centuries","ancient","medieval",
    "roman","greek","egypt","persian","mongol","viking","spartan",
    "never","always","only","first","last","single","entire",
    "dark","evil","brutal","savage","ruthless","feared","powerful",
    "church","vatican","pope","crusade","inquisition","heresy",
    "plague","disease","famine","revolt","revolution","assassination",
}

def get_audio_duration():
    try:
        try:
            from moviepy.editor import AudioFileClip
        except ImportError:
            from moviepy import AudioFileClip
        clip = AudioFileClip("voiceover.wav")
        dur  = clip.duration
        clip.close()
        return dur
    except:
        return 420.0

def transcribe_voiceover():
    """
    REFINED: Uses faster-whisper for exact timing.
    This ensures long-form captions never drift.
    """
    try:
        from faster_whisper import WhisperModel
        # Using 'tiny' for speed; change to 'base' for higher accuracy if needed
        model = WhisperModel("tiny", device="cpu", compute_type="int8")
        segments, _ = model.transcribe("voiceover.wav", word_timestamps=True)
        words = []
        for segment in segments:
            for w in segment.words:
                words.append({
                    "word": w.word.strip(),
                    "start": round(w.start, 2),
                    "end": round(w.end, 2)
                })
        return words
    except Exception as e:
        log.error(f"Whisper failed: {e}. Falling back to char-ratio sync.")
        return []

def identify_key_moments(script):
    """
    Identify 3-5 crucial sentences where captions should appear.
    Uses AI to find the most shocking/important moments.
    """
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)
        
        # Get first ~1500 chars of script for analysis
        script_sample = script[:1500]
        
        prompt = f"""Analyze this dark history script and identify 3-5 CRUCIAL sentences that summarize the core shocking truth.

Script: {script_sample}

Important: Return ONLY a JSON array of sentence START INDICES where captions should appear, like:
[0, 5, 12, 18]

These should be the most shocking/important moments. Do NOT explain, just return the JSON array."""
        
        r = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=100
        )
        
        result = r.choices[0].message.content.strip()
        
        # Try to parse JSON
        try:
            # Clean up response (remove markdown if present)
            result = result.replace("```json", "").replace("```", "").strip()
            key_indices = json.loads(result)
            
            if isinstance(key_indices, list) and len(key_indices) > 0:
                log.info(f"✅ Key caption moments identified: {key_indices}")
                return key_indices
            else:
                log.warning(f"Invalid key moments format: {result}")
                return []
        except json.JSONDecodeError:
            log.warning(f"Could not parse key moments JSON: {result}")
            # Fallback: mark every 4th caption
            return []
    
    except Exception as e:
        log.error(f"❌ Key moment detection failed: {e}")
        return []


def mark_captions_by_keywords(captions):
    """
    ALTERNATIVE METHOD: Mark captions based on crucial keywords.
    Use this if AI detection is unreliable.
    """
    crucial_keywords = CAPTION_KEYWORDS  # Already defined in your code
    
    rendered_count = 0
    for cap in captions:
        text_upper = cap["text"].upper()
        # Check if caption contains any crucial keywords
        has_crucial = any(kw.upper() in text_upper for kw in crucial_keywords)
        cap["render"] = has_crucial
        
        if has_crucial:
            rendered_count += 1
    
    log.info(f"✅ Marked {rendered_count}/{len(captions)} captions by keywords")
    return captions


def mark_key_captions(captions, key_moment_indices):
    """
    Mark which captions are at key moments for selective rendering.
    """
    for i, cap in enumerate(captions):
        cap["is_key"] = i in key_moment_indices
        cap["render"] = i in key_moment_indices
    
    rendered_count = sum(1 for c in captions if c.get('render', False))
    log.info(f"✅ Marked {rendered_count}/{len(captions)} captions as key moments")
    return captions


def generate_captions(script, total_duration, hook):
    """
    FIXED: Now includes 'render' flag for selective caption display.
    Captions are generated but marked for selective rendering.
    """
    words = transcribe_voiceover()
    
    # Backup: Character-ratio sync if Whisper transcription fails
    if not words:
        sentences = re.split(r'(?<=[.!?]) +', script)
        total_chars = sum(len(s) for s in sentences)
        captions = []
        curr = 0.0
        for s in sentences:
            if not s.strip(): continue
            dur = total_duration * (len(s) / total_chars)
            captions.append({
                "text": s.strip().upper(), 
                "start": curr, 
                "end": curr + dur,
                "color": "white",
                "size": "medium",
                "render": False  # ← Will be set by mark_key_captions()
            })
            curr += dur
        return captions

    # Primary: Whisper-based grouping
    captions = []
    chunk_size = 6 
    for i in range(0, len(words), chunk_size):
        chunk = words[i:i + chunk_size]
        text = " ".join([w["word"] for w in chunk]).upper()
        captions.append({
            "text": text,
            "start": chunk[0]["start"],
            "end": chunk[-1]["end"],
            "color": "white",
            "size": "medium",
            "render": False  # ← Will be set by mark_key_captions()
        })
    
    return captions

# ── FONT SETUP (No Changes Required) ──────────────────
_FONT_CANDIDATES_BOLD = [
    "/usr/share/fonts/truetype/custom/PlayfairDisplay-Bold.ttf",
    "/usr/share/fonts/truetype/fonts-playfair-display/PlayfairDisplay-Bold.ttf",
    "/usr/share/fonts/truetype/PlayfairDisplay-Bold.ttf",
    os.path.expanduser("~/.local/share/fonts/PlayfairDisplay-Bold.ttf"),
    "PlayfairDisplay-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSerif-Bold.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSerif-Bold.ttf",
]
_FONT_CANDIDATES_REGULAR = [
    "/usr/share/fonts/truetype/custom/PlayfairDisplay-Regular.ttf",
    "/usr/share/fonts/truetype/fonts-playfair-display/PlayfairDisplay-Regular.ttf",
    "/usr/share/fonts/truetype/PlayfairDisplay-Regular.ttf",
    os.path.expanduser("~/.local/share/fonts/PlayfairDisplay-Regular.ttf"),
    "PlayfairDisplay-Regular.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSerif.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSerif-Regular.ttf",
]

def _download_playfair_if_missing():
    bold_exists = any(os.path.exists(p) for p in _FONT_CANDIDATES_BOLD)
    if bold_exists: return
    log.info("Downloading Playfair Display...")
    vf_url = "https://github.com/google/fonts/raw/main/ofl/playfairdisplay/PlayfairDisplay%5Bwght%5D.ttf"
    for dest in ["PlayfairDisplay-Bold.ttf", "PlayfairDisplay-Regular.ttf"]:
        try:
            r = requests.get(vf_url, timeout=30)
            if r.status_code == 200:
                with open(dest, "wb") as f: f.write(r.content)
        except: pass

_font_cache: dict = {}

def get_font(size, bold=True):
    from PIL import ImageFont
    cache_key = (size, bold)
    if cache_key in _font_cache: return _font_cache[cache_key]
    candidates = _FONT_CANDIDATES_BOLD if bold else _FONT_CANDIDATES_REGULAR
    for path in candidates:
        if os.path.exists(path):
            try:
                font = ImageFont.truetype(path, size)
                _font_cache[cache_key] = font
                return font
            except: continue
    return ImageFont.load_default()

COLORS = {
    "white": (235,235,235),
    "gold":  (212,175,55),
    "cream": (255,245,220),
}

def render_caption(frame, text, color_name, size_name, progress):
    from PIL import Image, ImageDraw, ImageFont
    img  = Image.fromarray(frame)
    W, H = img.size

    if size_name == "large": font_size = int(H * 0.068)
    elif size_name == "medium": font_size = int(H * 0.055)
    else: font_size = int(H * 0.045)
    font_size = max(font_size, 52)
    font_size = min(font_size, 82)

    font  = get_font(font_size, bold=True)
    color = COLORS.get(color_name, COLORS["white"])

    fade  = min(1.0, progress * 4.0) if progress < 0.3 else min(1.0, (1.0 - progress) * 4.0)
    alpha = int(255 * fade)

    words   = text.split()
    lines   = []
    current = ""
    tmp_img  = Image.new("RGBA", (W, H))
    tmp_draw = ImageDraw.Draw(tmp_img)
    for word in words:
        test = f"{current} {word}".strip()
        bb   = tmp_draw.textbbox((0,0), test, font=font)
        if bb[2]-bb[0] > W * 0.75:
            if current: lines.append(current)
            current = word
        else: current = test
    if current: lines.append(current)
    lines = lines[:2]

    line_h  = font_size + 12
    total_h = len(lines) * line_h
    overlay = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw    = ImageDraw.Draw(overlay)
    y_block = int(H * 0.82) - total_h // 2

    for li, line in enumerate(lines):
        bbox = draw.textbbox((0, 0), line, font=font)
        tw   = bbox[2] - bbox[0]
        x    = (W - tw) // 2
        y    = y_block + li * line_h
        stroke = 5
        for dx in range(-stroke, stroke+1, 2):
            for dy in range(-stroke, stroke+1, 2):
                if dx == 0 and dy == 0: continue
                draw.text((x+dx, y+dy), line, font=font, fill=(0,0,0,alpha))
        draw.text((x + 1, y + 1), line, font=font, fill=(0, 0, 0, int(alpha * 0.6)))
        draw.text((x, y), line, font=font, fill=(*color, alpha))

    img = Image.alpha_composite(img.convert("RGBA"), overlay)
    return np.array(img.convert("RGB"))

def get_caption_at_time(captions, t):
    """
    FIXED: Only return captions marked for rendering.
    This prevents captions from showing on every frame.
    """
    for cap in captions:
        # ← KEY FIX: Check if this caption should be rendered
        if not cap.get("render", False):
            continue
            
        if cap["start"] <= t <= cap["end"]:
            dur  = cap["end"] - cap["start"]
            prog = (t - cap["start"]) / dur if dur > 0 else 0.5
            return cap, prog
    
    return None, 0.0  # Return None if no key caption at this time

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

    # audio_offset tracks position in the AUDIO timeline (matches Whisper timestamps)
    # time_offset tracks position in the VIDEO timeline (includes transitions)
    # Captions use audio_offset — NOT time_offset — for correct sync
    audio_offset = 0.0
    time_offset  = 0.0
    last_frame   = None

    for i,(mtype,scene,path) in enumerate(media):
        if audio_offset >= total_duration: break

        clip_dur  = CLIP_DUR_VIDEO if mtype=="video" else CLIP_DUR_IMAGE
        clip_dur  = min(clip_dur, total_duration - audio_offset)

        log.info(f"  [{i+1}/{len(media)}] {mtype}: {scene}/{os.path.basename(path)[:20]}")

        if last_frame is not None:
            next_f = get_first_frame(mtype,path)
            write_transition(writer,last_frame,next_f,get_transition(i))
            # Transitions add video time but NOT audio time
            time_offset += TRANS_DUR

        last_frame = write_clip_frames(
            writer,mtype,path,clip_dur,
            captions,audio_offset,total_duration,
            first_clip=(i==0)
        )
        audio_offset += clip_dur
        time_offset  += clip_dur
        log.info(f"    [audio:{audio_offset:.1f}s / {total_duration:.1f}s]")

    # Fade out
    if last_frame is not None:
        for fi in range(int(2.0*FPS)):
            frame = fade_black(last_frame,fi/(2.0*FPS))
            writer.write(cv2.cvtColor(frame,cv2.COLOR_RGB2BGR))

    writer.release()
    log.info("Frames written")

    # Add audio
    try:
        from moviepy.editor import VideoFileClip, AudioFileClip, CompositeAudioClip
    except ImportError:
        from moviepy import VideoFileClip, AudioFileClip, CompositeAudioClip
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
# SECTION 8 — YOUTUBE UPLOAD (UPDATED: UNIQUE THUMBS & SHEET SYNC)
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
    from googleapiclient.discovery import build
    import google.auth.transport.requests

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

def get_existing_titles():
    """Read all existing titles from sheet column C to check duplicates."""
    try:
        url  = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:json"
        r    = requests.get(url, timeout=15)
        text = r.text
        data = json.loads(text[text.find('{'):text.rfind('}')+1])
        rows = data.get("table",{}).get("rows",[])
        titles = []
        topics = []
        for row in rows[1:]:
            cells = row.get("c",[])
            if cells and len(cells) > 0:
                if cells[0] and cells[0].get("v"):
                    topics.append(cells[0]["v"].strip().lower())
                if len(cells) > 2 and cells[2] and cells[2].get("v"):
                    val = cells[2]["v"].strip()
                    if val and not val.startswith("http"):
                        titles.append(val.lower())
        return titles, topics
    except Exception as e:
        log.warning(f"Could not read existing titles: {e}")
        return [], []

def generate_title(topic, script):
    """Generate title and ensure it's not a duplicate in the sheet."""
    existing_titles, _ = get_existing_titles()

    def _make_title(attempt=0):
        try:
            from groq import Groq
            client = Groq(api_key=GROQ_KEY)
            prompt = f"""Viral YouTube title expert. Topic: {topic}. 
            Generate ONE title: Max 65 chars, starts/ends with emoji, controversial, curiosity gap.
            Output ONLY the title."""

            r = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role":"user","content":prompt}],
                temperature=0.9 + attempt*0.05
            )
            return r.choices[0].message.content.strip().replace('"','')
        except: return f"🔴 {topic} 💀"

    for attempt in range(3):
        title = _make_title(attempt)
        if title.lower() not in existing_titles:
            log.info(f"Title: {title}")
            return title
    return title

def generate_description(topic, script, title, duration):
    """SEO Optimized description with timestamps and CTAs."""
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)
        prompt = f"Write SEO description for '{title}'. Include dark history keywords. 150 words."
        r = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}]
        )
        summary = r.choices[0].message.content.strip()

        ts = "⏱️ CHAPTERS\n00:00 - Introduction\n02:00 - The Mystery\n05:00 - Hidden Truth\n08:00 - Final Verdict"
        cta = f"\n━━━━━━━━━━━━━━━━━━━━━━\n🔔 SUBSCRIBE to {CHANNEL_NAME}\n━━━━━━━━━━━━━━━━━━━━━━\n"
        tags = "#darkhistory #hiddenhistory #mystery #untoldstory"
        
        return f"{summary}\n\n{ts}{cta}\n{tags}"
    except: return f"{topic}\n\n#darkhistory #history"

def get_thumbnail_words(title):
    clean = re.sub(r'[^\x00-\x7F]+', '', title).strip()
    stop = {"the","a","is","and","of","to","in","for","with","this"}
    words = [w for w in clean.split() if w.lower() not in stop and len(w) > 2]
    return words[:3]

def generate_random_hook():
    """
    Generate a random hook phrase for thumbnail variety.
    Ensures each thumbnail has unique emotional impact.
    """
    hooks = [
        "THE TRUTH",
        "DARK SECRETS",
        "HIDDEN HISTORY",
        "UNTOLD STORY",
        "FORBIDDEN TRUTH",
        "LOST HISTORY",
        "BURIED SECRETS",
        "DARK REALITY",
        "SHOCKING TRUTH",
        "ANCIENT EVIL",
        "THE MYSTERY",
        "DARK PAST",
        "HIDDEN EVIL",
        "TRUE STORY",
        "EXPOSED",
    ]
    return random.choice(hooks)


def generate_thumbnail(topic, title):
    """
    FIXED: Fetches relevant image + overlays hook text
    Creates cinematic thumbnail with background image
    """
    from PIL import Image, ImageDraw, ImageFilter
    
    log.info("Generating thumbnail with background image...")
    
    unique_id = int(time.time() * 1000) % 1000000
    final_thumb_path = f"thumb_{unique_id}.jpg"
    
    try:
        # Fetch relevant background image
        bg_image = None
        keywords = get_thumbnail_words(title)
        search_query = " ".join(keywords[:2]) if keywords else topic
        
        # Try Pexels first
        try:
            pexels_url = f"https://api.pexels.com/v1/search?query={search_query}&per_page=1&orientation=landscape"
            r = requests.get(pexels_url, headers={"Authorization": PEXELS_KEY}, timeout=30)
            if r.status_code == 200:
                photos = r.json().get("photos", [])
                if photos:
                    photo_url = photos[0]["src"]["large"]
                    img_data = requests.get(photo_url, timeout=30).content
                    from io import BytesIO
                    bg_image = Image.open(BytesIO(img_data))
        except:
            pass
        
        # Fallback: create dark gradient background
        if bg_image is None:
            bg_image = Image.new("RGB", (1280, 720), color=(10, 5, 15))
        
        # Resize to thumbnail size
        bg_image = bg_image.resize((1280, 720), Image.LANCZOS)
        
        # Darken background for text readability
        dark_overlay = Image.new("RGBA", bg_image.size, (0, 0, 0, 180))
        bg_image = bg_image.convert("RGBA")
        bg_image = Image.alpha_composite(bg_image, dark_overlay)
        bg_image = bg_image.convert("RGB")
        
        # Get hook text
        title_words = get_thumbnail_words(title)
        main_text = " ".join(title_words).upper()[:35]
        hook_text = generate_random_hook()
        
        # Draw on image
        d = ImageDraw.Draw(bg_image)
        font_hook = get_font(80)
        font_main = get_font(120)
        
        gold = (212, 175, 55)
        white = (235, 235, 235)
        red = (240, 60, 60)
        
        # Draw hook (top)
        hook_bbox = d.textbbox((0, 0), hook_text, font=font_hook)
        hook_width = hook_bbox[2] - hook_bbox[0]
        hook_x = (1280 - hook_width) // 2
        
        for adj_x in [-3, -2, -1, 1, 2, 3]:
            for adj_y in [-3, -2, -1, 1, 2, 3]:
                d.text((hook_x + adj_x, 80 + adj_y), hook_text, fill=(0, 0, 0), font=font_hook)
        d.text((hook_x, 80), hook_text, fill=red, font=font_hook)
        
        # Draw main title (center)
        if len(main_text) > 20:
            words = main_text.split()
            mid = len(words) // 2
            line1 = " ".join(words[:mid])
            line2 = " ".join(words[mid:])
            lines = [line1, line2]
        else:
            lines = [main_text]
        
        total_height = len(lines) * 140
        start_y = (720 - total_height) // 2 + 50
        
        for line_idx, line in enumerate(lines):
            line_y = start_y + (line_idx * 140)
            line_bbox = d.textbbox((0, 0), line, font=font_main)
            line_width = line_bbox[2] - line_bbox[0]
            line_x = (1280 - line_width) // 2
            
            for adj_x in [-5, -3, -1, 1, 3, 5]:
                for adj_y in [-5, -3, -1, 1, 3, 5]:
                    d.text((line_x + adj_x, line_y + adj_y), line, fill=(0, 0, 0), font=font_main)
            
            d.text((line_x, line_y), line, fill=gold, font=font_main)
        
        # Add channel name (bottom)
        tagline = CHANNEL_NAME
        tagline_bbox = d.textbbox((0, 0), tagline, font=get_font(40))
        tagline_width = tagline_bbox[2] - tagline_bbox[0]
        tagline_x = (1280 - tagline_width) // 2
        d.text((tagline_x, 650), tagline, fill=white, font=get_font(40))
        
        bg_image.save(final_thumb_path, quality=98)
        log.info(f"✅ Thumbnail with image: {final_thumb_path}")
        return final_thumb_path
        
    except Exception as e:
        log.error(f"❌ Thumbnail generation failed: {e}")
        # Fallback: create simple text-only thumbnail
        try:
            fallback = Image.new("RGB", (1280, 720), color=(15, 5, 5))
            d = ImageDraw.Draw(fallback)
            hook_text = generate_random_hook()
            font = get_font(100)
            d.text((100, 300), hook_text, fill=(212, 175, 55), font=font)
            fallback.save(final_thumb_path, quality=98)
            return final_thumb_path
        except:
            return None
        
    except Exception as e:
        log.error(f"❌ Thumbnail generation failed: {e}")
        import traceback
        log.error(traceback.format_exc())
        return None

def upload_video(video_file, title, description, thumbnail_path):
    """
    UPDATED: Handles the unique thumbnail path and ensures cleanup.
    """
    from googleapiclient.http import MediaFileUpload
    youtube = get_youtube_service()
    if not youtube or not thumbnail_path: return None, None

    publish_at = get_schedule_time()
    body = {
        "snippet": {
            "title": title,
            "description": description,
            "categoryId": "27",
        },
        "status": {
            "privacyStatus": "private",
            "publishAt": None if TEST_MODE else publish_at,
            "selfDeclaredMadeForKids": False,
        }
    }

    try:
        media = MediaFileUpload(video_file, chunksize=5*1024*1024, resumable=True)
        request = youtube.videos().insert(part="snippet,status", body=body, media_body=media)
        
        response = None
        while response is None:
            status, response = request.next_chunk()
            if status: log.info(f" Upload: {int(status.progress()*100)}%")

        video_id = response["id"]
        
        # Set Thumbnail
        youtube.thumbnails().set(
            videoId=video_id,
            media_body=MediaFileUpload(thumbnail_path)
        ).execute()
        
        # Cleanup unique thumbnail file
        if os.path.exists(thumbnail_path):
            os.remove(thumbnail_path)
            
        return video_id, f"https://youtube.com/watch?v={video_id}"
    except Exception as e:
        log.error(f"Upload failed: {e}")
        return None, None

def update_sheet(topic, video_url, title):
    """
    FIXED: Proper service account auth + detailed error logging
    Maps to Columns: A=topic | B=date/time | C=title | D=video_url | E=status
    """
    try:
        log.info(f"Updating sheet for topic: {topic}")
        
        gc = get_sheet_client()
        if not gc:
            log.error("❌ Sheet client unavailable")
            return False
        
        try:
            sh = gc.open_by_key(SHEET_ID)
        except Exception as open_err:
            log.error(f"❌ Cannot open sheet {SHEET_ID}: {open_err}")
            return False
        
        try:
            ws = sh.get_worksheet(0)
        except Exception as ws_err:
            log.error(f"❌ Cannot get worksheet: {ws_err}")
            return False
        
        try:
            col_topics = ws.col_values(1)
        except Exception as col_err:
            log.error(f"❌ Cannot read column A: {col_err}")
            return False
        
        row_idx = None
        for i, val in enumerate(col_topics):
            if val and val.strip().lower() == topic.strip().lower():
                row_idx = i + 1
                break
        
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        status = "Scheduled" if not TEST_MODE else "Private/Test"
        
        if row_idx:
            try:
                ws.update(f"B{row_idx}:E{row_idx}", [[now, title, video_url, status]])
                log.info(f"✅ Sheet updated: Row {row_idx}")
                log.info(f"   Topic: {topic}")
                log.info(f"   URL: {video_url}")
                return True
            except Exception as update_err:
                log.error(f"❌ Failed to update row {row_idx}: {update_err}")
                return False
        else:
            try:
                ws.append_row([topic, now, title, video_url, status])
                log.info(f"✅ Sheet: New row appended")
                log.info(f"   Topic: {topic}")
                log.info(f"   URL: {video_url}")
                return True
            except Exception as append_err:
                log.error(f"❌ Failed to append row: {append_err}")
                return False
            
    except Exception as e:
        log.error(f"❌ Unexpected sheet error: {e}")
        return False

# ══════════════════════════════════════════════════════
# MAIN — RUN ALL SECTIONS (UPDATED)
# ══════════════════════════════════════════════════════
def main():
    log.info("="*50)
    log.info("DarkHistoryMind Pipeline Starting")
    log.info("="*50)

    # Setup
    setup_permanent_files()
    
    # NEW: Ensure fonts are downloaded before Section 4 (Captions) 
    # and Section 8 (Thumbnails) run.
    _download_playfair_if_missing()

    # Section 1 — Topic
    log.info("── SECTION 1: TOPIC ──")
    used  = get_used_topics()
    topic = select_topic(used)
    facts = get_facts(topic)
    hook = generate_hook(topic, facts)
    update_sheet(topic, "PENDING", "GENERATING...")

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

    # Section 4 — Captions (UPDATED with selective rendering)
    log.info("── SECTION 4: CAPTIONS ──")
    total_duration = get_audio_duration()
    captions = generate_captions(script, total_duration, hook)
    
    # Identify key moments for selective caption display
    key_moments = identify_key_moments(script)
    if key_moments:
        # Use AI-identified moments
        captions = mark_key_captions(captions, key_moments)
    else:
        # Fallback: Use keyword-based selection
        captions = mark_captions_by_keywords(captions)

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
    
    # Returns a unique filename (e.g., thumb_171123.jpg) to fix YouTube cache
    thumbnail_path = generate_thumbnail(topic,title)
    
    # Pass the unique path to the upload service
    video_id, video_url = upload_video(output_file, title, description, thumbnail_path)
    
    if video_url:
        update_sheet(topic, video_url, title)
        
        log.info("="*50)
        log.info("PIPELINE COMPLETE")
        log.info(f"Video: {video_url}")
        log.info(f"Title: {title}")
        log.info(f"Scheduled: 8 PM IST")
        log.info("="*50)
        
        # CLEANUP: Remove temporary files to save disk space
        files_to_clean = [output_file, "voiceover.wav"]
        for f in files_to_clean:
            if os.path.exists(f):
                os.remove(f)
                log.info(f"Cleaned up temporary file: {f}")
    else:
        log.error("Upload failed")
        sys.exit(1)

if __name__ == "__main__":
    main()
