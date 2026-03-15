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
    # Resolve fonts first — before any PIL rendering
    _download_playfair_if_missing()
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
# Uses faster-whisper to transcribe voiceover.wav and get
# exact word-level timestamps — captions always match voice
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
        from moviepy.editor import AudioFileClip
        clip = AudioFileClip("voiceover.wav")
        dur  = clip.duration
        clip.close()
        return dur
    except:
        return 420.0

def transcribe_voiceover():
    """
    Use faster-whisper to transcribe voiceover.wav.
    Returns list of word dicts: [{word, start, end}, ...]
    Falls back to empty list if whisper unavailable.
    """
    try:
        from faster_whisper import WhisperModel
        log.info("Transcribing voiceover with Whisper...")
        # base model for long-form — better accuracy than tiny
        model = WhisperModel("base", device="cpu", compute_type="int8")
        segments, info = model.transcribe(
            "voiceover.wav",
            word_timestamps=True,
            language="en",
            beam_size=2,
            vad_filter=True,
        )
        words = []
        for seg in segments:
            if seg.words:
                for w in seg.words:
                    words.append({
                        "word":  w.word.strip(),
                        "start": round(w.start, 3),
                        "end":   round(w.end, 3),
                    })
        log.info(f"Whisper transcribed: {len(words)} words")
        return words
    except ImportError:
        log.warning("faster-whisper not installed — captions will use script timing")
        return []
    except Exception as e:
        log.warning(f"Whisper failed: {e} — using script timing")
        return []

def generate_captions(script, total_duration, hook):
    """
    Build captions from real Whisper word timestamps.
    Groups 3-5 words per caption for long-form readability.
    Keywords highlighted in gold. Hook always shown at start.
    """
    # Get real word timestamps from audio
    word_timings = transcribe_voiceover()

    captions = []

    # Always add hook at start (first 6 seconds)
    captions.append({
        "text":  hook,
        "start": 1.0,
        "end":   6.5,
        "color": "gold",
        "size":  "large"
    })

    if word_timings:
        # Build captions from real timestamps
        i = 0
        while i < len(word_timings):
            # Group 3-5 words per caption for long-form
            gs    = random.choices([3,4,4,5], weights=[20,40,25,15])[0]
            group = word_timings[i:i+gs]
            if not group:
                break

            g_start = group[0]["start"]
            g_end   = group[-1]["end"]
            g_words = [w["word"] for w in group]
            g_text  = " ".join(g_words)

            # Minimum display time 1.0s
            if g_end - g_start < 1.0:
                g_end = g_start + 1.0

            # Cap at next word start to avoid overlap
            if i + gs < len(word_timings):
                next_start = word_timings[i+gs]["start"]
                g_end      = min(g_end, next_start - 0.05)

            g_end = min(g_end, total_duration - 0.2)
            if g_end <= g_start or g_start < 0.5:
                i += gs
                continue

            # Skip if overlaps with hook
            if g_start < 7.0:
                i += gs
                continue

            # Keyword = gold, normal = white
            has_kw = any(w.lower().strip(".,!?;:") in CAPTION_KEYWORDS
                         for w in g_words)
            color  = "gold" if (has_kw and random.random() < 0.25) else "white"

            captions.append({
                "text":  g_text.strip(),
                "start": g_start,
                "end":   g_end,
                "color": color,
                "size":  "large"
            })
            i += gs

        log.info(f"Captions from Whisper: {len(captions)} total")

    else:
        # Fallback: distribute script words evenly across duration
        log.warning("No Whisper timings — distributing script words evenly")
        words    = script.split()
        wdur     = total_duration / max(len(words), 1)
        i        = 0
        t        = 7.5  # start after hook
        gs       = 4

        while i < len(words) and t < total_duration - 2.0:
            group  = words[i:i+gs]
            if not group: break
            cd     = len(group) * wdur
            cd     = max(1.5, min(4.5, cd))
            has_kw = any(w.lower().strip(".,!?;:") in CAPTION_KEYWORDS
                         for w in group)
            captions.append({
                "text":  " ".join(group),
                "start": round(t, 2),
                "end":   round(min(t+cd, total_duration-0.2), 2),
                "color": "gold" if (has_kw and random.random()<0.25) else "white",
                "size":  "large"
            })
            i += gs
            t += cd + 0.1

        log.info(f"Captions from script fallback: {len(captions)} total")

    captions.sort(key=lambda x: x["start"])
    return captions

# ── FONT SETUP ────────────────────────────────────────
# Candidate font paths in priority order — covers GitHub Actions,
# Ubuntu 20/22/24, and any locally installed fallbacks.
_FONT_CANDIDATES_BOLD = [
    # Primary: custom install target
    "/usr/share/fonts/truetype/custom/PlayfairDisplay-Bold.ttf",
    # Fallback 1: fonts-playfair-display package path (Ubuntu)
    "/usr/share/fonts/truetype/fonts-playfair-display/PlayfairDisplay-Bold.ttf",
    # Fallback 2: flat truetype dir
    "/usr/share/fonts/truetype/PlayfairDisplay-Bold.ttf",
    # Fallback 3: local user fonts
    os.path.expanduser("~/.local/share/fonts/PlayfairDisplay-Bold.ttf"),
    # Fallback 4: working directory (downloaded at runtime)
    "PlayfairDisplay-Bold.ttf",
    # Fallback 5: any DejaVu bold present on virtually every Ubuntu runner
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
    """Download Playfair Display from Google Fonts if no candidate exists."""
    urls = {
        "PlayfairDisplay-Bold.ttf": (
            "https://github.com/google/fonts/raw/main/ofl/playfairdisplay/"
            "PlayfairDisplay%5Bwght%5D.ttf"
        ),
        "PlayfairDisplay-Regular.ttf": (
            "https://github.com/google/fonts/raw/main/ofl/playfairdisplay/"
            "PlayfairDisplay%5Bwght%5D.ttf"
        ),
    }
    # Only download if neither bold nor regular exists anywhere
    bold_exists = any(os.path.exists(p) for p in _FONT_CANDIDATES_BOLD)
    if bold_exists:
        return
    log.info("No Playfair Display found — downloading from Google Fonts...")
    # Use the variable font as both bold and regular (PIL picks weight via truetype)
    vf_url = (
        "https://github.com/google/fonts/raw/main/ofl/playfairdisplay/"
        "PlayfairDisplay%5Bwght%5D.ttf"
    )
    for dest in ["PlayfairDisplay-Bold.ttf", "PlayfairDisplay-Regular.ttf"]:
        if os.path.exists(dest):
            continue
        try:
            r = requests.get(vf_url, timeout=30)
            if r.status_code == 200 and len(r.content) > 1000:
                with open(dest, "wb") as f:
                    f.write(r.content)
                log.info(f"  ✅ Downloaded {dest}")
            else:
                log.warning(f"  Font download HTTP {r.status_code}")
        except Exception as e:
            log.warning(f"  Font download failed: {e}")

_font_cache: dict = {}

def get_font(size, bold=True):
    from PIL import ImageFont
    cache_key = (size, bold)
    if cache_key in _font_cache:
        return _font_cache[cache_key]

    candidates = _FONT_CANDIDATES_BOLD if bold else _FONT_CANDIDATES_REGULAR
    for path in candidates:
        if os.path.exists(path):
            try:
                font = ImageFont.truetype(path, size)
                _font_cache[cache_key] = font
                return font
            except Exception:
                continue

    # Last resort: PIL built-in (no size control but never fails)
    log.warning(f"All font candidates failed for size={size} bold={bold} — using default")
    font = ImageFont.load_default()
    _font_cache[cache_key] = font
    return font

COLORS = {
    "white": (235,235,235),
    "gold":  (212,175,55),
    "cream": (255,245,220),
}

def render_caption(frame, text, color_name, size_name, progress):
    from PIL import Image, ImageDraw, ImageFont
    img  = Image.fromarray(frame)
    W, H = img.size

    # Hidden Library style — elegant medium serif
    if size_name == "large":
        font_size = int(H * 0.042)   # ~45px at 1080p
    elif size_name == "medium":
        font_size = int(H * 0.036)
    else:
        font_size = int(H * 0.030)
    font_size = max(font_size, 32)
    font_size = min(font_size, 58)

    # Use regular weight for elegance
    font  = get_font(font_size, bold=False)
    color = COLORS.get(color_name, COLORS["white"])

    # Smooth fade in/out
    fade  = min(1.0, progress * 3.0) if progress < 0.4 \
            else min(1.0, (1.0 - progress) * 3.0)
    alpha = int(255 * fade)

    # Split into lines — max 40 chars per line
    words   = text.split()
    lines   = []
    current = ""
    for word in words:
        test = f"{current} {word}".strip()
        if len(test) <= 40:
            current = test
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    lines = lines[:2]

    line_h  = font_size + 10
    total_h = len(lines) * line_h
    pad_x   = 40
    pad_y   = 18

    # Create overlay
    overlay = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw    = ImageDraw.Draw(overlay)

    # Measure total text block width
    max_tw = 0
    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=font)
        tw   = bbox[2] - bbox[0]
        max_tw = max(max_tw, tw)

    # Position — center horizontal, 82% from top (bottom area)
    block_w = max_tw + pad_x * 2
    block_h = total_h + pad_y * 2
    x_block = (W - block_w) // 2
    y_block = int(H * 0.82) - block_h // 2

    # Dark semi-transparent background box — Hidden Library style
    box_alpha = int(175 * fade)
    draw.rounded_rectangle(
        [(x_block, y_block),
         (x_block + block_w, y_block + block_h)],
        radius=6,
        fill=(8, 6, 4, box_alpha)
    )

    # Subtle top border line — gold
    draw.line(
        [(x_block + 10, y_block),
         (x_block + block_w - 10, y_block)],
        fill=(212, 175, 55, int(120 * fade)),
        width=1
    )

    # Draw each line of text
    for li, line in enumerate(lines):
        bbox = draw.textbbox((0, 0), line, font=font)
        tw   = bbox[2] - bbox[0]
        x    = (W - tw) // 2
        y    = y_block + pad_y + li * line_h

        # Subtle shadow
        draw.text(
            (x + 1, y + 1), line,
            font=font, fill=(0, 0, 0, int(alpha * 0.6))
        )

        # Main text — white clean
        draw.text(
            (x, y), line,
            font=font, fill=(*color, alpha)
        )

    img = Image.alpha_composite(img.convert("RGBA"), overlay)
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
        prompt = f"""You are a viral YouTube title expert for dark history channels.
Topic: {topic}
Script excerpt: {script[:500]}

Write ONE YouTube title following these rules:
- Maximum 65 characters including emojis
- Start with ONE emoji: 🔴 ⚠️ 💀 🔥 🕵️
- Must include a high search volume keyword from the topic
- Extremely controversial — makes viewer angry or shocked
- Creates massive curiosity gap
- End with ONE emoji
- Target both Indian and US audiences
- Must feel like breaking news or forbidden knowledge

High performing examples:
🔴 The Dark Secret Napoleon Took To His Grave 💀
⚠️ What Rome Never Wanted The World To Know 🕵️
🔥 The Man Who Controlled Millions Through Fear 💀
🔴 How The Vatican Buried This Truth For 500 Years 🔥

Output only the title. Nothing else."""

        r     = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}],
            temperature=0.9, max_tokens=100
        )
        title = r.choices[0].message.content.strip()
        title = title.replace('"','').replace("'","").strip()
        log.info(f"Title: {title}")
        return title
    except Exception as e:
        log.warning(f"Title failed: {e}")
        return f"🔴 {topic} 💀"

def generate_description(topic, script, title, duration):
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)
        prompt = f"""Write a fully SEO optimised YouTube description for a dark history documentary.
Title: {title}
Topic: {topic}
Script: {script[:1000]}
Duration: {int(duration//60)} minutes

Structure:
1. First 2 lines — powerful hook (most important for SEO)
2. 3-4 lines — what the video reveals (use keywords naturally)
3. 1 line — curiosity cliffhanger

Rules:
- Use these keywords naturally: dark history, hidden truth, {clean_topic(topic)}, secret history, untold story
- Write conversationally — not like a robot
- Total 120-150 words
- No hashtags in this section

Output only the description text."""

        r       = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}],
            temperature=0.8, max_tokens=400
        )
        summary = r.choices[0].message.content.strip()

        # Timestamps
        mins   = int(duration // 60)
        inter  = max(1, mins // 5)
        labels = [
            "The Hidden Truth Begins",
            "Dark History Revealed",
            "The Real Story Unfolds",
            "The Final Truth"
        ]
        ts = "⏱️ CHAPTERS\n00:00 - Introduction\n"
        for i in range(1, 5):
            ts += f"{str(i*inter).zfill(2)}:00 - {labels[i-1]}\n"

        # CTA
        cta = (
            f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔔 SUBSCRIBE to {CHANNEL_NAME} — New dark history every 2 days\n"
            f"👍 LIKE if this changed how you see history\n"
            f"💬 COMMENT your thoughts below\n"
            f"🔁 SHARE with someone who needs to know this\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
        )

        # SEO hashtags — 30+ targeted
        search      = clean_topic(topic)
        topic_words = [w for w in search.lower().split() if len(w) > 3]
        base_tags   = [f"#{w}" for w in topic_words]
        power_tags  = [
            "#darkhistory", "#hiddenhistory", "#secrethistory",
            "#historyfacts", "#untoldhistory", "#historydocumentary",
            "#ancienthistory", "#historymystery", "#conspiracytheory",
            "#historicalfacts", "#darktruths", "#forbiddenknowledge",
            "#historylovers", "#historychannel", "#educationalvideo",
            "#mystery", "#truth", "#secrets", "#documentary",
            "#history", "#darkfacts", "#hiddentruth",
            "#historybuff", "#ancientmysteries", "#losthistory",
            "#india", "#indianhistory", "#worldhistory",
            "#viralhistory", "#mindblowing"
        ]
        all_tags = " ".join(base_tags + power_tags)

        return f"{summary}\n\n{ts}{cta}\n{all_tags}"

    except Exception as e:
        log.warning(f"Description failed: {e}")
        return f"{topic}\n\n#darkhistory #history #documentary"

def get_thumbnail_words(title):
    """Extract 2-3 most impactful words for thumbnail."""
    clean = re.sub(r'[^\x00-\x7F]+', '', title).strip()
    # Remove common filler words
    stop  = {
        "the","a","an","of","to","in","is","are","was","were",
        "and","or","but","for","with","that","this","from","by",
        "at","on","as","its","it","be","has","had","have","they",
        "their","his","her","our","your","what","how","why","who",
        "did","do","does","not","no","never","always","ever","real",
        "dark","truth","hidden","secret","story","history","about"
    }
    words    = [w for w in clean.split() if w.lower() not in stop and len(w) > 2]
    # Pick 2-3 most powerful words
    selected = words[:3] if len(words) >= 3 else words
    return selected

def generate_thumbnail(topic, title):
    from PIL import Image, ImageDraw, ImageFont
    log.info("Generating thumbnail...")
    search = clean_topic(topic)
    ai_ok  = False

    # Try AI image generation
    for seed in [42, 123, 777, 999, 555, 333]:
        try:
            prompt = (
                f"ultra dramatic dark cinematic {search} "
                f"mysterious figure dramatic shadows "
                f"sepia tone historical epic atmosphere "
                f"high contrast moody lighting "
                f"no text no watermark professional photo"
            )
            encoded = requests.utils.quote(prompt)
            url     = (
                f"https://image.pollinations.ai/prompt/{encoded}"
                f"?width=1280&height=720&nologo=true&seed={seed}"
            )
            r = requests.get(url, timeout=120)
            if r.status_code == 200 and len(r.content) > 5000:
                with open("thumb_base.jpg","wb") as f:
                    f.write(r.content)
                ai_ok = True
                log.info(f"AI thumbnail ready (seed {seed})")
                break
        except:
            continue

    # Fallback to best asset image
    if not ai_ok:
        log.warning("AI failed — using asset image")
        for scene in ["mystery","explanation","insight","reflection"]:
            folder = f"assets/images/{scene}"
            if os.path.exists(folder):
                imgs = [f for f in os.listdir(folder) if f.endswith(".jpg")]
                if imgs:
                    import shutil
                    shutil.copy(f"{folder}/{imgs[0]}", "thumb_base.jpg")
                    break

    try:
        img = Image.open("thumb_base.jpg").convert("RGB")
        img = img.resize((1280, 720), Image.LANCZOS)

        # ── DARK OVERLAY ──────────────────────────────
        overlay = Image.new("RGBA", (1280, 720), (0,0,0,0))
        od      = ImageDraw.Draw(overlay)

        # Heavy bottom gradient — text area
        for y in range(200, 720):
            alpha = int(230 * (y - 200) / 520)
            od.line([(0,y),(1280,y)], fill=(0,0,0,alpha))

        # Left and right dark edges
        for x in range(150):
            alpha = int(80 * (1 - x/150))
            od.line([(x,0),(x,720)], fill=(0,0,0,alpha))
            od.line([(1280-x,0),(1280-x,720)], fill=(0,0,0,alpha))

        # Top dark strip
        for y in range(80):
            alpha = int(60 * (1 - y/80))
            od.line([(0,y),(1280,y)], fill=(0,0,0,alpha))

        img = Image.alpha_composite(
            img.convert("RGBA"), overlay
        ).convert("RGB")

        # ── FONTS ─────────────────────────────────────
        font_huge    = get_font(115, bold=True)
        font_big     = get_font(88,  bold=True)
        font_channel = get_font(30,  bold=True)
        font_tag     = get_font(24,  bold=False)

        draw = ImageDraw.Draw(img)

        # ── THUMBNAIL TEXT — 2-3 BIG WORDS ────────────
        thumb_words = get_thumbnail_words(title)

        if len(thumb_words) >= 2:
            # Line 1 — first word/s in WHITE — huge
            line1 = " ".join(thumb_words[:2]).upper()
            # Line 2 — last word in RED — accent
            line2 = thumb_words[-1].upper() if len(thumb_words) >= 3 else ""
        else:
            line1 = thumb_words[0].upper() if thumb_words else "DARK"
            line2 = "SECRET"

        # Draw line 1 — WHITE massive text
        bbox1 = draw.textbbox((0,0), line1, font=font_huge)
        tw1   = bbox1[2] - bbox1[0]
        x1    = (1280 - tw1) // 2
        y1    = 390

        # 12-layer black shadow for line 1
        for dx, dy in [
            (-5,-5),(5,-5),(-5,5),(5,5),
            (-8,0),(8,0),(0,-8),(0,8),
            (-10,0),(10,0),(0,-10),(0,10)
        ]:
            draw.text((x1+dx, y1+dy), line1,
                     font=font_huge, fill=(0,0,0,255))

        # White main text line 1
        draw.text((x1, y1), line1,
                 font=font_huge, fill=(255,255,255,255))

        # Draw line 2 — RED accent word
        if line2:
            bbox2 = draw.textbbox((0,0), line2, font=font_big)
            tw2   = bbox2[2] - bbox2[0]
            x2    = (1280 - tw2) // 2
            y2    = y1 + 125

            # Red glow background
            glow_pad = 20
            draw.rectangle(
                [(x2 - glow_pad, y2 - 8),
                 (x2 + tw2 + glow_pad, y2 + 95)],
                fill=(160, 15, 15)
            )

            # Shadow for line 2
            for dx, dy in [(-4,-4),(4,-4),(-4,4),(4,4)]:
                draw.text((x2+dx, y2+dy), line2,
                         font=font_big, fill=(0,0,0,255))

            # Red/white text line 2
            draw.text((x2, y2), line2,
                     font=font_big, fill=(255,255,255,255))

        # ── RED ACCENT LINE above text ─────────────────
        draw.rectangle(
            [(x1, y1 - 18),(x1 + min(tw1, 300), y1 - 10)],
            fill=(200, 20, 20)
        )

        # ── CHANNEL NAME — top left ────────────────────
        draw.text(
            (30, 22), CHANNEL_NAME,
            font=font_channel, fill=(212,175,55)
        )

        # ── BOTTOM BAR ────────────────────────────────
        draw.rectangle([(0,672),(1280,720)], fill=(8,5,3))
        draw.text(
            (35, 684),
            "DARK HISTORY  •  HIDDEN TRUTH  •  CLASSIFIED",
            font=font_tag, fill=(180,140,40)
        )

        # ── GOLD TOP BORDER ────────────────────────────
        draw.line([(0,0),(1280,0)], fill=(212,175,55), width=5)

        img.save("thumbnail.jpg", quality=98)
        if os.path.exists("thumb_base.jpg"):
            os.remove("thumb_base.jpg")
        log.info("Thumbnail saved")
        return "thumbnail.jpg"

    except Exception as e:
        log.warning(f"Thumbnail failed: {e}")
        # Clean fallback
        img  = Image.new("RGB",(1280,720),(8,5,3))
        draw = ImageDraw.Draw(img)
        font = get_font(100, bold=True)
        clean = re.sub(r'[^\x00-\x7F]+','',title).strip()
        words = get_thumbnail_words(clean)
        line1 = " ".join(words[:2]).upper() if words else "DARK HISTORY"
        line2 = words[-1].upper() if len(words) >= 3 else "REVEALED"
        for line, y, color in [
            (line1, 280, (255,255,255)),
            (line2, 420, (220,30,30))
        ]:
            bbox = draw.textbbox((0,0),line,font=font)
            tw   = bbox[2]-bbox[0]
            draw.text(((1280-tw)//2,y),line,font=font,fill=color)
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
                "dark history","hidden history","secret history",
                "history documentary","history facts",
                "untold history","ancient history","mystery",
                "historical facts","educational","darkhistorymind",
                "conspiracy","forbidden knowledge","dark truths",
                "history channel","history mystery","lost history",
                "ancient mysteries","world history","india history",
                "dark facts","hidden truth","real history",
                "history secrets","historical documentary",
                "mindblowing history","viral history",
                "history buff","unknown history","classified"
            ],
            "categoryId": "27",
            "defaultLanguage": "en",
        },
        "status": {
            "privacyStatus": "private",
            "publishAt": None if TEST_MODE else publish_at,
            "selfDeclaredMadeForKids": False,
        }
    }
    if TEST_MODE:
        log.info("TEST MODE — uploading as Private, no schedule")

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
