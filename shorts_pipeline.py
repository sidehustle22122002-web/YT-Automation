#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════
# DarkHistoryMind — YouTube Shorts Pipeline (Refined v2)
# FIX 1: Word-level caption sync using Edge TTS word boundaries
# FIX 2: Music — yt-dlp YouTube Audio Library → fallback background.mp3
# FIX 3: Simple hard cut only transitions (no complex fades)
# FIX 4: 1080p quality — high-res assets, CRF 18, preset slow
# FIX 5: Duration enforced 30–40 seconds via script prompt + TTS
# ═══════════════════════════════════════════════════════════════
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
GDRIVE_MUSIC_ID   = os.environ["GDRIVE_MUSIC_ID"]
SHEET_ID          = os.environ["SHEET_ID"]
LONG_VIDEO_TOPIC  = os.environ.get("LONG_VIDEO_TOPIC", "")
TEST_MODE         = os.environ.get("TEST_MODE", "false").lower() == "true"
CHANNEL_NAME      = "DarkHistoryMind"

# ── VIDEO SPECS ────────────────────────────────────────────────
SW, SH        = 1080, 1920   # 9:16 vertical
FPS           = 30
MIN_DUR       = 30.0         # FIX 5: minimum 30s
MAX_DUR       = 40.0         # FIX 5: maximum 40s
# FIX 4: high quality encoding
VIDEO_CRF     = "18"         # visually lossless (lower = better)
VIDEO_PRESET  = "slow"       # better compression at same quality
VIDEO_BITRATE = "8M"         # minimum bitrate floor

# ── CAPTION STYLE ─────────────────────────────────────────────
CAPTION_SIZE  = 105
HOOK_SIZE     = 120
STROKE_W      = 7
CENTER_Y      = 0.50
SAFE_TOP      = 0.10
SAFE_BOTTOM   = 0.25

# ── FONT CANDIDATES ───────────────────────────────────────────
_FONTS = [
    "/usr/share/fonts/truetype/montserrat/Montserrat-ExtraBold.ttf",
    "/usr/share/fonts/truetype/fonts-montserrat/Montserrat-ExtraBold.ttf",
    "/usr/share/fonts/truetype/custom/Montserrat-ExtraBold.ttf",
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
        if os.path.exists(fname): continue
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200 and len(r.content) > 1000:
                open(fname, "wb").write(r.content)
                log.info(f"Font: {fname}")
        except Exception as e:
            log.warning(f"Font {fname}: {e}")

def get_font(size):
    from PIL import ImageFont
    if size in _FC: return _FC[size]
    for p in _FONTS:
        if os.path.exists(p):
            try:
                f = ImageFont.truetype(p, size); _FC[size] = f; return f
            except: continue
    f = ImageFont.load_default(); _FC[size] = f; return f

# ═══════════════════════════════════════════════════════════════
# FIX 2 — MUSIC: yt-dlp YouTube Audio Library → fallback background.mp3
# ═══════════════════════════════════════════════════════════════
# Curated YouTube Audio Library tracks — free, no copyright
# These are stable audio IDs from YouTube's free music library
YT_AUDIO_LIBRARY_SEARCHES = [
    "epic history cinematic dark no copyright",
    "ancient civilization dramatic orchestral free",
    "dark historical documentary music free",
    "cinematic epic orchestral history free music",
]

def resolve_music():
    """
    Resolve music file once before assembly.
    Priority: shorts_music.mp3 → yt-dlp → Pixabay → background.mp3
    Returns filepath or None.
    """
    if os.path.exists("shorts_music.mp3") and os.path.getsize("shorts_music.mp3") > 10000:
        log.info("Shorts music already downloaded")
        return "shorts_music.mp3"
    if os.path.exists("background.mp3") and os.path.getsize("background.mp3") > 10000:
        log.info("Using background.mp3 as music")
        return "background.mp3"
    return download_music()

def download_music():
    if os.path.exists("shorts_music.mp3") and os.path.getsize("shorts_music.mp3") > 10000:
        log.info("Shorts music exists")
        return "shorts_music.mp3"

    # Attempt 1: yt-dlp from YouTube Audio Library
    log.info("Trying YouTube Audio Library via yt-dlp...")
    for search in YT_AUDIO_LIBRARY_SEARCHES:
        try:
            cmd = [
                "yt-dlp",
                f"ytsearch1:{search} site:youtube.com/audiolibrary",
                "--extract-audio",
                "--audio-format", "mp3",
                "--audio-quality", "0",
                "--output", "shorts_music.%(ext)s",
                "--no-playlist",
                "--quiet",
                "--max-downloads", "1",
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            if os.path.exists("shorts_music.mp3") and os.path.getsize("shorts_music.mp3") > 10000:
                log.info("✅ Music from YouTube Audio Library")
                return "shorts_music.mp3"
        except Exception as e:
            log.warning(f"yt-dlp attempt: {e}")

    # Attempt 2: yt-dlp general YouTube search for royalty-free epic music
    log.info("Trying yt-dlp general search...")
    try:
        cmd = [
            "yt-dlp",
            "ytsearch3:epic dark history background music no copyright free use",
            "--extract-audio",
            "--audio-format", "mp3",
            "--audio-quality", "0",
            "--output", "shorts_music.%(ext)s",
            "--no-playlist",
            "--quiet",
            "--match-filter", "duration < 300",
            "--max-downloads", "1",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
        if os.path.exists("shorts_music.mp3") and os.path.getsize("shorts_music.mp3") > 10000:
            log.info("✅ Music from YouTube search")
            return "shorts_music.mp3"
    except Exception as e:
        log.warning(f"yt-dlp search: {e}")

    # Attempt 3: Pixabay free music API
    log.info("Trying Pixabay music...")
    try:
        r = requests.get(
            f"https://pixabay.com/api/?key={PIXABAY_KEY}"
            f"&q=epic+cinematic+history&media_type=music&per_page=5",
            timeout=20
        )
        for hit in r.json().get("hits", []):
            au = hit.get("audio", {}).get("url", "")
            if not au: continue
            ar = requests.get(au, timeout=30)
            if ar.status_code == 200 and len(ar.content) > 10000:
                open("shorts_music.mp3", "wb").write(ar.content)
                log.info("✅ Music from Pixabay")
                return "shorts_music.mp3"
    except Exception as e:
        log.warning(f"Pixabay music: {e}")

    # Attempt 4: Fallback to long-form background.mp3
    if os.path.exists("background.mp3"):
        log.info("✅ Using background.mp3 (long-form fallback)")
        return "background.mp3"

    log.warning("No music found — voice only")
    return None

# ═══════════════════════════════════════════════════════════════
# SECTION 1 — SCRIPT
# FIX 5: Script prompt enforces 55-70 words → ~30-38s at natural pace
# ═══════════════════════════════════════════════════════════════
def generate_script(topic):
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)
        prompt = f"""Write a viral YouTube Shorts dark history script.
Topic: {topic}

TARGET: exactly 55-65 words total when spoken = 30-38 seconds at natural pace.

Structure (word budget per section):
[HOOK] 6-8 words — ONE brutal shocking statement. No questions. Present tense.
[FACT1] 10-12 words — First dark fact.
[FACT2] 10-12 words — Second darker fact.
[STORY] 15-18 words — The shocking core truth.
[CONCLUSION] 10-12 words — Haunting final line. End with a statement not a question.

CRITICAL RULES:
- TOTAL script must be 55-65 words. Count carefully.
- Cold documentary tone. TRUE facts only.
- No Welcome, Today, In this video, Subscribe.
- Short punchy sentences only.
- Each section flows into next like a thriller.

Count the words in full_script — it MUST be 55-65 words.

Return ONLY valid JSON:
{{"hook":"","fact1":"","fact2":"","story":"","conclusion":"","full_script":"complete 55-65 word script"}}"""

        r    = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}],
            temperature=0.85, max_tokens=600
        )
        text = r.choices[0].message.content.strip()
        text = text.replace("```json","").replace("```","").strip()
        data = json.loads(text[text.find('{'):text.rfind('}')+1])
        wc   = len(data.get("full_script","").split())
        log.info(f"Script: {wc} words | Hook: {data.get('hook','')}")
        return data
    except Exception as e:
        log.error(f"Script: {e}")
        return None

# ═══════════════════════════════════════════════════════════════
# VOICE + WORD-LEVEL TIMING
# Step 1: edge-tts communicate.save() — reliable audio generation
# Step 2: faster-whisper word timestamps from the saved audio
# Step 3: fallback to even-distribution if whisper unavailable
# ═══════════════════════════════════════════════════════════════
def generate_voice_with_timing(script_data):
    """
    Reliable 2-step approach:
    1. Generate audio with edge-tts communicate.save() (always works)
    2. Extract word timestamps with faster-whisper (free, local, accurate)
    Returns (duration_seconds, word_timings_list)
    """
    full_script = script_data.get("full_script", "")
    if not full_script.strip():
        log.error("Empty script — cannot generate voice")
        return 0.0, []

    # STEP 1: Generate audio with edge-tts
    try:
        import asyncio, nest_asyncio, edge_tts
        nest_asyncio.apply()

        async def _save_audio():
            communicate = edge_tts.Communicate(
                full_script,
                voice="en-GB-ThomasNeural",
                rate="+0%",
                pitch="-8Hz"
            )
            await communicate.save("shorts_raw_voice.mp3")

        asyncio.run(_save_audio())

        if not os.path.exists("shorts_raw_voice.mp3") or            os.path.getsize("shorts_raw_voice.mp3") < 1000:
            log.error("Edge TTS produced no audio file")
            return 0.0, []

        log.info("Voice audio saved")

        from pydub import AudioSegment
        audio = AudioSegment.from_mp3("shorts_raw_voice.mp3").set_frame_rate(48000)
        audio = audio.apply_gain(-audio.dBFS + (-14))
        dur   = len(audio) / 1000
        audio.export("shorts_voice.wav", format="wav")
        log.info(f"Voice: {dur:.2f}s")

    except Exception as e:
        log.error(f"Edge TTS failed: {e}")
        return 0.0, []

    # STEP 2: Get word timestamps with faster-whisper
    word_timings = []
    try:
        from faster_whisper import WhisperModel
        log.info("Running Whisper for word timestamps...")
        model = WhisperModel("tiny", device="cpu", compute_type="int8")
        segments, _ = model.transcribe(
            "shorts_voice.wav",
            word_timestamps=True,
            language="en",
            beam_size=1,
            vad_filter=True,
        )
        for seg in segments:
            if seg.words:
                for w in seg.words:
                    word_timings.append({
                        "word":  w.word.strip(),
                        "start": round(w.start, 3),
                        "end":   round(w.end, 3),
                    })
        log.info(f"Whisper: {len(word_timings)} word timestamps")
    except ImportError:
        log.warning("faster-whisper not installed — using even distribution")
    except Exception as e:
        log.warning(f"Whisper failed: {e} — using even distribution")

    # STEP 3: Fallback — evenly distribute words over duration
    if not word_timings:
        log.info("Building estimated word timings...")
        words = full_script.split()
        wdur  = dur / max(len(words), 1)
        t     = 0.0
        for w in words:
            word_timings.append({
                "word":  w,
                "start": round(t, 3),
                "end":   round(t + wdur * 0.9, 3),
            })
            t += wdur
        log.info(f"Estimated {len(word_timings)} word timings")

    return dur, word_timings

def get_audio_dur():
    try:
        from moviepy.editor import AudioFileClip
        c = AudioFileClip("shorts_voice.wav"); d = c.duration; c.close(); return d
    except: return 35.0

# ═══════════════════════════════════════════════════════════════
# FIX 1 — CAPTIONS built from REAL word timestamps
# Groups 2-4 words using actual TTS timing data
# ═══════════════════════════════════════════════════════════════
KEYWORDS = {
    "empire","emperor","king","queen","pharaoh","caesar","blood","death",
    "war","battle","massacre","execution","torture","secret","hidden",
    "forbidden","truth","betrayal","vanished","destroyed","murdered",
    "million","billion","ancient","medieval","roman","greek","egypt",
    "persian","mongol","viking","spartan","never","only","first","last",
    "dark","evil","brutal","savage","ruthless","feared","powerful","night",
    "centuries","single","entire","real","true","buried","erased",
}

def build_synced_captions(script_data, word_timings, total_dur):
    """
    Build captions using REAL word boundary timestamps from Edge TTS.
    Groups 2-4 words per caption for readability.
    """
    if not word_timings:
        log.warning("No word timings — falling back to estimated captions")
        return build_estimated_captions(script_data, total_dur)

    hook_text = script_data.get("hook", "").strip()
    hook_words = hook_text.lower().split()
    captions   = []

    # ── HOOK: first N words as single big caption ──────────────
    n_hook = len(hook_words)
    if n_hook > 0 and len(word_timings) >= n_hook:
        hook_start = word_timings[0]["start"]
        hook_end   = word_timings[min(n_hook-1, len(word_timings)-1)]["end"]
        # Hold hook for at least 1.8s for impact
        hook_end   = max(hook_end, hook_start + 1.8)
        captions.append({
            "text":    hook_text.upper(),
            "start":   hook_start,
            "end":     min(hook_end, total_dur - 0.1),
            "is_hook": True,
            "emphasis":True,
            "color":   "yellow",
        })
        remaining = word_timings[n_hook:]
    else:
        remaining = word_timings

    # ── BODY: group 2-4 words using real timestamps ────────────
    i = 0
    while i < len(remaining):
        # Group size: prefer 3 words
        gs    = random.choices([2,3,3,4], weights=[15,45,25,15])[0]
        group = remaining[i:i+gs]
        if not group: break

        g_start = group[0]["start"]
        g_end   = group[-1]["end"]
        g_words = [w["word"] for w in group]
        g_text  = " ".join(g_words).upper()

        # Enforce minimum display time of 0.5s for readability
        if g_end - g_start < 0.5:
            g_end = g_start + 0.5

        # Cap at actual next word start to avoid overlap
        if i + gs < len(remaining):
            next_start = remaining[i+gs]["start"]
            g_end      = min(g_end, next_start - 0.02)

        g_end = min(g_end, total_dur - 0.1)
        if g_end <= g_start:
            i += gs; continue

        has_k = any(w.lower().strip(".,!?;:'\"") in KEYWORDS for w in g_words)
        emph  = has_k and random.random() < 0.35

        captions.append({
            "text":    g_text,
            "start":   round(g_start, 3),
            "end":     round(g_end, 3),
            "is_hook": False,
            "emphasis":emph,
            "color":   "yellow" if emph else "white",
        })
        i += gs

    log.info(f"Synced captions: {len(captions)} groups from {len(word_timings)} words")
    return captions

def build_estimated_captions(script_data, total_dur):
    """Fallback: estimate timing when word_boundary data unavailable."""
    words    = script_data.get("full_script","").split()
    hook     = script_data.get("hook","")
    if not words: return []
    wdur     = total_dur / max(len(words), 1)
    captions = []
    hook_end = min(1.8, len(hook.split()) * wdur * 1.1)
    captions.append({"text":hook.upper(),"start":0.1,"end":hook_end,
                     "is_hook":True,"emphasis":True,"color":"yellow"})
    i = len(hook.split()); t = hook_end + 0.05
    while i < len(words) and t < total_dur - 0.3:
        gs    = random.choices([2,3,3,4], weights=[15,45,25,15])[0]
        group = words[i:i+gs]
        if not group: break
        cd    = max(0.5, min(1.4, len(group)*wdur))
        has_k = any(w.lower().strip(".,!?;:") in KEYWORDS for w in group)
        emph  = has_k and random.random() < 0.35
        captions.append({"text":" ".join(group).upper(),"start":round(t,3),
                         "end":round(min(t+cd,total_dur-0.1),3),
                         "is_hook":False,"emphasis":emph,"color":"yellow" if emph else "white"})
        i += gs; t += cd + 0.03
    log.info(f"Estimated captions: {len(captions)}")
    return captions

# ═══════════════════════════════════════════════════════════════
# SECTION 4 — ASSETS (FIX 4: high-res only)
# ═══════════════════════════════════════════════════════════════
SCENE_KW = {
    "hook":       ["ancient ruins dramatic dark","mysterious fortress medieval dark"],
    "fact1":      ["ancient manuscript historical sepia","old parchment candlelight library"],
    "fact2":      ["roman ruins cinematic dramatic","greek statue dramatic shadow"],
    "story":      ["battle painting historical dramatic","medieval war scene painting"],
    "climax":     ["dramatic ancient empire ruins","mysterious dark historical shadows"],
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
    """Smart center-crop to 9:16, always output SW×SH at full 1080p."""
    from PIL import Image
    W, H  = img.size
    tw    = int(H * 9 / 16)
    if tw <= W:
        l   = (W - tw) // 2
        img = img.crop((l, 0, l+tw, H))
    else:
        th  = int(W * 16 / 9)
        ni  = Image.new("RGB", (W, th), (8, 5, 3))
        ni.paste(img, (0, (th-H)//2))
        img = ni
    # FIX 4: always upscale to full 1080×1920 with LANCZOS
    return img.resize((SW, SH), Image.LANCZOS)

def fetch_image(query, idx):
    """
    FIX 4: Request highest resolution available.
    Pexels: original (4K+) | Pixabay: largeImageURL | AI: 1080×1920
    """
    from PIL import Image
    import io

    # Pexels — request original resolution
    try:
        r = requests.get(
            "https://api.pexels.com/v1/search",
            headers={"Authorization": PEXELS_KEY},
            params={"query": query, "per_page": 5, "orientation": "landscape"},
            timeout=20
        )
        photos = r.json().get("photos", [])
        if photos:
            photo = random.choice(photos)
            # Try original first, then large2x
            url   = photo["src"].get("original") or photo["src"]["large2x"]
            data  = requests.get(url, timeout=45).content
            img   = Image.open(io.BytesIO(data)).convert("RGB")
            # Only use if high enough resolution
            if img.width >= 1200 or img.height >= 1200:
                path = f"shorts_assets/img_{idx}.jpg"
                crop_916(img).save(path, quality=95, subsampling=0)
                log.info(f"  Pexels {img.width}×{img.height} → {query[:25]}")
                return path
    except Exception as e:
        log.warning(f"Pexels: {e}")

    # Pixabay — request large image
    try:
        r    = requests.get(
            f"https://pixabay.com/api/?key={PIXABAY_KEY}"
            f"&q={query.replace(' ','+')}&image_type=photo"
            f"&orientation=horizontal&per_page=5&order=popular&min_width=1280",
            timeout=20
        )
        hits = r.json().get("hits", [])
        if hits:
            hit  = random.choice(hits)
            url  = hit.get("largeImageURL", "")
            data = requests.get(url, timeout=45).content
            img  = Image.open(io.BytesIO(data)).convert("RGB")
            path = f"shorts_assets/img_{idx}.jpg"
            crop_916(img).save(path, quality=95, subsampling=0)
            log.info(f"  Pixabay {img.width}×{img.height} → {query[:25]}")
            return path
    except Exception as e:
        log.warning(f"Pixabay: {e}")

    # Pollinations AI — native 1080×1920
    try:
        prompt = f"ultra detailed dark cinematic historical {query} dramatic sepia 4k no text no watermark"
        url    = (f"https://image.pollinations.ai/prompt/{requests.utils.quote(prompt)}"
                  f"?width=1080&height=1920&nologo=true&seed={idx*7}&enhance=true")
        r = requests.get(url, timeout=90)
        if r.status_code == 200 and len(r.content) > 20000:
            img  = Image.open(io.BytesIO(r.content)).convert("RGB").resize((SW,SH), Image.LANCZOS)
            path = f"shorts_assets/img_{idx}.jpg"
            img.save(path, quality=95, subsampling=0)
            log.info(f"  AI 1080×1920 → {query[:25]}")
            return path
    except Exception as e:
        log.warning(f"AI image: {e}")

    return None

def fetch_assets(topic):
    os.makedirs("shorts_assets", exist_ok=True)
    assets  = []
    idx     = 0
    clean_t = clean_topic(topic)
    sections = ["hook","fact1","fact2","story","story","climax","climax","conclusion"]
    for section in sections:
        kw   = random.choice(SCENE_KW.get(section, SCENE_KW["hook"])) + f" {clean_t}"
        path = fetch_image(kw, idx)
        if path: assets.append(path)
        idx += 1; time.sleep(0.4)
    while len(assets) < 6:
        kw   = random.choice(SCENE_KW["story"]) + f" {clean_t}"
        path = fetch_image(kw, idx)
        if path: assets.append(path)
        idx += 1
    log.info(f"Assets: {len(assets)}")
    return assets

# ═══════════════════════════════════════════════════════════════
# SECTION 5 — COLOR GRADING (FIX 4: quality preserved)
# ═══════════════════════════════════════════════════════════════
def grade(frame):
    """Dark cinematic grade — sepia, vignette, grain, warm tones."""
    img = frame.astype(np.float32) / 255.0
    # Exposure
    img = np.clip(img * 0.78, 0, 1)
    # Contrast
    img = np.clip(img + 1.25 * 0.10 * (img - 0.5), 0, 1)
    # Warm color temp
    img[:,:,0] = np.clip(img[:,:,0] + 0.05, 0, 1)
    img[:,:,2] = np.clip(img[:,:,2] - 0.05, 0, 1)
    # Sepia blend 30%
    gray  = 0.299*img[:,:,0] + 0.587*img[:,:,1] + 0.114*img[:,:,2]
    sepia = np.stack([np.clip(gray*1.08,0,1), np.clip(gray*0.86,0,1), np.clip(gray*0.67,0,1)], axis=2)
    img   = np.clip(img * 0.70 + sepia * 0.30, 0, 1)
    # Vignette
    H, W  = img.shape[:2]
    Y, X  = np.ogrid[:H, :W]
    dist  = np.sqrt(((X-W/2)/(W/2))**2 + ((Y-H/2)/(H/2))**2)
    img   = np.clip(img * (1 - np.clip(dist*0.60, 0, 0.65))[:,:,np.newaxis], 0, 1)
    # Light grain (15/100)
    img   = np.clip(img + np.random.normal(0, 0.013, img.shape).astype(np.float32), 0, 1)
    return (img * 255).astype(np.uint8)

# ═══════════════════════════════════════════════════════════════
# SECTION 6 — CAPTION RENDERING
# Pop-in scale animation: 85%→100% in 0.2s
# Heavy black stroke (7px) — readable on any background
# ═══════════════════════════════════════════════════════════════
def render_frame(frame_rgb, t, captions):
    from PIL import Image, ImageDraw
    img  = Image.fromarray(frame_rgb).convert("RGBA")
    W, H = img.size

    cap = next((c for c in captions if c["start"] <= t <= c["end"]), None)
    if cap is None:
        return np.array(img.convert("RGB"))

    cap_t = t - cap["start"]
    # Pop-in animation
    ap    = min(1.0, cap_t / 0.20)
    fp    = min(1.0, cap_t / 0.12)
    scale = 0.85 + 0.15 * (1 - (1 - ap) ** 3)
    alpha = int(255 * fp)

    base  = HOOK_SIZE if cap.get("is_hook") else CAPTION_SIZE
    if cap.get("emphasis"): base = int(base * 1.05)
    fsize = max(60, int(base * scale))
    font  = get_font(fsize)

    tcol  = (255, 230, 0, alpha) if (cap.get("is_hook") or cap.get("emphasis")) else (245, 245, 245, alpha)
    scol  = (0, 0, 0, alpha)

    # Word wrap: 68% screen width
    text  = cap["text"]
    lines, cur = [], ""
    td = ImageDraw.Draw(Image.new("RGBA", (W, H)))
    for w in text.split():
        test = f"{cur} {w}".strip()
        if td.textbbox((0,0), test, font=font)[2] > W * 0.68:
            if cur: lines.append(cur)
            cur = w
        else: cur = test
    if cur: lines.append(cur)

    lh    = fsize + 14
    th    = len(lines) * lh
    ys    = int(H * CENTER_Y) - th // 2
    ys    = max(int(H * SAFE_TOP), min(int(H * (1 - SAFE_BOTTOM)) - th, ys))

    ov = Image.new("RGBA", (W, H), (0,0,0,0))
    od = ImageDraw.Draw(ov)

    for li, line in enumerate(lines):
        bb = od.textbbox((0,0), line, font=font)
        x  = (W - (bb[2]-bb[0])) // 2
        y  = ys + li * lh
        # 7px black stroke
        for dx in range(-STROKE_W, STROKE_W+1, 2):
            for dy in range(-STROKE_W, STROKE_W+1, 2):
                if dx==0 and dy==0: continue
                od.text((x+dx, y+dy), line, font=font, fill=scol)
        od.text((x, y), line, font=font, fill=tcol)

    img = Image.alpha_composite(img, ov)
    return np.array(img.convert("RGB"))

# ═══════════════════════════════════════════════════════════════
# FIX 3 — TRANSITIONS: simple hard cut only
# FIX 4 — QUALITY: slow zoom 100→106% (subtle, not distracting)
# ═══════════════════════════════════════════════════════════════
def zoom_frame(frame, t, clip_dur):
    """Subtle slow zoom: 100% → 106% over clip duration."""
    from PIL import Image
    scale = 1.0 + 0.06 * (t / max(clip_dur, 0.1))
    img   = Image.fromarray(frame)
    nW, nH = int(SW * scale), int(SH * scale)
    img   = img.resize((nW, nH), Image.LANCZOS)
    x, y  = (nW - SW) // 2, (nH - SH) // 2
    return np.array(img.crop((x, y, x+SW, y+SH)))

def dust(frame, seed=0):
    import cv2
    np.random.seed(seed % 500)
    ov = np.zeros((SH, SW), dtype=np.float32)
    for _ in range(5):
        cv2.circle(ov,
                   (np.random.randint(0,SW), np.random.randint(0,SH)),
                   np.random.randint(1,3),
                   float(np.random.uniform(0.15, 0.40)), -1)
    ov = cv2.GaussianBlur(ov, (5,5), 0)
    ff = frame.astype(np.float32) / 255.0
    return (np.clip(ff + np.stack([ov]*3, axis=2) * 0.08, 0, 1) * 255).astype(np.uint8)

# ═══════════════════════════════════════════════════════════════
# SECTION 7 — VIDEO ASSEMBLY
# FIX 3: HARD CUT ONLY — no crossfade, no blur
# FIX 4: CRF 18, preset slow → true 1080p quality
# FIX 5: clip duration calculated from voice duration
# ═══════════════════════════════════════════════════════════════
def assemble(assets, captions, total_dur, music_file, output_file):
    import cv2
    log.info(f"Assembling {total_dur:.1f}s video with {len(assets)} clips...")
    log.info(f"  Voice: shorts_voice.wav ({os.path.getsize('shorts_voice.wav')//1024}KB)")
    log.info(f"  Music: {music_file or 'none'}")

    # FIX 4: pre-load all images at full 1080p quality
    frames_data = []
    for path in assets:
        img = cv2.imread(path)
        if img is None: continue
        img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        img = cv2.resize(img, (SW, SH), interpolation=cv2.INTER_LANCZOS4)
        frames_data.append(grade(img))

    if not frames_data:
        log.error("No frames"); return False

    # FIX 5: clip duration from actual voice duration
    n_clips  = len(frames_data)
    clip_dur = total_dur / n_clips
    clip_dur = max(2.0, min(5.0, clip_dur))  # 2–5s per clip
    n_clips  = math.ceil(total_dur / clip_dur)
    log.info(f"{n_clips} clips × {clip_dur:.2f}s = {n_clips*clip_dur:.1f}s (voice: {total_dur:.1f}s)")

    # Write raw video (no audio)
    writer = cv2.VideoWriter(
        "shorts_temp.mp4",
        cv2.VideoWriter_fourcc(*'mp4v'),
        FPS, (SW, SH)
    )

    gf = 0
    for ci in range(n_clips):
        if gf / FPS >= total_dur: break
        curr    = frames_data[ci % len(frames_data)]
        n_f     = int(clip_dur * FPS)

        for fi in range(n_f):
            tg = gf / FPS
            if tg >= total_dur: break

            # Subtle slow zoom per clip
            frame = zoom_frame(curr, fi/FPS, clip_dur)
            frame = dust(frame, seed=gf)
            frame = render_frame(frame, tg, captions)

            # FIX 3: HARD CUT — no transition frames at all
            # Just a clean 2-frame black flash at cut point for rhythm
            if fi == 0 and ci > 0:
                black = np.zeros((SH, SW, 3), dtype=np.uint8)
                # Write 1 black frame (1/30s = 0.033s flash) for visual beat
                writer.write(cv2.cvtColor(black, cv2.COLOR_RGB2BGR))

            # Fade in first 0.5s of video
            if ci == 0 and fi < int(0.5 * FPS):
                frame = (frame.astype(np.float32) * (fi / (0.5*FPS))).astype(np.uint8)

            # Fade out last 0.8s of video
            if tg > total_dur - 0.8:
                fade  = max(0.0, (total_dur - tg) / 0.8)
                frame = (frame.astype(np.float32) * fade).astype(np.uint8)

            writer.write(cv2.cvtColor(frame, cv2.COLOR_RGB2BGR))
            gf += 1

    writer.release()
    log.info("Raw frames written")

    # ── AUDIO MIX + FINAL ENCODE ──────────────────────────────
    log.info("Starting FFmpeg audio mux...")
    log.info(f"  shorts_voice.wav exists: {os.path.exists('shorts_voice.wav')}")
    log.info(f"  music_file: {music_file}, exists: {music_file and os.path.exists(music_file)}")

    if music_file and os.path.exists(music_file):
        af  = (
            "[1:a]volume=1.0[v];"
            "[2:a]volume=0.15,aloop=0:size=2e+09,atrim=0={dur}[m];"
            "[v][m]amix=inputs=2:duration=first[aout]"
        ).format(dur=total_dur)
        cmd = [
            "ffmpeg", "-y",
            "-i", "shorts_temp.mp4",
            "-i", "shorts_voice.wav",
            "-i", music_file,
            "-filter_complex", af,
            "-map", "0:v", "-map", "[aout]",
            "-c:v", "libx264",
            "-crf", VIDEO_CRF,           # FIX 4: quality
            "-preset", VIDEO_PRESET,     # FIX 4: quality
            "-profile:v", "high",
            "-level", "4.1",
            "-c:a", "aac", "-b:a", "192k", "-ar", "48000",
            "-t", str(total_dur),
            "-movflags", "+faststart",
            "-pix_fmt", "yuv420p",
            output_file
        ]
    else:
        cmd = [
            "ffmpeg", "-y",
            "-i", "shorts_temp.mp4",
            "-i", "shorts_voice.wav",
            "-c:v", "libx264",
            "-crf", VIDEO_CRF,
            "-preset", VIDEO_PRESET,
            "-profile:v", "high",
            "-level", "4.1",
            "-c:a", "aac", "-b:a", "192k", "-ar", "48000",
            "-t", str(total_dur),
            "-movflags", "+faststart",
            "-pix_fmt", "yuv420p",
            output_file
        ]

    log.info(f"FFmpeg cmd: {' '.join(cmd[:8])}...")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.error(f"FFmpeg FAILED (code {result.returncode})")
        log.error(f"FFmpeg stderr: {result.stderr[-600:]}")
        import shutil; shutil.copy("shorts_temp.mp4", output_file)
        log.warning("Fallback: copied silent video")
    else:
        size_mb = os.path.getsize(output_file) / 1024 / 1024
        log.info(f"FFmpeg SUCCESS: {output_file} ({size_mb:.1f} MB)")
        # Verify audio stream exists in output
        probe = subprocess.run(
            ["ffprobe","-v","error","-select_streams","a",
             "-show_entries","stream=codec_name","-of","csv=p=0", output_file],
            capture_output=True, text=True
        )
        if probe.stdout.strip():
            log.info(f"Audio stream verified: {probe.stdout.strip()}")
        else:
            log.error("WARNING: Output video has NO audio stream!")

    if os.path.exists("shorts_temp.mp4"):
        os.remove("shorts_temp.mp4")
    return True

# ═══════════════════════════════════════════════════════════════
# SECTION 8 — THUMBNAIL
# ═══════════════════════════════════════════════════════════════
def gen_thumbnail(topic, hook):
    from PIL import Image, ImageDraw
    import io
    log.info("Generating thumbnail...")
    clean_t = clean_topic(topic)
    thumb   = None
    for seed in [42, 123, 777]:
        try:
            p   = (f"ultra dramatic dark cinematic {clean_t} ancient historical "
                   f"sepia shadows vertical portrait 4k professional no text no watermark")
            url = (f"https://image.pollinations.ai/prompt/{requests.utils.quote(p)}"
                   f"?width=1080&height=1920&nologo=true&seed={seed}&enhance=true")
            r   = requests.get(url, timeout=90)
            if r.status_code==200 and len(r.content)>5000:
                thumb = Image.open(io.BytesIO(r.content)).convert("RGB").resize((SW,SH),Image.LANCZOS)
                break
        except: continue
    if thumb is None: thumb = Image.new("RGB",(SW,SH),(8,5,3))

    ov = Image.new("RGBA",(SW,SH),(0,0,0,0))
    od = ImageDraw.Draw(ov)
    for y in range(SH//2, SH):
        od.line([(0,y),(SW,y)], fill=(0,0,0,int(210*(y-SH//2)/(SH//2))))
    thumb = Image.alpha_composite(thumb.convert("RGBA"),ov).convert("RGB")
    draw  = ImageDraw.Draw(thumb)
    font  = get_font(92)
    words = hook.upper().split()
    lines, cur = [], ""
    for w in words:
        test = f"{cur} {w}".strip()
        if draw.textbbox((0,0),test,font=font)[2] > SW*0.85:
            if cur: lines.append(cur)
            cur=w
        else: cur=test
    if cur: lines.append(cur)
    ys = SH - 400 - len(lines)*105//2
    for li,line in enumerate(lines):
        bb = draw.textbbox((0,0),line,font=font)
        x  = (SW-(bb[2]-bb[0]))//2
        y  = ys+li*105
        for dx in range(-7,8,3):
            for dy in range(-7,8,3):
                draw.text((x+dx,y+dy),line,font=font,fill=(0,0,0,255))
        draw.text((x,y),line,font=font,fill=(255,230,0,255))
    draw.text((40,60), CHANNEL_NAME, font=get_font(44), fill=(212,175,55,255))
    draw.line([(0,0),(SW,0)], fill=(212,175,55), width=6)
    thumb.save("shorts_thumbnail.jpg", quality=97, subsampling=0)
    return "shorts_thumbnail.jpg"

# ═══════════════════════════════════════════════════════════════
# SECTION 9 — SEO
# ═══════════════════════════════════════════════════════════════
def gen_seo(topic, script_data, hook):
    try:
        from groq import Groq
        client  = Groq(api_key=GROQ_KEY)
        clean_t = clean_topic(topic)
        prompt  = f"""Viral YouTube Shorts SEO for dark history.
Topic: {topic} | Hook: {hook}
Facts: {script_data.get('fact1','')} {script_data.get('fact2','')}

Return ONLY valid JSON:
{{"title":"max 60 chars, shocking, historical name, one emoji, ends #Shorts",
  "description":"2-3 punchy lines: hook rewritten + dark fact tease + subscribe CTA.",
  "tags":["15 specific+broad tags"]}}

Title example: "THIS Empire Vanished In ONE Night 💀 #Shorts" """
        r    = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}],
            temperature=0.85, max_tokens=400
        )
        text = r.choices[0].message.content.strip().replace("```json","").replace("```","").strip()
        data = json.loads(text[text.find('{'):text.rfind('}')+1])
        slug = clean_t.replace(' ','').lower()
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

# ═══════════════════════════════════════════════════════════════
# SECTION 10 — UPLOAD
# ═══════════════════════════════════════════════════════════════
PEAK_UTC = [(1,30),(7,30),(13,30),(14,30)]

def get_schedule(offset=0):
    now   = datetime.datetime.utcnow()
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
        log.info("TEST MODE — Private, no schedule")
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
        log.info(f"✅ Uploaded: {url}")
        if TEST_MODE: log.info("Review: https://studio.youtube.com → Content → Private")
        try:
            yt.thumbnails().set(videoId=vid,media_body=MediaFileUpload(thumbnail)).execute()
            log.info("✅ Thumbnail set")
        except Exception as e: log.warning(f"Thumb: {e}")
        return vid,url
    except Exception as e:
        log.error(f"Upload: {e}"); return None,None

def dl_drive(fid, path):
    try:
        url = f"https://drive.google.com/uc?export=download&id={fid}"
        s   = requests.Session(); r = s.get(url,stream=True,timeout=60)
        tok = next((v for k,v in r.cookies.items() if "download_warning" in k),None)
        if tok: r = s.get(f"{url}&confirm={tok}",stream=True,timeout=60)
        with open(path,"wb") as f:
            for chunk in r.iter_content(32768):
                if chunk: f.write(chunk)
        return os.path.getsize(path) >= 100
    except Exception as e:
        log.error(f"Drive: {e}"); return False

def setup_auth():
    for name,fid in [("client_secrets.json",GDRIVE_SECRETS_ID),("youtube_token.pkl",GDRIVE_TOKEN_ID)]:
        if not os.path.exists(name):
            log.info(f"Downloading {name}..."); dl_drive(fid,name)
    # Also download background.mp3 as music fallback
    if not os.path.exists("background.mp3"):
        log.info("Downloading background.mp3 as music fallback...")
        dl_drive(GDRIVE_MUSIC_ID, "background.mp3")

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
        ws.append_row([topic,num,title,url,
                       datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),"scheduled"])
        log.info("Sheet updated")
    except Exception as e: log.warning(f"Sheet: {e}")

def get_related_topics(long_topic, n=2):
    try:
        from groq import Groq
        c      = Groq(api_key=GROQ_KEY)
        prompt = f"""Main long-form video: {long_topic}
Generate {n} related Shorts topics — different dark angles of the same topic.
Each: shocking, specific, 5-10 words. No repeats.
Return ONLY JSON array: ["Topic 1","Topic 2"]"""
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

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def run_short(topic, num, offset):
    log.info(f"\n{'='*50}\nSHORT #{num} — {topic}\n{'='*50}")

    # 1. Script
    script = generate_script(topic)
    if not script:
        log.error("ABORT: Script generation failed")
        return None
    hook = script.get("hook","The truth was buried for centuries.")
    log.info(f"Script OK — hook: {hook}")

    # 2. Voice — hard verify file exists and has content
    dur, word_timings = generate_voice_with_timing(script)

    # Hard check: voice file must exist
    if not os.path.exists("shorts_voice.wav"):
        log.error("ABORT: shorts_voice.wav not found after voice generation")
        return None
    voice_size = os.path.getsize("shorts_voice.wav")
    if voice_size < 1000:
        log.error(f"ABORT: shorts_voice.wav too small ({voice_size} bytes)")
        return None

    # Get real duration from file
    dur = get_audio_dur()
    log.info(f"Voice file verified: {voice_size//1024}KB, {dur:.2f}s")

    # Enforce 30-40s
    dur = max(MIN_DUR, min(MAX_DUR, dur))
    log.info(f"Final duration: {dur:.2f}s")

    # 3. Captions — only built if we have real voice
    captions = build_synced_captions(script, word_timings, dur)
    log.info(f"Captions built: {len(captions)}")

    # 4. Music — resolve BEFORE assembly
    music_file = resolve_music()
    log.info(f"Music: {music_file or 'NONE — voice only'}")

    # 5. Assets
    assets = fetch_assets(topic)
    if not assets:
        log.error("ABORT: No assets fetched")
        return None

    # 6. Assemble
    safe = re.sub(r'[^\w\s]','',topic).replace(' ','_')[:40]
    out  = f"short_{num}_{safe}.mp4"
    ok   = assemble(assets, captions, dur, music_file, out)
    if not ok or not os.path.exists(out): return None

    size_mb = os.path.getsize(out)/1024/1024
    log.info(f"✅ {out} — {size_mb:.1f} MB")

    # 6. Thumbnail
    thumb = gen_thumbnail(topic, hook)

    # 7. SEO
    seo = gen_seo(topic, script, hook)

    # 8. Upload
    _, url = upload(out, seo, thumb, offset)
    if url:
        update_sheet(topic, url, seo["title"], num)
        log.info(f"Short #{num}: {url}\nTitle: {seo['title']}")

    # Cleanup
    keep = [out] if TEST_MODE else []
    for f in ["shorts_thumbnail.jpg","shorts_raw_voice.mp3","shorts_voice.wav"]:
        if os.path.exists(f): os.remove(f)
    if out not in keep and os.path.exists(out): os.remove(out)
    for f in os.listdir("shorts_assets"):
        try: os.remove(f"shorts_assets/{f}")
        except: pass

    return url

def main():
    log.info("="*55)
    log.info("DarkHistoryMind SHORTS Pipeline v2")
    log.info("="*55)
    if TEST_MODE:
        log.info("*** TEST MODE: 1 short | Private | No schedule | Artifact saved ***")

    download_fonts()
    setup_auth()
    os.makedirs("shorts_assets", exist_ok=True)

    long_topic = LONG_VIDEO_TOPIC.strip() or "The Dark Truth About The Roman Empire"
    log.info(f"Long topic: {long_topic}")

    n_shorts = 1 if TEST_MODE else 2
    topics   = get_related_topics(long_topic, n=n_shorts)

    results = []
    for i, topic in enumerate(topics, 1):
        url = run_short(topic, num=i, offset=i-1)
        results.append(url)
        if i < len(topics): time.sleep(10)

    log.info("="*55+"\nSHORTS COMPLETE")
    for i,url in enumerate(results,1):
        log.info(f"Short #{i}: {url or 'FAILED'}")
    if TEST_MODE:
        log.info("→ Review: https://studio.youtube.com → Content → Private")
    if not any(results): sys.exit(1)

if __name__ == "__main__":
    main()
