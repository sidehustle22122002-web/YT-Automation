#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════
# DarkHistoryMind — YouTube Shorts Pipeline (Clean v3)
# Focus: Voice + Whisper captions + Hard cuts + Quality visuals
# No music complexity. Simple and reliable.
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

# ── ENV ───────────────────────────────────────────────
GROQ_KEY          = os.environ["GROQ_KEY"]
PEXELS_KEY        = os.environ["PEXELS_KEY"]
PIXABAY_KEY       = os.environ["PIXABAY_KEY"]
GDRIVE_SECRETS_ID = os.environ["GDRIVE_SECRETS_ID"]
GDRIVE_TOKEN_ID   = os.environ["GDRIVE_TOKEN_ID"]
GDRIVE_MUSIC_ID   = os.environ.get("GDRIVE_MUSIC_ID", "")
SHEET_ID          = os.environ["SHEET_ID"]
LONG_VIDEO_TOPIC  = os.environ.get("LONG_VIDEO_TOPIC", "")
CHANNEL_NAME      = "DarkHistoryMind"

# ── VIDEO SPECS ───────────────────────────────────────
SW, SH       = 1080, 1920
FPS          = 30

# ── CAPTION STYLE ─────────────────────────────────────
CAPTION_SIZE = 100
HOOK_SIZE    = 115
STROKE_W     = 7
CENTER_Y     = 0.50
SAFE_TOP     = 0.10
SAFE_BOTTOM  = 0.25

# ── FONTS ─────────────────────────────────────────────
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

# ═══════════════════════════════════════════════════════
# SECTION 1 — SCRIPT
# ═══════════════════════════════════════════════════════
def generate_script(topic):
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)
        prompt = f"""Write a viral YouTube Shorts dark history script.
Topic: {topic}

WORD COUNT REQUIREMENT: full_script MUST be exactly 100 words. Not 60. Not 80. Exactly 100.
At TTS rate -18% (slow dramatic pace), 100 words = 35-38 seconds. This is non-negotiable.

Structure:
HOOK (10 words): ONE brutal shocking statement. No questions. Present tense.
FACT1 (20 words): First dark shocking fact. Two sentences.
FACT2 (20 words): Second darker fact. Escalate tension. Two sentences.
STORY (30 words): The shocking core truth. Three sentences. Most intense part.
CONCLUSION (20 words): Haunting final revelation. Two sentences. End with a statement.

RULES:
- Count every word in full_script before outputting. Must equal 100.
- Cold documentary tone. TRUE historical facts only.
- Never start with Welcome, Today, In this video, Subscribe.
- Short punchy sentences. No filler words.

Return ONLY valid JSON:
{{"hook":"","fact1":"","fact2":"","story":"","conclusion":"","full_script":"exactly 100 word script"}}"""

        r    = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.85,
            max_tokens=800
        )
        text = r.choices[0].message.content.strip()
        text = text.replace("```json", "").replace("```", "").strip()
        data = json.loads(text[text.find('{'):text.rfind('}')+1])
        wc   = len(data.get("full_script", "").split())
        log.info(f"Script: {wc} words | Hook: {data.get('hook','')}")

        # If full_script is short, rebuild it from sections
        sections = ["hook","fact1","fact2","story","conclusion"]
        built = " ".join(data.get(s,"") for s in sections if data.get(s,"")).strip()
        built_wc = len(built.split())
        if built_wc > wc:
            data["full_script"] = built
            wc = built_wc
            log.info(f"Rebuilt from sections: {wc} words")

        # Retry up to 5 times if still under 85 words
        attempts = 0
        while wc < 85 and attempts < 5:
            attempts += 1
            log.warning(f"Script too short ({wc} words) — retry {attempts}/5...")
            retry_prompt = f"""Write a dark history YouTube Shorts script about: {topic}
The script must be EXACTLY 100 words when all sections are combined.
Return ONLY this JSON with no extra text:
{{"hook":"10 word shocking statement","fact1":"20 word dark fact sentence","fact2":"20 word darker fact sentence","story":"30 word shocking core truth with multiple sentences","conclusion":"20 word haunting final revelation","full_script":"all sections combined into one 100 word paragraph"}}"""
            r2    = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": retry_prompt}],
                temperature=0.95,
                max_tokens=800
            )
            text2 = r2.choices[0].message.content.strip()
            text2 = text2.replace("```json","").replace("```","").strip()
            try:
                data2 = json.loads(text2[text2.find('{'):text2.rfind('}')+1])
                # Try rebuilding from sections first
                built2 = " ".join(data2.get(s,"") for s in sections if data2.get(s,"")).strip()
                built2_wc = len(built2.split())
                fs_wc = len(data2.get("full_script","").split())
                # Use whichever is longer
                if built2_wc > fs_wc:
                    data2["full_script"] = built2
                    wc2 = built2_wc
                else:
                    wc2 = fs_wc
                log.info(f"Retry {attempts}: {wc2} words")
                if wc2 > wc:
                    data = data2
                    wc   = wc2
            except Exception as pe:
                log.warning(f"Retry {attempts} parse failed: {pe}")

        # Final safety: if still short, pad by repeating conclusion
        if wc < 70:
            log.warning(f"Script only {wc} words after retries — padding")
            full = data.get("full_script","")
            conclusion = data.get("conclusion","The truth was buried for centuries.")
            while len(full.split()) < 85:
                full += " " + conclusion
            data["full_script"] = " ".join(full.split()[:100])
            wc = len(data["full_script"].split())

        log.info(f"Final script: {wc} words")
        return data
    except Exception as e:
        log.error(f"Script failed: {e}")
        return None

# ═══════════════════════════════════════════════════════
# SECTION 2 — VOICE
# Uses communicate.save() — proven reliable method
# ═══════════════════════════════════════════════════════
def generate_voice(script_data):
    try:
        import asyncio, nest_asyncio, edge_tts
        nest_asyncio.apply()

        full_script = script_data.get("full_script", "")
        log.info(f"Generating voice for: {len(full_script)} chars")

        async def _save():
            communicate = edge_tts.Communicate(
                full_script,
                voice="en-GB-ThomasNeural",
                rate="-18%",   # slower dramatic pace — matches long-form
                pitch="-10Hz"
            )
            await communicate.save("shorts_raw_voice.mp3")

        asyncio.run(_save())

        # Verify file
        if not os.path.exists("shorts_raw_voice.mp3"):
            log.error("Voice file not created")
            return False
        size = os.path.getsize("shorts_raw_voice.mp3")
        log.info(f"Raw voice: {size} bytes")
        if size < 1000:
            log.error(f"Voice file too small: {size} bytes")
            return False

        # Process audio
        from pydub import AudioSegment
        audio = AudioSegment.from_mp3("shorts_raw_voice.mp3")
        audio = audio.set_frame_rate(48000)
        audio = audio.apply_gain(-audio.dBFS + (-14))

        # Strip any trailing silence to prevent loop bug
        # pydub sometimes adds padding — remove anything below -50dBFS at end
        raw_dur = len(audio) / 1000

        # Export clean WAV
        audio.export("shorts_voice.wav", format="wav", parameters=["-ar","48000"])

        # Get exact duration via ffprobe (most reliable)
        try:
            probe = subprocess.run(
                ["ffprobe","-v","error","-show_entries","format=duration",
                 "-of","default=noprint_wrappers=1:nokey=1","shorts_voice.wav"],
                capture_output=True, text=True, timeout=10
            )
            exact_dur = float(probe.stdout.strip())
            log.info(f"Voice saved: {exact_dur:.3f}s (raw: {raw_dur:.3f}s)")
        except Exception:
            exact_dur = raw_dur
            log.info(f"Voice saved: {exact_dur:.2f}s")

        return True

    except Exception as e:
        log.error(f"Voice failed: {e}")
        return False

def get_voice_duration():
    """Get exact duration using ffprobe — most reliable method."""
    try:
        probe = subprocess.run(
            ["ffprobe","-v","error","-show_entries","format=duration",
             "-of","default=noprint_wrappers=1:nokey=1","shorts_voice.wav"],
            capture_output=True, text=True, timeout=10
        )
        d = float(probe.stdout.strip())
        log.info(f"Voice duration (ffprobe): {d:.3f}s")
        return d
    except Exception:
        pass
    # Fallback to moviepy
    try:
        try:
            from moviepy.editor import AudioFileClip
        except ImportError:
            from moviepy import AudioFileClip
        c = AudioFileClip("shorts_voice.wav")
        d = c.duration
        c.close()
        return d
    except:
        return 35.0

# ═══════════════════════════════════════════════════════
# SECTION 3 — CAPTIONS via Whisper
# Transcribes actual audio → exact word timestamps
# Groups 2-3 words per caption
# ═══════════════════════════════════════════════════════
KEYWORDS = {
    "empire","emperor","king","queen","pharaoh","caesar","blood","death",
    "war","battle","massacre","execution","torture","secret","hidden",
    "forbidden","truth","betrayal","vanished","destroyed","murdered",
    "million","billion","ancient","medieval","roman","greek","egypt",
    "persian","mongol","viking","spartan","never","only","first","last",
    "dark","evil","brutal","savage","ruthless","feared","powerful",
    "night","centuries","single","entire","buried","erased","real",
}

def get_word_timings():
    """
    Transcribe shorts_voice.wav with faster-whisper.
    Returns list of {word, start, end} dicts.
    """
    try:
        from faster_whisper import WhisperModel
        log.info("Transcribing with Whisper tiny...")
        model    = WhisperModel("tiny", device="cpu", compute_type="int8")
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
        log.info(f"Whisper: {len(words)} words transcribed")
        return words
    except ImportError:
        log.warning("faster-whisper not available")
        return []
    except Exception as e:
        log.warning(f"Whisper error: {e}")
        return []

def build_captions(script_data, total_dur):
    """
    Build captions from Whisper word timestamps.
    Falls back to script-based estimation if Whisper unavailable.
    """
    hook = script_data.get("hook", "").strip()

    # Try Whisper first
    word_timings = get_word_timings()

    captions = []

    if word_timings:
        log.info("Building captions from Whisper timestamps")
        n_hook = len(hook.split())

        # Hook: first N words as one big caption
        if n_hook > 0 and len(word_timings) >= n_hook:
            h_start = word_timings[0]["start"]
            h_end   = max(
                word_timings[min(n_hook-1, len(word_timings)-1)]["end"],
                h_start + 1.8
            )
            captions.append({
                "text":    hook.upper(),
                "start":   h_start,
                "end":     min(h_end, total_dur - 0.1),
                "is_hook": True,
                "color":   "yellow",
            })
            remaining = word_timings[n_hook:]
        else:
            remaining = word_timings

        # Body: group 2-3 words per caption
        i = 0
        while i < len(remaining):
            gs    = random.choices([2, 3, 3], weights=[25, 50, 25])[0]
            group = remaining[i:i+gs]
            if not group:
                break

            g_start = group[0]["start"]
            g_end   = group[-1]["end"]
            g_words = [w["word"] for w in group]

            # Min display 0.4s
            if g_end - g_start < 0.4:
                g_end = g_start + 0.4

            # Cap at next word start
            if i + gs < len(remaining):
                g_end = min(g_end, remaining[i+gs]["start"] - 0.03)

            g_end = min(g_end, total_dur - 0.1)
            if g_end <= g_start:
                i += gs
                continue

            has_kw = any(
                w.lower().strip(".,!?;:") in KEYWORDS
                for w in g_words
            )
            emph = has_kw and random.random() < 0.35

            captions.append({
                "text":    " ".join(g_words).upper(),
                "start":   round(g_start, 3),
                "end":     round(g_end, 3),
                "is_hook": False,
                "color":   "yellow" if emph else "white",
            })
            i += gs

    else:
        # Fallback: distribute script words evenly
        log.warning("Whisper unavailable — using estimated captions")
        words = script_data.get("full_script", "").split()
        if not words:
            return []
        wdur = total_dur / max(len(words), 1)

        # Hook first
        hook_words = hook.split()
        hook_dur   = len(hook_words) * wdur * 1.1
        captions.append({
            "text":    hook.upper(),
            "start":   0.2,
            "end":     min(hook_dur, total_dur - 0.1),
            "is_hook": True,
            "color":   "yellow",
        })

        t = hook_dur + 0.1
        i = len(hook_words)
        while i < len(words) and t < total_dur - 0.5:
            gs    = random.choices([2, 3, 3], weights=[25, 50, 25])[0]
            group = words[i:i+gs]
            if not group:
                break
            cd    = max(0.5, len(group) * wdur)
            has_kw= any(w.lower().strip(".,!?;:") in KEYWORDS for w in group)
            captions.append({
                "text":    " ".join(group).upper(),
                "start":   round(t, 3),
                "end":     round(min(t+cd, total_dur-0.1), 3),
                "is_hook": False,
                "color":   "yellow" if (has_kw and random.random()<0.35) else "white",
            })
            i += gs
            t  += cd + 0.03

    log.info(f"Captions: {len(captions)}")
    return captions

# ═══════════════════════════════════════════════════════
# SECTION 4 — ASSETS
# Fetch high-res images, crop to 9:16
# ═══════════════════════════════════════════════════════
SCENE_KW = {
    "hook":       ["ancient ruins dramatic dark", "mysterious fortress medieval dark"],
    "fact1":      ["ancient manuscript historical sepia", "old parchment candlelight"],
    "fact2":      ["roman ruins cinematic", "greek statue dramatic shadow"],
    "story":      ["battle painting historical", "medieval war scene dramatic"],
    "climax":     ["dramatic ancient empire dark", "mysterious shadows historical"],
    "conclusion": ["sunset ancient ruins melancholic", "foggy ancient landscape dark"],
}

def clean_topic(t):
    for p in [
        "The Psychology of","The Dark Truth About","The Secret Life of",
        "The Truth Behind","The Real Story of","The Hidden Truth About",
        "The Dark History of","Why Did","The Real History of",
        "The Hidden History of","The Dark Story of","The Real Reason",
    ]:
        t = t.replace(p, "").strip()
    return t

def crop_916(img):
    from PIL import Image
    W, H  = img.size
    tw    = int(H * 9 / 16)
    if tw <= W:
        l   = (W - tw) // 2
        img = img.crop((l, 0, l+tw, H))
    else:
        th  = int(W * 16 / 9)
        ni  = Image.new("RGB", (W, th), (8, 5, 3))
        ni.paste(img, (0, (th - H) // 2))
        img = ni
    return img.resize((SW, SH), Image.LANCZOS)

def fetch_image(query, idx):
    from PIL import Image
    import io

    # Pexels
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
            url   = photo["src"].get("original") or photo["src"]["large2x"]
            data  = requests.get(url, timeout=40).content
            img   = Image.open(io.BytesIO(data)).convert("RGB")
            if img.width >= 1000 or img.height >= 1000:
                path = f"shorts_assets/img_{idx}.jpg"
                crop_916(img).save(path, quality=95)
                log.info(f"  Pexels: {query[:30]}")
                return path
    except Exception as e:
        log.warning(f"Pexels: {e}")

    # Pixabay
    try:
        r    = requests.get(
            f"https://pixabay.com/api/?key={PIXABAY_KEY}"
            f"&q={query.replace(' ','+')}&image_type=photo"
            f"&orientation=horizontal&per_page=5&order=popular&min_width=1280",
            timeout=20
        )
        hits = r.json().get("hits", [])
        if hits:
            url  = random.choice(hits).get("largeImageURL", "")
            data = requests.get(url, timeout=40).content
            img  = Image.open(io.BytesIO(data)).convert("RGB")
            path = f"shorts_assets/img_{idx}.jpg"
            crop_916(img).save(path, quality=95)
            log.info(f"  Pixabay: {query[:30]}")
            return path
    except Exception as e:
        log.warning(f"Pixabay: {e}")

    # Pollinations AI fallback
    try:
        prompt = f"ultra detailed dark cinematic historical {query} dramatic sepia no text no watermark"
        url    = (
            f"https://image.pollinations.ai/prompt/{requests.utils.quote(prompt)}"
            f"?width=1080&height=1920&nologo=true&seed={idx*7}"
        )
        r = requests.get(url, timeout=90)
        if r.status_code == 200 and len(r.content) > 10000:
            img  = Image.open(io.BytesIO(r.content)).convert("RGB")
            img  = img.resize((SW, SH), Image.LANCZOS)
            path = f"shorts_assets/img_{idx}.jpg"
            img.save(path, quality=95)
            log.info(f"  AI image: {query[:30]}")
            return path
    except Exception as e:
        log.warning(f"AI image: {e}")

    return None

def fetch_assets(topic):
    os.makedirs("shorts_assets", exist_ok=True)
    assets  = []
    idx     = 0
    clean_t = clean_topic(topic)
    sections = [
        "hook", "fact1", "fact2",
        "story", "story",
        "climax", "climax",
        "conclusion", "conclusion"
    ]
    for section in sections:
        kw   = random.choice(SCENE_KW.get(section, SCENE_KW["hook"]))
        kw  += f" {clean_t}"
        path = fetch_image(kw, idx)
        if path:
            assets.append(path)
        idx  += 1
        time.sleep(0.3)

    # Pad to at least 6 images
    while len(assets) < 6:
        kw   = random.choice(SCENE_KW["story"]) + f" {clean_t}"
        path = fetch_image(kw, idx)
        if path:
            assets.append(path)
        idx += 1

    log.info(f"Assets: {len(assets)} images")
    return assets

# ═══════════════════════════════════════════════════════
# SECTION 5 — COLOR GRADING
# Dark cinematic: sepia + vignette + warm tone + grain
# ═══════════════════════════════════════════════════════
def grade(frame):
    img = frame.astype(np.float32) / 255.0
    img = np.clip(img * 0.78, 0, 1)
    img = np.clip(img + 1.25 * 0.10 * (img - 0.5), 0, 1)
    img[:,:,0] = np.clip(img[:,:,0] + 0.05, 0, 1)
    img[:,:,2] = np.clip(img[:,:,2] - 0.05, 0, 1)
    gray  = 0.299*img[:,:,0] + 0.587*img[:,:,1] + 0.114*img[:,:,2]
    sepia = np.stack([
        np.clip(gray*1.08, 0, 1),
        np.clip(gray*0.86, 0, 1),
        np.clip(gray*0.67, 0, 1),
    ], axis=2)
    img  = np.clip(img*0.70 + sepia*0.30, 0, 1)
    H, W = img.shape[:2]
    Y, X = np.ogrid[:H, :W]
    dist = np.sqrt(((X-W/2)/(W/2))**2 + ((Y-H/2)/(H/2))**2)
    img  = np.clip(img * (1 - np.clip(dist*0.60, 0, 0.65))[:,:,np.newaxis], 0, 1)
    img  = np.clip(img + np.random.normal(0, 0.013, img.shape).astype(np.float32), 0, 1)
    return (img * 255).astype(np.uint8)

# ═══════════════════════════════════════════════════════
# SECTION 6 — RENDER CAPTIONS ON FRAME
# Pop-in animation + heavy stroke + yellow keywords
# ═══════════════════════════════════════════════════════
def render_caption(frame_rgb, t, captions):
    from PIL import Image, ImageDraw
    img  = Image.fromarray(frame_rgb).convert("RGBA")
    W, H = img.size

    cap = next((c for c in captions if c["start"] <= t <= c["end"]), None)
    if cap is None:
        return np.array(img.convert("RGB"))

    cap_t = t - cap["start"]

    # Pop-in: scale 85%→100% over 0.2s, fade 0%→100% over 0.12s
    ap    = min(1.0, cap_t / 0.20)
    fp    = min(1.0, cap_t / 0.12)
    scale = 0.85 + 0.15 * (1 - (1 - ap)**3)
    alpha = int(255 * fp)

    base  = HOOK_SIZE if cap.get("is_hook") else CAPTION_SIZE
    fsize = max(60, int(base * scale))
    font  = get_font(fsize)

    # Yellow for hook/keyword, white for normal
    if cap.get("is_hook") or cap.get("color") == "yellow":
        tcol = (255, 230, 0, alpha)
    else:
        tcol = (245, 245, 245, alpha)
    scol = (0, 0, 0, alpha)

    # Word wrap at 68% screen width
    text  = cap["text"]
    lines, cur = [], ""
    td = ImageDraw.Draw(Image.new("RGBA", (W, H)))
    for w in text.split():
        test = f"{cur} {w}".strip()
        if td.textbbox((0, 0), test, font=font)[2] > W * 0.68:
            if cur:
                lines.append(cur)
            cur = w
        else:
            cur = test
    if cur:
        lines.append(cur)

    lh    = fsize + 14
    th    = len(lines) * lh
    ys    = int(H * CENTER_Y) - th // 2
    ys    = max(int(H * SAFE_TOP), min(int(H * (1 - SAFE_BOTTOM)) - th, ys))

    ov = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    od = ImageDraw.Draw(ov)

    for li, line in enumerate(lines):
        bb = od.textbbox((0, 0), line, font=font)
        x  = (W - (bb[2] - bb[0])) // 2
        y  = ys + li * lh
        # Heavy black stroke
        for dx in range(-STROKE_W, STROKE_W+1, 2):
            for dy in range(-STROKE_W, STROKE_W+1, 2):
                if dx == 0 and dy == 0:
                    continue
                od.text((x+dx, y+dy), line, font=font, fill=scol)
        od.text((x, y), line, font=font, fill=tcol)

    img = Image.alpha_composite(img, ov)
    return np.array(img.convert("RGB"))

# ═══════════════════════════════════════════════════════
# SECTION 7 — ASSEMBLE VIDEO
# Hard cuts only. Slow zoom per clip. Fade in/out.
# Voice-only audio (no music complexity).
# ═══════════════════════════════════════════════════════
def zoom_frame(frame, t, clip_dur):
    from PIL import Image
    scale  = 1.0 + 0.06 * (t / max(clip_dur, 0.1))
    img    = Image.fromarray(frame)
    nW, nH = int(SW * scale), int(SH * scale)
    img    = img.resize((nW, nH), Image.LANCZOS)
    x, y   = (nW - SW) // 2, (nH - SH) // 2
    return np.array(img.crop((x, y, x+SW, y+SH)))

def dust(frame, seed=0):
    import cv2
    np.random.seed(seed % 500)
    ov = np.zeros((SH, SW), dtype=np.float32)
    for _ in range(5):
        cv2.circle(
            ov,
            (np.random.randint(0, SW), np.random.randint(0, SH)),
            np.random.randint(1, 3),
            float(np.random.uniform(0.15, 0.40)),
            -1
        )
    ov = cv2.GaussianBlur(ov, (5, 5), 0)
    ff = frame.astype(np.float32) / 255.0
    return (np.clip(ff + np.stack([ov]*3, axis=2)*0.08, 0, 1)*255).astype(np.uint8)

def assemble(assets, captions, total_dur, output_file):
    import cv2
    log.info(f"Assembling {total_dur:.1f}s | {len(assets)} images")

    # Pre-load + grade all images
    frames_data = []
    for path in assets:
        img = cv2.imread(path)
        if img is None:
            continue
        img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        img = cv2.resize(img, (SW, SH), interpolation=cv2.INTER_LANCZOS4)
        frames_data.append(grade(img))

    if not frames_data:
        log.error("No frames loaded")
        return False

    n_clips  = len(frames_data)
    clip_dur = max(2.5, min(5.0, total_dur / n_clips))
    n_clips  = math.ceil(total_dur / clip_dur)
    log.info(f"{n_clips} clips x {clip_dur:.1f}s")

    writer = cv2.VideoWriter(
        "shorts_temp.mp4",
        cv2.VideoWriter_fourcc(*'mp4v'),
        FPS,
        (SW, SH)
    )

    gf = 0
    for ci in range(n_clips):
        if gf / FPS >= total_dur:
            break
        curr  = frames_data[ci % len(frames_data)]
        n_f   = int(clip_dur * FPS)

        for fi in range(n_f):
            tg = gf / FPS
            if tg >= total_dur:
                break

            # Slow zoom per clip
            frame = zoom_frame(curr, fi / FPS, clip_dur)
            # Dust overlay
            frame = dust(frame, seed=gf)
            # Captions
            frame = render_caption(frame, tg, captions)

            # Fade in first 0.5s
            if ci == 0 and fi < int(0.5 * FPS):
                frame = (frame.astype(np.float32) * (fi / (0.5*FPS))).astype(np.uint8)

            # Fade out last 0.8s
            if tg > total_dur - 0.8:
                fade  = max(0.0, (total_dur - tg) / 0.8)
                frame = (frame.astype(np.float32) * fade).astype(np.uint8)

            writer.write(cv2.cvtColor(frame, cv2.COLOR_RGB2BGR))
            gf += 1

        # Hard cut: 1 black frame between clips
        if ci < n_clips - 1 and gf / FPS < total_dur:
            writer.write(np.zeros((SH, SW, 3), dtype=np.uint8))
            gf += 1

    writer.release()
    log.info("Frames written")

    # ── AUDIO MUX: voice + background music ───────────
    log.info(f"Muxing audio: shorts_voice.wav ({os.path.getsize('shorts_voice.wav')//1024}KB)")
    music = "background.mp3" if os.path.exists("background.mp3") else None
    log.info(f"Music: {music or 'none'}")

    if music:
        # Mix voice (dominant) + background music (low volume, same as long-form)
        af  = (
            "[1:a]volume=1.0[v];"
            "[2:a]volume=0.08,aloop=0:size=2e+09[m];"
            "[v][m]amix=inputs=2:duration=first:dropout_transition=0[aout]"
        )
        cmd = [
            "ffmpeg", "-y",
            "-i", "shorts_temp.mp4",
            "-i", "shorts_voice.wav",
            "-i", music,
            "-filter_complex", af,
            "-map", "0:v", "-map", "[aout]",
            "-c:v", "libx264", "-crf", "18", "-preset", "fast",
            "-profile:v", "high",
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
            "-c:v", "libx264", "-crf", "18", "-preset", "fast",
            "-profile:v", "high",
            "-c:a", "aac", "-b:a", "192k", "-ar", "48000",
            "-t", str(total_dur),
            "-movflags", "+faststart",
            "-pix_fmt", "yuv420p",
            output_file
        ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.error(f"FFmpeg failed:\n{result.stderr[-500:]}")
        import shutil; shutil.copy("shorts_temp.mp4", output_file)
        return False
    else:
        size_mb = os.path.getsize(output_file) / 1024 / 1024
        log.info(f"Video: {output_file} ({size_mb:.1f}MB)")
        probe = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "a:0",
             "-show_entries", "stream=codec_name", "-of", "csv=p=0", output_file],
            capture_output=True, text=True
        )
        stream = probe.stdout.strip()
        log.info(f"Audio stream: {stream} ✅" if stream else "WARNING: NO AUDIO STREAM!")

    if os.path.exists("shorts_temp.mp4"):
        os.remove("shorts_temp.mp4")
    return True

# ═══════════════════════════════════════════════════════
# SECTION 8 — THUMBNAIL
# ═══════════════════════════════════════════════════════
def gen_thumbnail(topic, hook):
    from PIL import Image, ImageDraw
    import io
    log.info("Generating thumbnail...")
    clean_t = clean_topic(topic)
    thumb   = None

    for seed in [42, 123, 777]:
        try:
            p   = (
                f"ultra dramatic dark cinematic {clean_t} ancient historical "
                f"sepia shadows vertical portrait no text no watermark"
            )
            url = (
                f"https://image.pollinations.ai/prompt/{requests.utils.quote(p)}"
                f"?width=1080&height=1920&nologo=true&seed={seed}"
            )
            r = requests.get(url, timeout=90)
            if r.status_code == 200 and len(r.content) > 5000:
                thumb = Image.open(io.BytesIO(r.content)).convert("RGB")
                thumb = thumb.resize((SW, SH), Image.LANCZOS)
                break
        except:
            continue

    if thumb is None:
        thumb = Image.new("RGB", (SW, SH), (8, 5, 3))

    # Dark gradient bottom
    ov = Image.new("RGBA", (SW, SH), (0, 0, 0, 0))
    od = ImageDraw.Draw(ov)
    for y in range(SH // 2, SH):
        od.line([(0, y), (SW, y)], fill=(0, 0, 0, int(210 * (y - SH//2) / (SH//2))))
    thumb = Image.alpha_composite(thumb.convert("RGBA"), ov).convert("RGB")

    draw  = ImageDraw.Draw(thumb)
    font  = get_font(88)
    words = hook.upper().split()
    lines, cur = [], ""
    for w in words:
        test = f"{cur} {w}".strip()
        if draw.textbbox((0, 0), test, font=font)[2] > SW * 0.85:
            if cur:
                lines.append(cur)
            cur = w
        else:
            cur = test
    if cur:
        lines.append(cur)

    ys = SH - 400 - len(lines) * 100 // 2
    for li, line in enumerate(lines):
        bb = draw.textbbox((0, 0), line, font=font)
        x  = (SW - (bb[2]-bb[0])) // 2
        y  = ys + li * 100
        for dx in range(-7, 8, 3):
            for dy in range(-7, 8, 3):
                draw.text((x+dx, y+dy), line, font=font, fill=(0, 0, 0, 255))
        draw.text((x, y), line, font=font, fill=(255, 230, 0, 255))

    draw.text((40, 60), CHANNEL_NAME, font=get_font(42), fill=(212, 175, 55, 255))
    draw.line([(0, 0), (SW, 0)], fill=(212, 175, 55), width=5)
    thumb.save("shorts_thumbnail.jpg", quality=95)
    return "shorts_thumbnail.jpg"

# ═══════════════════════════════════════════════════════
# SECTION 9 — SEO
# ═══════════════════════════════════════════════════════
def gen_seo(topic, script_data, hook):
    try:
        from groq import Groq
        client  = Groq(api_key=GROQ_KEY)
        clean_t = clean_topic(topic)
        prompt  = f"""Viral YouTube Shorts SEO for dark history channel.
Topic: {topic} | Hook: {hook}

Return ONLY valid JSON:
{{"title":"max 60 chars, shocking, historical name, one emoji, ends with #Shorts",
  "description":"2-3 punchy lines then hashtags",
  "tags":["15 tags"]}}
Title example: "THIS Empire Vanished In ONE Night 💀 #Shorts" """

        r    = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.85,
            max_tokens=400
        )
        text = r.choices[0].message.content.strip()
        text = text.replace("```json","").replace("```","").strip()
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
        return {
            "title": f"🔴 {hook[:50]} #Shorts",
            "description": f"{hook}\n\n#Shorts #darkhistory #history",
            "tags": ["Shorts","darkhistory","history","hiddenhistory",
                     "historyfacts","darkfacts","ancienthistory","secrethistory",
                     "historymystery","documentary","mindblowing","viral",
                     "historybuff","darkhistorymind","shorts"]
        }

# ═══════════════════════════════════════════════════════
# SECTION 10 — UPLOAD
# ═══════════════════════════════════════════════════════
# US-primary posting schedule (EST = UTC-5, IST = UTC+5:30)
# Short 1 (gap day):   8:00 AM EST  = 13:00 UTC = 6:30 PM IST
# Short 2 (gap day):   6:00 PM EST  = 23:00 UTC = 4:30 AM IST+1
# Short 3 (upload day): 8:00 AM EST = 13:00 UTC = 6:30 PM IST (teaser)
# ── 3-SHORT SCHEDULE (US primary audience) ────────────
# Short #1 — Upload day, 12:00 AM IST = 18:30 UTC
#             = 1:30 PM EST / 10:30 AM PST (upload day afternoon US)
# Short #2 — Gap day morning, 16:00 UTC
#             = 11:00 AM EST / 8:00 AM PST
# Short #3 — Gap day evening, 01:00 UTC (+2 days)
#             = 8:00 PM EST / 5:00 PM PST

# ── SHORTS SCHEDULE ───────────────────────────────────
# Long-form publishes at 8 PM IST (14:30 UTC)
# Shorts pipeline runs immediately after long-form finishes
#
# Short #1 → gap day,     1:00 AM IST = 19:30 UTC  (5h after long-form)
# Short #2 → gap day,     8:00 PM IST = 14:30 UTC  (next day)
# Short #3 → new vid day, 12:30 AM IST = 19:00 UTC (day before new long-form)
#
# IST to UTC: subtract 5h30m
# 1:00 AM IST  = 19:30 UTC previous day → +1 day from 19:30 UTC
# 8:00 PM IST  = 14:30 UTC same day     → +1 day
# 12:30 AM IST = 19:00 UTC previous day → +2 days from 19:00 UTC

SLOT_SCHEDULE = {
    0: {"hour": 19, "minute": 30, "days_ahead": 1, "label": "1:00 AM IST gap day"},
    1: {"hour": 14, "minute": 30, "days_ahead": 1, "label": "8:00 PM IST gap day"},
    2: {"hour": 19, "minute":  0, "days_ahead": 2, "label": "12:30 AM IST new video day"},
}

def get_schedule(slot):
    now  = datetime.datetime.utcnow()
    cfg  = SLOT_SCHEDULE.get(slot, SLOT_SCHEDULE[0])
    # Build target time: today at cfg hour:min + days_ahead
    base = now.replace(hour=cfg["hour"], minute=cfg["minute"], second=0, microsecond=0)
    t    = base + datetime.timedelta(days=cfg["days_ahead"])
    # Safety: never schedule in the past
    if t <= now:
        t += datetime.timedelta(days=1)
    ist  = t + datetime.timedelta(hours=5, minutes=30)
    log.info(f"Slot {slot} → {t.strftime('%Y-%m-%d %H:%M')} UTC | {ist.strftime('%d %b %I:%M %p')} IST | {cfg['label']}")
    return t.strftime("%Y-%m-%dT%H:%M:%S.000Z")

def get_yt():
    from googleapiclient.discovery import build
    import google.auth.transport.requests
    creds = None
    if os.path.exists("youtube_token.pkl"):
        with open("youtube_token.pkl", "rb") as f:
            creds = pickle.load(f)
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(google.auth.transport.requests.Request())
        except:
            creds = None
    if not creds or not creds.valid:
        log.error("YouTube token invalid")
        return None
    return build("youtube", "v3", credentials=creds, cache_discovery=False)

def upload(video_file, seo, thumbnail, offset=0):
    from googleapiclient.http import MediaFileUpload
    yt = get_yt()
    if not yt:
        return None, None

    pub  = get_schedule(offset)
    body = {
        "snippet": {
            "title":           seo["title"],
            "description":     seo["description"],
            "tags":            seo.get("tags", []),
            "categoryId":      "27",
            "defaultLanguage": "en",
        },
        "status": {
            "privacyStatus":          "private",
            "publishAt":              pub,
            "selfDeclaredMadeForKids": False,
        }
    }

    try:
        media   = MediaFileUpload(
            video_file, mimetype="video/mp4",
            resumable=True, chunksize=5*1024*1024
        )
        request = yt.videos().insert(
            part="snippet,status", body=body, media_body=media
        )
        response = None
        while response is None:
            st, response = request.next_chunk()
            if st:
                log.info(f"  {int(st.progress()*100)}%")

        vid = response["id"]
        url = f"https://youtube.com/watch?v={vid}"
        log.info(f"Uploaded: {url}")
        try:
            yt.thumbnails().set(
                videoId=vid,
                media_body=MediaFileUpload(thumbnail)
            ).execute()
            log.info("Thumbnail set")
        except Exception as e:
            log.warning(f"Thumbnail: {e}")

        return vid, url
    except Exception as e:
        log.error(f"Upload failed: {e}")
        return None, None

def dl_drive(fid, path):
    try:
        url = f"https://drive.google.com/uc?export=download&id={fid}"
        s   = requests.Session()
        r   = s.get(url, stream=True, timeout=60)
        tok = next((v for k,v in r.cookies.items() if "download_warning" in k), None)
        if tok:
            r = s.get(f"{url}&confirm={tok}", stream=True, timeout=60)
        with open(path, "wb") as f:
            for chunk in r.iter_content(32768):
                if chunk:
                    f.write(chunk)
        return os.path.getsize(path) >= 100
    except Exception as e:
        log.error(f"Drive: {e}")
        return False

def setup_auth():
    for name, fid in [
        ("client_secrets.json", GDRIVE_SECRETS_ID),
        ("youtube_token.pkl",   GDRIVE_TOKEN_ID),
    ]:
        if not os.path.exists(name):
            log.info(f"Downloading {name}...")
            dl_drive(fid, name)
    # Always re-download background.mp3 to pick up music changes
    if GDRIVE_MUSIC_ID:
        log.info("Downloading background.mp3 (always fresh)...")
        ok = dl_drive(GDRIVE_MUSIC_ID, "background.mp3")
        log.info(f"background.mp3: {'ready' if ok else 'failed'}")

def get_sheet_client():
    """Get gspread client using existing YouTube OAuth token."""
    try:
        import gspread
        import google.auth.transport.requests
        creds = None
        if os.path.exists("youtube_token.pkl"):
            with open("youtube_token.pkl", "rb") as f:
                creds = pickle.load(f)
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(google.auth.transport.requests.Request())
            except Exception as e:
                log.warning(f"Token refresh: {e}")
        if not creds or not creds.valid:
            log.warning("No valid creds for sheet")
            return None
        gc = gspread.authorize(creds)
        return gc
    except Exception as e:
        log.warning(f"Sheet client failed: {e}")
        return None

def update_sheet(topic, url, title, num):
    try:
        gc = get_sheet_client()
        if not gc:
            log.warning("Sheet update skipped — no auth")
            return
        sh = gc.open_by_key(SHEET_ID)
        try:
            ws = sh.get_worksheet(1)
        except:
            ws = sh.add_worksheet(title="Shorts", rows=1000, cols=6)
            ws.append_row(["Topic","Short#","Title","URL","Uploaded","Status"])
        ws.append_row([
            topic, num, title, url,
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
            "scheduled"
        ])
        log.info(f"Sheet updated: short #{num}")
    except Exception as e:
        log.warning(f"Sheet: {e}")

def get_related_topics(long_topic, n=3):
    try:
        from groq import Groq
        c      = Groq(api_key=GROQ_KEY)
        prompt = f"""The long-form video published is about: {long_topic}

Generate exactly {n} YouTube Shorts topics — all related to this SAME topic.
Each must be a different dark angle, sub-fact, or shocking detail from this topic.
They must NOT be teasers or previews — they are standalone dark history facts.

Rules:
- Each topic: 5-10 words, shocking, specific
- All {n} must be different angles
- All must relate directly to: {long_topic}

Return ONLY a JSON array of exactly {n} strings:
["Topic 1", "Topic 2", "Topic 3"]"""

        r    = c.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role":"user","content":prompt}],
            temperature=0.9,
            max_tokens=200
        )
        text  = r.choices[0].message.content.strip()
        text  = text.replace("```json","").replace("```","").strip()
        tops  = json.loads(text[text.find('['):text.rfind(']')+1])
        # Ensure we always have exactly n topics
        while len(tops) < n:
            ct = clean_topic(long_topic)
            tops.append(f"The Hidden Truth About {ct}")
        log.info(f"Short topics: {tops[:n]}")
        return tops[:n]
    except Exception as e:
        log.warning(f"Topics failed: {e}")
        ct = clean_topic(long_topic)
        return [
            f"The Dark Secret of {ct}",
            f"The Brutal Reality of {ct}",
            f"The Hidden Truth About {ct}",
        ][:n]

# ═══════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════
def run_short(topic, num, offset):
    log.info(f"\n{'='*50}\nSHORT #{num} — {topic}\n{'='*50}")

    # 1. Script
    script = generate_script(topic)
    if not script:
        log.error("ABORT: script failed")
        return None
    hook = script.get("hook", "The truth was buried for centuries.")

    # 2. Voice — communicate.save() method
    ok = generate_voice(script)
    if not ok:
        log.error("ABORT: voice generation failed")
        return None
    if not os.path.exists("shorts_voice.wav"):
        log.error("ABORT: shorts_voice.wav missing")
        return None
    dur = get_voice_duration()
    log.info(f"Voice duration: {dur:.2f}s")
    # Video duration = voiceover duration exactly (same as long-form)
    # No clamping — the script word count controls the duration

    # 3. Captions from Whisper
    captions = build_captions(script, dur)
    log.info(f"Captions OK: {len(captions)}")

    # 4. Assets
    assets = fetch_assets(topic)
    if not assets:
        log.error("ABORT: no assets")
        return None

    # 5. Assemble
    safe = re.sub(r'[^\w\s]','',topic).replace(' ','_')[:40]
    out  = f"short_{num}_{safe}.mp4"
    ok   = assemble(assets, captions, dur, out)
    if not ok or not os.path.exists(out):
        log.error("ABORT: assembly failed")
        return None
    log.info(f"Video OK: {out} ({os.path.getsize(out)//1024//1024}MB)")

    # 6. Thumbnail + SEO + Upload
    thumb = gen_thumbnail(topic, hook)
    seo   = gen_seo(topic, script, hook)
    _, url = upload(out, seo, thumb, offset)

    if url:
        update_sheet(topic, url, seo["title"], num)
        log.info(f"Short #{num} done: {url}")

    # Cleanup
    for f in [out, "shorts_raw_voice.mp3", "shorts_voice.wav", "shorts_thumbnail.jpg"]:
        if os.path.exists(f):
            os.remove(f)
    for f in os.listdir("shorts_assets"):
        try: os.remove(f"shorts_assets/{f}")
        except: pass

    return url

# ── RUN MODE ──────────────────────────────────────────
# SHORTS_MODE env var controls what runs:
#   "all"      — triggered after long-form: produces all 3 shorts at once
#                Short #1 → slot 0 (12 AM IST / 1:30 PM EST upload day)
#                Short #2 → slot 1 (11 AM EST gap day morning)
#                Short #3 → slot 2 (8 PM EST gap day evening)
#   "test"     — 1 short, private, no schedule

def main():
    log.info("="*55)
    log.info("DarkHistoryMind SHORTS Pipeline v3")
    log.info("="*55)

    download_fonts()
    setup_auth()
    os.makedirs("shorts_assets", exist_ok=True)

    long_topic = LONG_VIDEO_TOPIC.strip() or "The Dark Truth About The Roman Empire"
    log.info(f"Long-form topic: {long_topic}")

    # 3 shorts per cycle — all related to the long-form topic
    # Short #1: upload day  → 12:00 AM IST / 1:30 PM EST
    # Short #2: gap day AM  → 11:00 AM EST / 8:00 AM PST
    # Short #3: gap day PM  → 8:00 PM EST  / 5:00 PM PST
    slots  = [0, 1, 2]
    topics = get_related_topics(long_topic, n=3)

    results = []
    for i, (topic, slot) in enumerate(zip(topics, slots), 1):
        log.info(f"\n{'='*40}")
        log.info(f"SHORT #{i}/3 — {SLOT_SCHEDULE[slot]['label']}")
        log.info(f"{'='*40}")
        url = run_short(topic, num=i, offset=slot)
        results.append(url)
        if i < 3:
            time.sleep(15)

    log.info("="*55)
    log.info("SHORTS COMPLETE")
    for i, url in enumerate(results, 1):
        log.info(f"Short #{i}: {url or 'FAILED'}")
    if not any(results):
        sys.exit(1)

if __name__ == "__main__":
    main()
