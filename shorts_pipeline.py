#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════════════════
# DarkHistoryMind — DAILY SHORTS PIPELINE (4 Shorts Per Day)
# Pure automation: Runs daily, uploads 4 shorts at IST: 12 AM, 6 AM, 12 PM, 6 PM
# ═══════════════════════════════════════════════════════════════════════════
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

GROQ_KEY          = os.environ.get("GROQ_KEY", "")
PEXELS_KEY        = os.environ.get("PEXELS_KEY", "")
PIXABAY_KEY       = os.environ.get("PIXABAY_KEY", "")
GDRIVE_SECRETS_ID = os.environ.get("GDRIVE_SECRETS_ID", "")
GDRIVE_TOKEN_ID   = os.environ.get("GDRIVE_TOKEN_ID", "")
GDRIVE_MUSIC_ID   = os.environ.get("GDRIVE_MUSIC_ID", "")
SHEET_ID          = os.environ.get("SHEET_ID", "")
CHANNEL_NAME      = "DarkHistoryMind"

SW, SH = 1080, 1920
FPS    = 30

CAPTION_SIZE = 95
HOOK_SIZE    = 130
STROKE_W     = 8
CENTER_Y     = 0.50

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
                log.info(f"✅ Font: {fname}")
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

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1 — EXTENDED SCRIPT (45-50 SECONDS)
# ═══════════════════════════════════════════════════════════════════════════
def generate_extended_script(topic):
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)

        prompt = f"""Write an EXTREMELY VIRAL YouTube Shorts script for dark history.

Topic: {topic}

REQUIREMENTS:
- 200-240 words (45-50 seconds)
- First 3 seconds: SHOCKING hook
- 3-4 facts with emotional escalation
- End with cliffhanger
- Sound HUMAN, not AI
- Include dramatic pauses

Return ONLY valid JSON:
{{"hook":"","fact1":"","fact2":"","fact3":"","fact4":"","twist":"","cliffhanger":"","full_script":""}}

RULES:
- hook: 12-15 words, shocking
- fact1: 35-45 words
- fact2: 35-45 words
- fact3: 35-45 words
- fact4: 25-35 words
- twist: 20-25 words
- cliffhanger: 15-20 words
- NO labels in values
- NO repeated sentences
- NATURAL speech patterns"""

        def fetch_parse():
            r = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.9,
                max_tokens=1200
            )
            raw = r.choices[0].message.content.strip()
            match = re.search(r'\{.*\}', raw, re.DOTALL)
            if not match: return None
            d = json.loads(match.group())
            
            sections = ["hook","fact1","fact2","fact3","fact4","twist","cliffhanger"]
            for s in sections:
                val = d.get(s, "").strip()
                val = re.sub(r'^(hook|fact\s*\d|twist|cliffhanger|story|conclusion|fact):\s*', '', val, flags=re.IGNORECASE)
                d[s] = val

            d["full_script"] = " ".join(d[s] for s in sections if d[s])
            return d

        data = fetch_parse()
        if not data or "full_script" not in data:
            return None
            
        wc = len(data["full_script"].split())

        if not (190 <= wc <= 250):
            log.warning(f"Script word count ({wc}) out of range, retrying...")
            new_data = fetch_parse()
            if new_data and "full_script" in new_data:
                data = new_data
                wc = len(data["full_script"].split())

        log.info(f"✅ Extended script: {wc} words")
        return data
    except Exception as e:
        log.error(f"❌ Script generation failed: {e}")
        return None

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2 — VOICE (45-50 SECONDS)
# ═══════════════════════════════════════════════════════════════════════════
def generate_voice(script):
    try:
        import asyncio
        import nest_asyncio
        import edge_tts
        nest_asyncio.apply()

        text = script.get("full_script", "")
        if not text:
            log.error("❌ No script text")
            return False

        log.info("Generating voiceover...")
        
        async def speak():
            communicate = edge_tts.Communicate(
                text, 
                voice="en-GB-ThomasNeural", 
                rate="-20%",
                pitch="-10Hz"
            )
            await communicate.save("short_voice.mp3")

        asyncio.run(speak())
        
        if not os.path.exists("short_voice.mp3"):
            log.error("❌ Voice file not created")
            return False
            
        log.info("✅ Voice generated: short_voice.mp3")
        return True
    except Exception as e:
        log.error(f"❌ Voice generation failed: {e}")
        return False

def get_voice_duration():
    try:
        try:
            from moviepy.editor import AudioFileClip
        except ImportError:
            from moviepy import AudioFileClip
        clip = AudioFileClip("short_voice.mp3")
        dur = clip.duration
        clip.close()
        log.info(f"✅ Voice duration: {dur:.1f} seconds")
        return dur
    except Exception as e:
        log.error(f"❌ Cannot get voice duration: {e}")
        return 0.0

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3 — CAPTIONS (FIXED: Start from 0.0s)
# ═══════════════════════════════════════════════════════════════════════════
def build_captions(script, dur):
    sections = ["hook", "fact1", "fact2", "fact3", "fact4", "twist", "cliffhanger"]
    texts = [script.get(s, "") for s in sections]
    texts = [t for t in texts if t]
    
    if not texts: return []

    captions = []
    sec_dur = dur / len(texts)
    curr = 0.0
    
    for idx, text in enumerate(texts):
        is_hook = idx == 0
        is_twist = idx == len(texts) - 2
        
        captions.append({
            "text": text.upper(),
            "start": curr,
            "end": curr + sec_dur,
            "is_hook": is_hook,
            "is_twist": is_twist,
            "color": (255, 100, 100) if is_twist else (212, 175, 55)
        })
        curr += sec_dur
    
    log.info(f"✅ Captions synced: {len(captions)} captions from 0.0s")
    return captions

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4 — PROFESSIONAL ASSETS
# ═══════════════════════════════════════════════════════════════════════════
def fetch_premium_assets(topic, dur):
    os.makedirs("shorts_assets/videos", exist_ok=True)
    os.makedirs("shorts_assets/images", exist_ok=True)
    
    assets = {"videos": [], "images": []}
    
    num_videos = max(6, int(dur / 5))
    num_images = max(4, int(dur / 6))
    
    log.info(f"Fetching {num_videos} videos + {num_images} images")
    
    search_strategies = [topic, f"{topic} history", f"{topic} revelation", f"{topic} mystery", f"{topic} truth", f"{topic} dark"]
    
    video_count = 0
    for strategy in search_strategies:
        if video_count >= num_videos: break
        
        try:
            url = f"https://pixabay.com/api/videos/?key={PIXABAY_KEY}&q={strategy.replace(' ','+')}&per_page=5&order=trending"
            r = requests.get(url, timeout=30)
            videos = r.json().get("hits", [])
            
            for hit in videos[:4]:
                if video_count >= num_videos: break
                try:
                    vurl = hit["videos"]["medium"]["url"]
                    path = f"shorts_assets/videos/v{video_count}.mp4"
                    video_data = requests.get(vurl, timeout=60).content
                    open(path, "wb").write(video_data)
                    assets["videos"].append(path)
                    video_count += 1
                except: pass
        except: pass
    
    image_count = 0
    for strategy in search_strategies:
        if image_count >= num_images: break
        
        try:
            url = f"https://api.pexels.com/v1/search?query={strategy}&per_page=5&orientation=portrait"
            r = requests.get(url, headers={"Authorization": PEXELS_KEY}, timeout=30)
            photos = r.json().get("photos", [])
            
            for photo in photos[:3]:
                if image_count >= num_images: break
                try:
                    path = f"shorts_assets/images/img{image_count}.jpg"
                    img_data = requests.get(photo["src"]["large2x"], timeout=60).content
                    open(path, "wb").write(img_data)
                    assets["images"].append(path)
                    image_count += 1
                except: pass
        except: pass
    
    log.info(f"✅ Fetched {len(assets['videos'])} videos + {len(assets['images'])} images")
    return assets if (len(assets['videos']) >= 5 and len(assets['images']) >= 3) else None

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5 — PROFESSIONAL CAPTIONS RENDERING
# ═══════════════════════════════════════════════════════════════════════════
def render_caption(frame, caption_data, progress):
    from PIL import Image, ImageDraw
    img = Image.fromarray(frame)
    draw = ImageDraw.Draw(img)
    
    text = caption_data["text"]
    is_hook = caption_data.get("is_hook", False)
    color = caption_data.get("color", (212, 175, 55))
    
    font_size = HOOK_SIZE if is_hook else CAPTION_SIZE
    font = get_font(font_size)
    
    if is_hook:
        alpha = int(min(1.0, progress * 4.0) * 255)
    else:
        alpha = int(min(1.0, progress * 3.0) * 255) if progress < 0.33 else int(min(1.0, (1.0 - progress) * 3.0) * 255)
    
    alpha = max(0, min(255, alpha))
    
    words = text.split()
    lines = []
    curr_line = ""
    for w in words:
        test = f"{curr_line} {w}".strip()
        bb = draw.textbbox((0, 0), test, font=font)
        if bb[2] - bb[0] > SW * 0.9:
            if curr_line:
                lines.append(curr_line)
            curr_line = w
        else:
            curr_line = test
    if curr_line:
        lines.append(curr_line)
    lines = lines[:4]
    
    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    o_draw = ImageDraw.Draw(overlay)
    
    line_h = font_size + 15
    total_h = len(lines) * line_h
    y_start = int(SH * CENTER_Y) - total_h // 2
    
    for li, line in enumerate(lines):
        y_pos = y_start + li * line_h
        bb = o_draw.textbbox((0, 0), line, font=font)
        tw = bb[2] - bb[0]
        x_pos = (SW - tw) // 2
        
        for dx in range(-STROKE_W, STROKE_W + 1, 2):
            for dy in range(-STROKE_W, STROKE_W + 1, 2):
                if dx == 0 and dy == 0: continue
                o_draw.text((x_pos + dx, y_pos + dy), line, font=font, fill=(0, 0, 0, alpha))
        
        o_draw.text((x_pos, y_pos), line, font=font, fill=(*color, alpha))
    
    img = Image.alpha_composite(img.convert("RGBA"), overlay)
    return np.array(img.convert("RGB"))

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 6 — PREMIUM VIDEO ASSEMBLY
# ═══════════════════════════════════════════════════════════════════════════
def apply_premium_effects(frame, t, dur, intensity=1.0):
    import cv2
    
    progress = t / dur
    ease_progress = progress - math.sin(progress * 2 * math.pi) / (2 * math.pi)
    zoom_factor = 1.0 + (0.2 * ease_progress * intensity)
    
    h, w = frame.shape[:2]
    center_x, center_y = w // 2, h // 2
    
    zoom_matrix = cv2.getRotationMatrix2D((center_x, center_y), 0, zoom_factor)
    frame = cv2.warpAffine(frame, zoom_matrix, (w, h), borderMode=cv2.BORDER_REFLECT_101)
    
    kernel_x = cv2.getGaussianKernel(w, w * 0.4)
    kernel_y = cv2.getGaussianKernel(h, h * 0.4)
    kernel = kernel_y @ kernel_x.T
    mask = (kernel / kernel.max() * 255).astype(np.uint8)
    mask = np.dstack([mask] * 3)
    
    vignette = (frame.astype(float) * (mask.astype(float) / 255)).astype(np.uint8)
    frame = cv2.addWeighted(frame, 0.80, vignette, 0.20, 0)
    
    frame[:, :, 0] = np.clip(frame[:, :, 0] * 1.15, 0, 255)
    frame[:, :, 1] = np.clip(frame[:, :, 1] * 0.92, 0, 255)
    frame[:, :, 2] = np.clip(frame[:, :, 2] * 0.85, 0, 255)
    
    hsv = cv2.cvtColor(frame.astype(np.uint8), cv2.COLOR_BGR2HSV).astype(float)
    hsv[:, :, 1] = np.clip(hsv[:, :, 1] * 1.1, 0, 255)
    frame = cv2.cvtColor(hsv.astype(np.uint8), cv2.COLOR_HSV2BGR)
    
    return frame.astype(np.uint8)

def assemble_premium_short(assets, captions, dur, out_file):
    import cv2
    
    log.info("🎬 Assembling premium short...")
    
    temp_video = "temp_video.mp4"
    writer = cv2.VideoWriter(temp_video, cv2.VideoWriter_fourcc(*"mp4v"), FPS, (SW, SH))
    
    total_frames = int(dur * FPS)
    videos = assets.get("videos", [])
    images = assets.get("images", [])
    
    pool = []
    if videos:
        pool.extend([(v, "video") for v in videos] * 2)
    if images:
        pool.extend([(i, "image") for i in images])
    
    if not pool:
        log.error("❌ No assets")
        return False
    
    vi = 0
    last_path = None
    transition_frame = None
    transition_duration = 0.4
    transition_frames = int(transition_duration * FPS)
    
    caption_cuts = [cap["start"] for cap in captions]
    
    for frame_idx in range(total_frames):
        path, ftype = pool[vi % len(pool)]
        t = frame_idx / FPS
        
        try:
            if ftype == "video":
                cap = cv2.VideoCapture(path)
                if not cap.isOpened():
                    frame = np.zeros((SH, SW, 3), dtype=np.uint8)
                else:
                    fc = cap.get(cv2.CAP_PROP_FRAME_COUNT)
                    frame_pos = int(fc * (t % dur) / dur) if dur > 0 else 0
                    cap.set(cv2.CAP_PROP_POS_FRAMES, frame_pos)
                    ret, frame = cap.read()
                    cap.release()
                    if not ret: frame = np.zeros((SH, SW, 3), dtype=np.uint8)
            else:
                frame = cv2.imread(path)
                if frame is None: frame = np.zeros((SH, SW, 3), dtype=np.uint8)
            
            frame = cv2.resize(frame, (SW, SH))
            
            intensity = 1.2 if any(abs(t - cut) < 0.3 for cut in caption_cuts) else 1.0
            frame = apply_premium_effects(frame, t, dur, intensity)
            
            if path != last_path and transition_frame is not None:
                transition_progress = min(1.0, (frame_idx % transition_frames) / transition_frames)
                frame = cv2.addWeighted(transition_frame, 1.0 - transition_progress, frame, transition_progress, 0)
            
            transition_frame = frame.copy()
            last_path = path
            
            for cap_data in captions:
                if cap_data["start"] <= t <= cap_data["end"]:
                    prog = (t - cap_data["start"]) / (cap_data["end"] - cap_data["start"]) if (cap_data["end"] - cap_data["start"]) > 0 else 0.5
                    frame = render_caption(frame, cap_data, prog)
                    break
            
            writer.write(frame)
            
            if any(abs(t - cut) < 0.1 for cut in caption_cuts):
                vi += 1
            elif frame_idx > 0 and frame_idx % int(FPS * random.uniform(4, 6)) == 0:
                vi += 1
                
        except Exception as e:
            log.warning(f"Frame {frame_idx}: {e}")
            writer.write(np.zeros((SH, SW, 3), dtype=np.uint8))
    
    writer.release()
    log.info(f"✅ Video assembled: {temp_video}")
    
    try:
        from moviepy.editor import VideoFileClip, AudioFileClip
        
        log.info("🎵 Mixing audio...")
        
        video = VideoFileClip(temp_video)
        audio = AudioFileClip("short_voice.mp3")
        
        final_video = video.set_audio(audio)
        
        final_video.write_videofile(
            out_file,
            fps=FPS,
            codec='libx264',
            audio_codec='aac',
            preset='fast',
            verbose=False,
            logger=None,
            threads=4
        )
        
        video.close()
        audio.close()
        final_video.close()
        
        if os.path.exists(temp_video):
            try:
                os.remove(temp_video)
            except:
                pass
        
        log.info(f"✅ Short complete: {out_file}")
        return os.path.exists(out_file)
        
    except Exception as e:
        log.error(f"❌ Audio mixing error: {e}")
        if os.path.exists(temp_video):
            try:
                os.rename(temp_video, out_file)
                log.warning(f"⚠️ Using video without audio")
            except:
                pass
        return False

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 7 — VIRAL SEO (1 hashtag title + 15 hashtags description + 20-25 backend)
# ═══════════════════════════════════════════════════════════════════════════
def generate_viral_seo(topic, script, hook):
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)
        
        prompt = f"""YouTube Shorts viral SEO expert.

Topic: {topic}
Hook: {hook}
Script: {script.get('full_script', '')[:300]}

Create VIRAL SEO:

Return ONLY JSON:
{{"title":"","title_hashtag":"","description":"","hashtags":"","backend_keywords":""}}

RULES:
- title: 50-60 chars, NO hashtag
- title_hashtag: EXACTLY 1 hashtag (most viral)
- description: 150-180 chars, end with question
- hashtags: EXACTLY 15 hashtags space-separated
- backend_keywords: 20-25 keywords comma-separated

Hashtags: #DarkHistory #ShockingTruth #MustWatch #HistoryRevealed #Revealed #HistoryFacts #AncientSecrets #Mystery #Conspiracy #Hidden #Shorts #Viral #YouTube #TrendingNow #FYP

Backend keywords MUST relate to {topic}"""
        
        r = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.8,
            max_tokens=900
        )
        
        raw = r.choices[0].message.content.strip()
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if not match:
            return get_default_seo(topic, hook)
        
        seo = json.loads(match.group())
        
        hashtags_list = seo.get('hashtags', '').split()
        if len(hashtags_list) < 15:
            default_hashtags = "#DarkHistory #HistoryRevealed #ShockingTruth #MustWatch #HistoryFacts #AncientSecrets #Mystery #Conspiracy #Hidden #Shorts #Viral #YouTube #TrendingNow #FYP #Revealed"
            seo['hashtags'] = ' '.join(default_hashtags.split()[:15])
        elif len(hashtags_list) > 15:
            seo['hashtags'] = ' '.join(hashtags_list[:15])
        
        log.info(f"✅ SEO generated: 1 title hashtag + 15 desc hashtags + {len(seo.get('backend_keywords', '').split(','))} keywords")
        return seo
        
    except Exception as e:
        log.warning(f"SEO generation failed: {e}")
        return get_default_seo(topic, hook)

def get_default_seo(topic, hook):
    return {
        "title": f"{hook} - {topic[:40]}",
        "title_hashtag": "#DarkHistory",
        "description": f"Discover the shocking truth about {topic}. What we found will SHOCK you. 🎥",
        "hashtags": "#DarkHistory #HistoryRevealed #ShockingTruth #MustWatch #HistoryFacts #AncientSecrets #Mystery #Conspiracy #Hidden #Shorts #Viral #YouTube #TrendingNow #FYP #Revealed",
        "backend_keywords": f"{topic}, dark history, history facts, shocking truth, hidden history, mysterious, revelation, ancient secrets, conspiracy, historical facts, hidden truth, dark secrets, untold story, forbidden truth, lost history, buried secrets, dark reality, ancient evil, truth exposed, history documentary, revealed secrets, conspiracy theory, hidden knowledge, historical mystery"
    }

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 8 — PROFESSIONAL THUMBNAIL
# ═══════════════════════════════════════════════════════════════════════════
def gen_premium_thumbnail(topic, hook):
    from PIL import Image, ImageDraw
    
    img = Image.new("RGB", (1080, 1920), color=(12, 4, 8))
    d = ImageDraw.Draw(img)
    
    font_hook = get_font(140)
    font_sub = get_font(60)
    
    text = hook.upper()[:30]
    bb = d.textbbox((0, 0), text, font=font_hook)
    tw = bb[2] - bb[0]
    x = (1080 - tw) // 2
    
    for dx in range(-5, 6):
        for dy in range(-5, 6):
            if dx == 0 and dy == 0: continue
            d.text((x + dx, 800 + dy), text, fill=(0, 0, 0), font=font_hook)
    
    d.text((x, 800), text, fill=(255, 80, 80), font=font_hook)
    
    subtitle = "DARK HISTORY"
    d.text((100, 400), subtitle, fill=(212, 175, 55), font=font_sub)
    
    path = f"thumb_{int(time.time())}.jpg"
    img.save(path, quality=98)
    return path

# ═══════════════════════════════════════════════════════════════════════════
# AUTH & UPLOAD
# ═══════════════════════════════════════════════════════════════════════════
def setup_auth():
    files = [
        ("client_secrets.json", GDRIVE_SECRETS_ID),
        ("youtube_token.pkl", GDRIVE_TOKEN_ID),
    ]
    for name, fid in files:
        if not fid:
            log.warning(f"⚠️ {name} file ID not set")
            continue
        if os.path.exists(name): continue
        try:
            url = f"https://drive.google.com/uc?export=download&id={fid}"
            r = requests.get(url, timeout=60)
            if r.status_code == 200:
                open(name, "wb").write(r.content)
                log.info(f"✅ Auth: {name}")
        except Exception as e:
            log.warning(f"Auth {name}: {e}")

# DAILY SCHEDULE: 12 AM, 6 AM, 12 PM, 6 PM IST
DAILY_SCHEDULE = [
    {"hour": 18, "minute": 30, "label": "12:00 AM IST"},
    {"hour": 0, "minute": 30, "label": "6:00 AM IST"},
    {"hour": 6, "minute": 30, "label": "12:00 PM IST"},
    {"hour": 12, "minute": 30, "label": "6:00 PM IST"},
]

def get_schedule(slot):
    """Get schedule time for slot 0-3"""
    now = datetime.datetime.utcnow()
    cfg = DAILY_SCHEDULE[slot % 4]
    
    base = now.replace(hour=cfg["hour"], minute=cfg["minute"], second=0, microsecond=0)
    t = base
    if t <= now:
        t += datetime.timedelta(days=1)
    
    ist = t + datetime.timedelta(hours=5, minutes=30)
    log.info(f"📅 Slot {slot}: {t.strftime('%Y-%m-%d %H:%M')} UTC = {ist.strftime('%d %b %I:%M %p')} IST")
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
        log.error("❌ YouTube token invalid"); return None
    return build("youtube","v3",credentials=creds,cache_discovery=False)

def upload(video_file, seo, thumbnail, slot=0):
    from googleapiclient.http import MediaFileUpload
    yt = get_yt()
    if not yt: return None, None
    pub = get_schedule(slot)
    
    # Title with 1 hashtag
    title = f"{seo['title']} {seo['title_hashtag']}"
    
    # Description with 15 hashtags
    description = f"""{seo['description']}

{seo['hashtags']}"""
    
    body = {
        "snippet": {
            "title": title[:100],
            "description": description[:5000],
            "tags": seo.get("title_hashtag", "").replace("#", "").split(),
            "categoryId": "27",
            "defaultLanguage": "en"
        },
        "status": {
            "privacyStatus": "private",
            "publishAt": pub,
            "selfDeclaredMadeForKids": False
        }
    }
    
    try:
        media = MediaFileUpload(video_file, mimetype="video/mp4", resumable=True, chunksize=5*1024*1024)
        request = yt.videos().insert(part="snippet,status", body=body, media_body=media)
        response = None
        while response is None:
            st, response = request.next_chunk()
            if st: log.info(f"  {int(st.progress()*100)}%")
        
        vid = response["id"]
        url = f"https://youtube.com/watch?v={vid}"
        log.info(f"✅ Uploaded: {url}")
        
        try:
            yt.thumbnails().set(videoId=vid, media_body=MediaFileUpload(thumbnail)).execute()
            log.info("✅ Thumbnail set")
        except: pass
        
        return vid, url
    except Exception as e:
        log.error(f"❌ Upload failed: {e}")
        return None, None

def update_sheet(topic, url, title, num):
    try:
        import gspread
        import google.auth.transport.requests
        creds = None
        if os.path.exists("youtube_token.pkl"):
            with open("youtube_token.pkl", "rb") as f: creds = pickle.load(f)
        if creds and hasattr(creds,"expired") and creds.expired and hasattr(creds,"refresh_token") and creds.refresh_token:
            try: creds.refresh(google.auth.transport.requests.Request())
            except: pass
        if not creds: return
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)
        ws = sh.get_worksheet(0)
        ws.append_row([topic, datetime.datetime.now().strftime("%Y-%m-%d %H:%M"), title, url, "Scheduled"])
    except Exception as e:
        log.warning(f"Sheet update failed: {e}")

# ═══════════════════════════════════════════════════════════════════════════
# TOPIC GENERATION
# ═══════════════════════════════════════════════════════════════════════════
def get_viral_topics(n=4):
    """Get 4 viral topics for today"""
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)
        
        prompt = f"""Generate {n} VIRAL dark history topics for YouTube Shorts TODAY.
Each topic:
- Shocking/mysterious
- Lesser-known
- Engaging
- Unique (don't repeat)

Return ONLY JSON array:
["topic1","topic2",...]"""
        
        r = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.95,
            max_tokens=500
        )
        
        raw = r.choices[0].message.content.strip()
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if match:
            topics = json.loads(match.group())
            return topics[:n] if len(topics) >= n else topics + ["Dark History Mystery"] * (n - len(topics))
    except Exception as e:
        log.warning(f"Topic generation failed: {e}")
    
    return ["Dark History Mystery #1", "Dark History Mystery #2", "Dark History Mystery #3", "Dark History Mystery #4"]

# ═══════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE: 4 SHORTS PER DAY
# ═══════════════════════════════════════════════════════════════════════════
def run_daily_shorts():
    """Generate and upload 4 shorts daily"""
    log.info("="*70)
    log.info("🚀 DARKHISTORYMIND: DAILY SHORTS PIPELINE (4 SHORTS)")
    log.info("="*70)
    log.info(f"📅 {datetime.datetime.now().strftime('%Y-%m-%d')}")
    log.info("⏰ Posting at: 12 AM, 6 AM, 12 PM, 6 PM IST")
    log.info("="*70)
    
    if not GROQ_KEY or not PEXELS_KEY or not PIXABAY_KEY:
        log.error("❌ Missing required API keys!")
        log.error("   Required: GROQ_KEY, PEXELS_KEY, PIXABAY_KEY")
        return False

    download_fonts()
    setup_auth()
    
    if not os.path.exists("shorts_assets"):
        os.makedirs("shorts_assets")

    # Get 4 topics for today
    topics = get_viral_topics(n=4)
    log.info(f"\n📌 Today's Topics:")
    for i, topic in enumerate(topics, 1):
        log.info(f"   {i}. {topic}")
    log.info("")
    
    results = []
    for slot, topic in enumerate(topics):
        log.info(f"\n{'─'*70}")
        log.info(f"📹 SHORT #{slot+1}: {topic}")
        log.info(f"{'─'*70}")
        
        # Generate script
        script = generate_extended_script(topic)
        if not script:
            log.error("❌ Script generation failed")
            results.append(None)
            continue
        
        # Generate voice
        if not generate_voice(script):
            log.error("❌ Voice generation failed")
            results.append(None)
            continue
        
        # Get duration
        dur = get_voice_duration()
        if dur < 40 or dur > 55:
            log.warning(f"⚠️  Duration {dur:.1f}s outside 40-55s range")
        
        # Build captions
        captions = build_captions(script, dur)
        
        # Fetch assets
        assets = fetch_premium_assets(topic, dur)
        if not assets:
            log.error("❌ No assets found")
            results.append(None)
            continue
        
        # Assemble video
        safe_topic = re.sub(r'[^\w\s]', '', topic).strip().replace(' ', '_')[:30]
        out_file = f"short_{slot+1}_{safe_topic}.mp4"
        
        success = assemble_premium_short(assets, captions, dur, out_file)
        if not success or not os.path.exists(out_file):
            log.error("❌ Assembly failed")
            results.append(None)
            continue
        
        # Generate SEO
        seo = generate_viral_seo(topic, script, script.get("hook", ""))
        
        # Upload
        try:
            thumb = gen_premium_thumbnail(topic, script.get("hook", ""))
            _, url = upload(out_file, seo, thumb, slot=slot)
            
            if url:
                update_sheet(topic, url, seo["title"], slot+1)
                log.info(f"\n✅ SHORT #{slot+1} READY")
                log.info(f"   Duration: {dur:.1f}s")
                log.info(f"   Title: {seo['title']}")
                log.info(f"   URL: {url}")
                results.append(url)
            else:
                results.append(None)
        except Exception as e:
            log.error(f"❌ Upload failed: {e}")
            results.append(None)
    
    log.info("\n" + "="*70)
    log.info("✅ DAILY SHORTS PIPELINE COMPLETE")
    log.info("="*70)
    for i, url in enumerate(results, 1):
        status = "✅ POSTED" if url else "❌ FAILED"
        log.info(f"Short #{i}: {status}")
    
    return all(results)

if __name__ == "__main__":
    run_daily_shorts()
