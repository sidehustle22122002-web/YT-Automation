#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════
# DarkHistoryMind — Shorts Pipeline (FIXED - Audio + Assets)
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
# ══════════════════════════════════════════════════════
def generate_script(topic):
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)

        prompt = f"""Write a YouTube Shorts dark history script.
Topic: {topic}
EXACT REQUIREMENT: full_script must be between 120-140 words total.

Return ONLY valid JSON:
{{"hook":"","fact1":"","fact2":"","fact3":"","story":"","conclusion":"","full_script":""}}

Rules:
- hook: One brutal statement (8-12 words).
- fact1: Dark fact (20-25 words).
- fact2: Darker fact (20-25 words).
- fact3: Hidden truth (20-25 words).
- story: Shocking narrative (25-35 words).
- conclusion: Final statement (12-18 words).
- No labels like 'Hook:' or 'Fact:' in the values.
- No repeated sentences."""

        def fetch_parse():
            r = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.8,
                max_tokens=700
            )
            raw = r.choices[0].message.content.strip()
            match = re.search(r'\{.*\}', raw, re.DOTALL)
            if not match: return None
            d = json.loads(match.group())
            
            sections = ["hook","fact1","fact2","fact3","story","conclusion"]
            for s in sections:
                val = d.get(s, "").strip()
                val = re.sub(r'^(hook|fact\s*\d|story|conclusion|fact):\s*', '', val, flags=re.IGNORECASE)
                d[s] = val

            d["full_script"] = " ".join(d[s] for s in sections if d[s])
            return d

        data = fetch_parse()
        wc = len(data["full_script"].split())

        for attempt in range(2):
            if 110 <= wc <= 150:
                break
            log.warning(f"Script word count ({wc}) out of range. Retry {attempt+1}")
            new_data = fetch_parse()
            if new_data:
                data = new_data
                wc = len(data["full_script"].split())

        log.info(f"Script: {wc} words")
        return data
    except Exception as e:
        log.error(f"Script generation failed: {e}")
        return None

# ══════════════════════════════════════════════════════
# SECTION 2 — VOICE (using edge-tts)
# ══════════════════════════════════════════════════════
def generate_voice(script):
    try:
        import asyncio
        import nest_asyncio
        import edge_tts
        nest_asyncio.apply()

        text = script.get("full_script", "")
        if not text:
            log.error("❌ No script text for voice generation")
            return False

        log.info("Generating voice...")
        
        async def speak():
            communicate = edge_tts.Communicate(
                text, 
                voice="en-GB-ThomasNeural", 
                rate="-15%", 
                pitch="-5Hz"
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
        import traceback
        log.error(traceback.format_exc())
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

# ══════════════════════════════════════════════════════
# SECTION 3 — CAPTIONS
# ══════════════════════════════════════════════════════
def build_captions(script, dur):
    sections = ["hook", "fact1", "fact2", "fact3", "story", "conclusion"]
    texts = [script.get(s, "") for s in sections]
    texts = [t for t in texts if t]
    
    if not texts: return []

    captions = []
    sec_dur = dur / len(texts)
    curr = 0.0
    for text in texts:
        captions.append({"text": text.upper(), "start": curr, "end": curr + sec_dur})
        curr += sec_dur
    return captions

# ══════════════════════════════════════════════════════
# SECTION 4 — ASSETS (FIXED - Diverse Videos + Images)
# ══════════════════════════════════════════════════════
def fetch_assets(topic, dur):
    """
    FIXED: Fetch diverse videos and images based on duration
    For 35-40 sec short: 5 videos + 5 images
    For 40-45 sec short: 6 videos + 6 images
    """
    os.makedirs("shorts_assets/videos", exist_ok=True)
    os.makedirs("shorts_assets/images", exist_ok=True)
    
    assets = {"videos": [], "images": []}
    
    # Calculate how many assets needed based on duration
    # Each asset ~7-8 seconds on screen
    num_videos = max(4, int(dur / 7))
    num_images = max(3, int(dur / 8))
    
    log.info(f"Fetching {num_videos} videos + {num_images} images for {dur:.1f}s duration")
    
    # Generate diverse search queries based on topic
    keywords = topic.split()[:2]  # First 2 words
    search_variants = [
        topic,
        keywords[0] if keywords else topic,
        f"{topic} history",
        f"{topic} dark",
        f"{keywords[0]} facts" if keywords else topic,
    ]
    
    # Fetch DIFFERENT videos using different search queries
    video_count = 0
    for search_idx, search_query in enumerate(search_variants):
        if video_count >= num_videos:
            break
        
        url = f"https://pixabay.com/api/videos/?key={PIXABAY_KEY}&q={search_query.replace(' ','+')}&per_page=5&order=popular"
        try:
            r = requests.get(url, timeout=30)
            videos = r.json().get("hits", [])
            
            # Fetch up to 3 videos per search query
            for idx, hit in enumerate(videos[:3]):
                if video_count >= num_videos:
                    break
                try:
                    vurl = hit["videos"]["medium"]["url"]
                    path = f"shorts_assets/videos/v{video_count}.mp4"
                    video_data = requests.get(vurl, timeout=60).content
                    open(path, "wb").write(video_data)
                    assets["videos"].append(path)
                    video_count += 1
                    log.info(f"  Video {video_count}/{num_videos}: {search_query}")
                except:
                    pass
        except:
            pass
    
    # Fetch DIFFERENT images using different search queries
    image_count = 0
    for search_idx, search_query in enumerate(search_variants):
        if image_count >= num_images:
            break
        
        url = f"https://api.pexels.com/v1/search?query={search_query}&per_page=5&orientation=portrait"
        try:
            r = requests.get(url, headers={"Authorization": PEXELS_KEY}, timeout=30)
            photos = r.json().get("photos", [])
            
            # Fetch up to 2 images per search query
            for idx, photo in enumerate(photos[:2]):
                if image_count >= num_images:
                    break
                try:
                    path = f"shorts_assets/images/img{image_count}.jpg"
                    img_data = requests.get(photo["src"]["large2x"], timeout=60).content
                    open(path, "wb").write(img_data)
                    assets["images"].append(path)
                    image_count += 1
                    log.info(f"  Image {image_count}/{num_images}: {search_query}")
                except:
                    pass
        except:
            pass
    
    log.info(f"✅ Fetched {len(assets['videos'])} videos + {len(assets['images'])} images")
    return assets if (assets["videos"] and assets["images"]) else None

# ══════════════════════════════════════════════════════
# SECTION 5 — CAPTIONS RENDERING
# ══════════════════════════════════════════════════════
def render_caption(frame, text, progress):
    from PIL import Image, ImageDraw
    img = Image.fromarray(frame)
    draw = ImageDraw.Draw(img)
    
    font_cap = get_font(CAPTION_SIZE)
    font_hook = get_font(HOOK_SIZE)
    
    is_hook = progress < 0.15
    font_use = font_hook if is_hook else font_cap
    
    alpha = min(1.0, progress * 3.0) if progress < 0.3 else min(1.0, (1.0 - progress) * 3.0)
    alpha = int(255 * alpha)
    
    words = text.split()
    lines = []
    curr_line = ""
    for w in words:
        test = f"{curr_line} {w}".strip()
        bb = draw.textbbox((0, 0), test, font=font_use)
        if bb[2] - bb[0] > SW * 0.85:
            if curr_line:
                lines.append(curr_line)
            curr_line = w
        else:
            curr_line = test
    if curr_line:
        lines.append(curr_line)
    lines = lines[:3]
    
    overlay = Image.new("RGBA", img.size, (0, 0, 0, 0))
    o_draw = ImageDraw.Draw(overlay)
    
    line_h = CAPTION_SIZE + 20
    total_h = len(lines) * line_h
    y_start = int(SH * CENTER_Y) - total_h // 2
    
    for li, line in enumerate(lines):
        y_pos = y_start + li * line_h
        bb = o_draw.textbbox((0, 0), line, font=font_use)
        tw = bb[2] - bb[0]
        x_pos = (SW - tw) // 2
        
        for dx in range(-STROKE_W, STROKE_W + 1, 2):
            for dy in range(-STROKE_W, STROKE_W + 1, 2):
                if dx == 0 and dy == 0: continue
                o_draw.text((x_pos + dx, y_pos + dy), line, font=font_use, fill=(0, 0, 0, alpha))
        o_draw.text((x_pos, y_pos), line, font=font_use, fill=(255, 255, 255, alpha))
    
    img = Image.alpha_composite(img.convert("RGBA"), overlay)
    return np.array(img.convert("RGB"))

# ══════════════════════════════════════════════════════
# SECTION 6 — ASSEMBLY (WITH CINEMATIC EFFECTS + AUDIO)
# ══════════════════════════════════════════════════════
def apply_cinematic_effects(frame, t, dur):
    """Apply cinematic effects: zoom, vignette, color grading"""
    import cv2
    
    zoom_factor = 1.0 + (0.15 * np.sin(t * np.pi / dur))
    h, w = frame.shape[:2]
    center_x, center_y = w // 2, h // 2
    
    zoom_matrix = cv2.getRotationMatrix2D((center_x, center_y), 0, zoom_factor)
    frame = cv2.warpAffine(frame, zoom_matrix, (w, h), borderMode=cv2.BORDER_REFLECT)
    
    kernel_x = cv2.getGaussianKernel(w, w/3)
    kernel_y = cv2.getGaussianKernel(h, h/3)
    kernel = kernel_y @ kernel_x.T
    mask = (kernel / kernel.max() * 255).astype(np.uint8)
    mask = np.dstack([mask] * 3)
    
    frame = cv2.addWeighted(frame, 0.95, np.zeros_like(frame), 0, 0)
    vignette = (frame.astype(float) * (mask.astype(float) / 255)).astype(np.uint8)
    frame = cv2.addWeighted(frame, 0.85, vignette, 0.15, 0)
    
    frame[:, :, 0] = np.clip(frame[:, :, 0] * 1.1, 0, 255)
    frame[:, :, 1] = np.clip(frame[:, :, 1] * 0.95, 0, 255)
    frame[:, :, 2] = np.clip(frame[:, :, 2] * 0.9, 0, 255)
    
    return frame.astype(np.uint8)

def assemble(assets, captions, dur, out_file):
    """
    FIXED: Creates video, mixes with audio, aligns assets with duration
    """
    import cv2
    
    log.info("Assembling video with assets and effects...")
    
    # Create temporary video without audio
    temp_video = "temp_video.mp4"
    writer = cv2.VideoWriter(temp_video, cv2.VideoWriter_fourcc(*"mp4v"), FPS, (SW, SH))
    
    total_frames = int(dur * FPS)
    videos = assets.get("videos", [])
    images = assets.get("images", [])
    
    # Create smart pool: mix videos and images, more videos than images
    pool = []
    if videos:
        pool.extend([(v, "video") for v in videos] * 2)
    if images:
        pool.extend([(i, "image") for i in images])
    
    if not pool:
        log.error("❌ No assets in pool")
        return False
    
    # Distribute assets evenly across duration
    asset_duration = dur / len(pool)
    vi = 0
    last_path = None
    transition_frame = None
    transition_duration = 0.5
    transition_frames = int(transition_duration * FPS)
    
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
            frame = apply_cinematic_effects(frame, t, dur)
            
            # Handle transitions
            if path != last_path and transition_frame is not None:
                transition_progress = min(1.0, (frame_idx % transition_frames) / transition_frames)
                frame = cv2.addWeighted(
                    transition_frame, 
                    1.0 - transition_progress,
                    frame, 
                    transition_progress,
                    0
                )
            
            transition_frame = frame.copy()
            last_path = path
            
            # Render captions
            for cap in captions:
                if cap["start"] <= t <= cap["end"]:
                    prog = (t - cap["start"]) / (cap["end"] - cap["start"]) if (cap["end"] - cap["start"]) > 0 else 0.5
                    frame = render_caption(frame, cap["text"], prog)
                    break
            
            writer.write(frame)
            
            # Change asset based on duration distribution
            if frame_idx > 0 and frame_idx % int(asset_duration * FPS) == 0:
                vi += 1
                
        except Exception as e:
            log.warning(f"Frame {frame_idx}: {e}")
            writer.write(np.zeros((SH, SW, 3), dtype=np.uint8))
    
    writer.release()
    log.info(f"✅ Video assembled (without audio): {temp_video}")
    
    # ══════════════════════════════════════════════════════
    # MIX AUDIO INTO VIDEO (CRITICAL FIX)
    # ══════════════════════════════════════════════════════
    try:
        from moviepy.editor import VideoFileClip, AudioFileClip, CompositeAudioClip
        
        log.info("Mixing audio into video...")
        
        # Load video and audio
        video = VideoFileClip(temp_video)
        audio = AudioFileClip("short_voice.mp3")
        
        # Set audio to video
        final_video = video.set_audio(audio)
        
        # Write final video with audio
        final_video.write_videofile(
            out_file,
            codec='libx264',
            audio_codec='aac',
            verbose=False,
            logger=None
        )
        
        # Cleanup
        video.close()
        audio.close()
        final_video.close()
        
        # Remove temporary file
        if os.path.exists(temp_video):
            os.remove(temp_video)
        
        log.info(f"✅ Final video with audio: {out_file}")
        return os.path.exists(out_file)
        
    except Exception as e:
        log.error(f"❌ Audio mixing failed: {e}")
        import traceback
        log.error(traceback.format_exc())
        
        # Fallback: rename temp video without audio
        if os.path.exists(temp_video):
            os.rename(temp_video, out_file)
            log.warning(f"⚠️  Using video without audio as fallback")
            return True
        
        return False

# ══════════════════════════════════════════════════════
# SECTION 7 — METADATA
# ══════════════════════════════════════════════════════
def gen_thumbnail(topic, hook):
    from PIL import Image, ImageDraw
    img = Image.new("RGB", (1080, 1920), color=(10, 5, 10))
    d = ImageDraw.Draw(img)
    font = get_font(120)
    text = hook.upper()[:25]
    bb = d.textbbox((0, 0), text, font=font)
    tw = bb[2] - bb[0]
    x = (1080 - tw) // 2
    y = 900
    d.text((x, y), text, fill=(255, 100, 100), font=font)
    path = f"thumb_{int(time.time())}.jpg"
    img.save(path, quality=95)
    return path

def gen_seo(topic, script, hook):
    return {
        "title": f"{hook}",
        "description": f"#DarkHistory #{topic.split()[0]}",
        "tags": ["dark history", "history facts", "mystery"]
    }

# ══════════════════════════════════════════════════════
# SECTION 8 — AUTH
# ══════════════════════════════════════════════════════
def setup_auth():
    files = [
        ("client_secrets.json", os.environ["GDRIVE_SECRETS_ID"]),
        ("youtube_token.pkl", os.environ["GDRIVE_TOKEN_ID"]),
    ]
    for name, fid in files:
        if os.path.exists(name): continue
        try:
            url = f"https://drive.google.com/uc?export=download&id={fid}"
            r = requests.get(url, timeout=60)
            if r.status_code == 200:
                open(name, "wb").write(r.content)
                log.info(f"Auth: {name}")
        except Exception as e:
            log.warning(f"Auth {name}: {e}")

# ══════════════════════════════════════════════════════
# SECTION 9 — UPLOAD + SCHEDULE
# ══════════════════════════════════════════════════════
SLOT_SCHEDULE = {
    0: {"hour": 18, "minute": 30, "days_ahead": -1, "label": "12:00 AM IST — upload day"},
    1: {"hour": 0, "minute": 30, "days_ahead": 0, "label": "6:00 AM IST — upload day"},
    2: {"hour": 6, "minute": 30, "days_ahead": 0, "label": "12:00 PM IST — upload day"},
    3: {"hour": 18, "minute": 30, "days_ahead": 0, "label": "12:00 AM IST — gap day"},
    4: {"hour": 0, "minute": 30, "days_ahead": 1, "label": "6:00 AM IST — gap day"},
    5: {"hour": 6, "minute": 30, "days_ahead": 1, "label": "12:00 PM IST — gap day"},
    6: {"hour": 12, "minute": 30, "days_ahead": 1, "label": "6:00 PM IST — gap day"},
}

def get_schedule(slot):
    now = datetime.datetime.utcnow()
    cfg = SLOT_SCHEDULE.get(slot, SLOT_SCHEDULE[0])
    base = now.replace(hour=cfg["hour"], minute=cfg["minute"], second=0, microsecond=0)
    t = base + datetime.timedelta(days=cfg["days_ahead"])
    if t <= now:
        t += datetime.timedelta(days=1)
    ist = t + datetime.timedelta(hours=5, minutes=30)
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

def upload(video_file, seo, thumbnail, offset=0):
    from googleapiclient.http import MediaFileUpload
    yt = get_yt()
    if not yt: return None, None
    pub = get_schedule(offset)
    body = {
        "snippet": {"title": seo["title"], "description": seo["description"],
                    "tags": seo.get("tags", []), "categoryId": "27", "defaultLanguage": "en"},
        "status": {"privacyStatus": "private", "publishAt": pub, "selfDeclaredMadeForKids": False}
    }
    try:
        media = MediaFileUpload(video_file, mimetype="video/mp4", resumable=True, chunksize=5*1024*1024)
        request = yt.videos().insert(part="snippet,status", body=body, media_body=media)
        response = None
        while response is None:
            st, response = request.next_chunk()
            if st: log.info(f"  {int(st.progress()*100)}%")
        vid = response["id"]; url = f"https://youtube.com/watch?v={vid}"
        log.info(f"✅ Uploaded: {url}")
        try:
            yt.thumbnails().set(videoId=vid, media_body=MediaFileUpload(thumbnail)).execute()
            log.info("✅ Thumbnail set")
        except Exception as e: log.warning(f"Thumb: {e}")
        return vid, url
    except Exception as e:
        log.error(f"❌ Upload failed: {e}")
        return None, None

# ══════════════════════════════════════════════════════
# SECTION 10 — SHEET
# ══════════════════════════════════════════════════════
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

# ══════════════════════════════════════════════════════
# SECTION 11 — RELATED TOPICS
# ══════════════════════════════════════════════════════
def get_related_topics(base, n=7):
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)
        prompt = f"""Give {n} distinct dark history sub-topics related to: {base}
Return ONLY a JSON array of strings, one topic per line, no labels.
Example: ["topic1","topic2",...]"""
        r = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.9,
            max_tokens=300
        )
        raw = r.choices[0].message.content.strip()
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if match:
            topics = json.loads(match.group())
            return topics[:n] if len(topics) >= n else topics + [base] * (n - len(topics))
    except Exception as e:
        log.warning(f"Related topics failed: {e}")
    return [base] * n

# ══════════════════════════════════════════════════════
# SECTION 12 — RUN SHORT
# ══════════════════════════════════════════════════════
def run_short(topic, num, offset=0):
    log.info(f"\n{'='*55}\nShort #{num}: {topic}\n{'='*55}")
    
    script = generate_script(topic)
    if not script:
        log.error("ABORT: Script generation failed.")
        return None
    
    if not generate_voice(script):
        log.error("ABORT: Voice generation failed.")
        return None
    
    dur = get_voice_duration()
    if dur <= 0:
        log.error("ABORT: Invalid voice duration.")
        return None

    captions = build_captions(script, dur)

    # FIXED: Pass duration to fetch_assets for proper asset count
    assets = fetch_assets(topic, dur)
    if not assets:
        log.error(f"❌ No assets found for {topic}")
        return None

    safe_topic = re.sub(r'[^\w\s]', '', topic).strip().replace(' ', '_')[:30]
    out_file = f"short_{num}_{safe_topic}.mp4"
    
    success = assemble(assets, captions, dur, out_file)
    if not success or not os.path.exists(out_file):
        log.error("ABORT: Assembly failed.")
        return None

    try:
        thumb = gen_thumbnail(topic, script.get("hook", ""))
        seo = gen_seo(topic, script, script.get("hook", ""))
        _, url = upload(out_file, seo, thumb, offset)

        if url:
            update_sheet(topic, url, seo["title"], num)
            log.info(f"✅ Short #{num} Published: {url}")
            return url
    except Exception as e:
        log.error(f"❌ Upload/Sheet phase failed: {e}")
    
    return None

def main():
    log.info("="*55 + "\nDARK HISTORY MIND: SHORTS PIPELINE\n" + "="*55)
    
    if not GROQ_KEY or not PEXELS_KEY:
        log.error("Missing API Keys in Environment!")
        sys.exit(1)

    download_fonts()
    setup_auth()
    
    if not os.path.exists("shorts_assets"):
        os.makedirs("shorts_assets")

    base_topic = LONG_VIDEO_TOPIC.strip() or "Darkest Secrets of History"
    log.info(f"Base Topic: {base_topic}")

    topics = get_related_topics(base_topic, n=7)
    
    results = []
    for i, topic in enumerate(topics, 1):
        url = run_short(topic, num=i, offset=i-1)
        results.append(url)
        
        if i < len(topics):
            log.info("Waiting 20s before next short...")
            time.sleep(20)

    log.info("\n" + "="*55 + "\nALL SHORTS PROCESSED\n" + "="*55)
    for i, url in enumerate(results, 1):
        status = url if url else "FAILED"
        log.info(f"Short #{i}: {status}")

if __name__ == "__main__":
    main()
