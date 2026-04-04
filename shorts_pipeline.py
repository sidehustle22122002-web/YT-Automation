#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════════════════
# DarkHistoryMind — VIRAL SHORTS PIPELINE (Professional Grade)
# Designed for maximum engagement, retention, and viral potential
# 45-50 second shorts with professional SEO and human-touch editing
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

GROQ_KEY          = os.environ["GROQ_KEY"]
PEXELS_KEY        = os.environ["PEXELS_KEY"]
PIXABAY_KEY       = os.environ["PIXABAY_KEY"]
GDRIVE_SECRETS_ID = os.environ["GDRIVE_SECRETS_ID"]
GDRIVE_TOKEN_ID   = os.environ["GDRIVE_TOKEN_ID"]
GDRIVE_MUSIC_ID   = os.environ.get("GDRIVE_MUSIC_ID", "")
SHEET_ID          = os.environ["SHEET_ID"]
LONG_VIDEO_TOPIC  = os.environ.get("LONG_VIDEO_TOPIC", "")
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
    """
    Generate 200-240 word script for 45-50 second shorts
    Includes dramatic hook, multiple facts, cliffhanger, and call-to-action
    """
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)

        prompt = f"""Write an EXTREMELY VIRAL YouTube Shorts script for dark history that makes people STOP SCROLLING.

Topic: {topic}

CRITICAL REQUIREMENTS:
- Total word count: 200-240 words (for 45-50 seconds)
- First 3 seconds: SHOCKING hook that makes people stop scrolling
- Include 3-4 different facts building on each other
- Emotional escalation throughout
- End with MAJOR revelation or cliffhanger
- Make it sound like a real person telling a story (NOT AI)
- Include dramatic pauses and emphasis words

Return ONLY valid JSON with these fields:
{{"hook":"","fact1":"","fact2":"","fact3":"","fact4":"","twist":"","cliffhanger":"","full_script":""}}

RULES:
- hook: Shocking opening statement (12-15 words) - MUST make people stop scrolling
- fact1: First revelation (35-45 words) - Build intrigue
- fact2: Deeper truth (35-45 words) - Escalate emotion
- fact3: Hidden detail (35-45 words) - Add complexity
- fact4: Dark element (25-35 words) - Increase tension
- twist: Unexpected turn (20-25 words) - Shock moment
- cliffhanger: Ending question (15-20 words) - Make them want more
- full_script: Complete script combining all sections
- NO labels like "Hook:" in values - JUST the content
- NO repeated sentences
- Sound NATURAL and HUMAN-like, not robotic"""

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
        wc = len(data["full_script"].split())

        for attempt in range(2):
            if 190 <= wc <= 250:
                break
            log.warning(f"Script word count ({wc}) out of range. Retry {attempt+1}")
            new_data = fetch_parse()
            if new_data:
                data = new_data
                wc = len(data["full_script"].split())

        log.info(f"✅ Extended script: {wc} words (~{wc*60//130:.0f} seconds)")
        return data
    except Exception as e:
        log.error(f"❌ Extended script generation failed: {e}")
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

        log.info("Generating extended voiceover...")
        
        async def speak():
            # Slower rate for dramatic effect (keeps engagement)
            communicate = edge_tts.Communicate(
                text, 
                voice="en-GB-ThomasNeural", 
                rate="-20%",  # 20% slower for dramatic pause effect
                pitch="-10Hz"  # Slightly lower pitch for authority
            )
            await communicate.save("short_voice.mp3")

        asyncio.run(speak())
        
        if not os.path.exists("short_voice.mp3"):
            log.error("❌ Voice file not created")
            return False
            
        log.info("✅ Extended voice: short_voice.mp3")
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

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3 — CAPTIONS (RETENTION FOCUSED)
# ═══════════════════════════════════════════════════════════════════════════
def build_captions(script, dur):
    """
    Build captions with retention focus:
    - Highlight shocking words
    - Strategic pauses with visuals
    - Emotional keywords in different colors
    """
    sections = ["hook", "fact1", "fact2", "fact3", "fact4", "twist", "cliffhanger"]
    texts = [script.get(s, "") for s in sections]
    texts = [t for t in texts if t]
    
    if not texts: return []

    captions = []
    sec_dur = dur / len(texts)
    curr = 0.0
    
    for idx, text in enumerate(texts):
        is_hook = idx == 0  # First section is hook
        is_twist = idx == len(texts) - 2  # Twist section
        
        captions.append({
            "text": text.upper(),
            "start": curr,
            "end": curr + sec_dur,
            "is_hook": is_hook,
            "is_twist": is_twist,
            "color": (255, 100, 100) if is_twist else (212, 175, 55)  # Red for twist, gold for others
        })
        curr += sec_dur
    
    return captions

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4 — PROFESSIONAL ASSETS (8-10 per short)
# ═══════════════════════════════════════════════════════════════════════════
def fetch_premium_assets(topic, dur):
    """
    Fetch 8-10 premium assets (videos + images) for 45-50 second duration
    Uses multiple search strategies for maximum variety
    """
    os.makedirs("shorts_assets/videos", exist_ok=True)
    os.makedirs("shorts_assets/images", exist_ok=True)
    
    assets = {"videos": [], "images": []}
    
    # Dynamic asset count for longer videos
    num_videos = max(6, int(dur / 5))  # ~5 sec per video
    num_images = max(4, int(dur / 6))  # ~6 sec per image
    
    log.info(f"Fetching {num_videos} premium videos + {num_images} premium images")
    
    # Generate premium search queries (multiple angles of same topic)
    search_strategies = [
        topic,
        f"{topic} history",
        f"{topic} revelation",
        f"{topic} mystery",
        f"{topic} truth",
        f"{topic} dark secret",
    ]
    
    # Fetch DIFFERENT videos
    video_count = 0
    for strategy in search_strategies:
        if video_count >= num_videos: break
        
        url = f"https://pixabay.com/api/videos/?key={PIXABAY_KEY}&q={strategy.replace(' ','+')}&per_page=5&order=trending"
        try:
            r = requests.get(url, timeout=30)
            videos = r.json().get("hits", [])
            
            for idx, hit in enumerate(videos[:4]):
                if video_count >= num_videos: break
                try:
                    vurl = hit["videos"]["medium"]["url"]
                    path = f"shorts_assets/videos/v{video_count}.mp4"
                    video_data = requests.get(vurl, timeout=60).content
                    open(path, "wb").write(video_data)
                    assets["videos"].append(path)
                    video_count += 1
                    log.info(f"  Video {video_count}/{num_videos}: {strategy[:30]}")
                except: pass
        except: pass
    
    # Fetch DIFFERENT images
    image_count = 0
    for strategy in search_strategies:
        if image_count >= num_images: break
        
        url = f"https://api.pexels.com/v1/search?query={strategy}&per_page=5&orientation=portrait"
        try:
            r = requests.get(url, headers={"Authorization": PEXELS_KEY}, timeout=30)
            photos = r.json().get("photos", [])
            
            for idx, photo in enumerate(photos[:3]):
                if image_count >= num_images: break
                try:
                    path = f"shorts_assets/images/img{image_count}.jpg"
                    img_data = requests.get(photo["src"]["large2x"], timeout=60).content
                    open(path, "wb").write(img_data)
                    assets["images"].append(path)
                    image_count += 1
                    log.info(f"  Image {image_count}/{num_images}: {strategy[:30]}")
                except: pass
        except: pass
    
    log.info(f"✅ Fetched {len(assets['videos'])} videos + {len(assets['images'])} images")
    return assets if (len(assets['videos']) >= 5 and len(assets['images']) >= 3) else None

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5 — PROFESSIONAL CAPTIONS RENDERING
# ═══════════════════════════════════════════════════════════════════════════
def render_caption(frame, caption_data, progress):
    """
    Render engaging captions with:
    - Dynamic color changes for emphasis
    - Text effects for retention
    - Strategic timing for hooks and twists
    """
    from PIL import Image, ImageDraw
    img = Image.fromarray(frame)
    draw = ImageDraw.Draw(img)
    
    text = caption_data["text"]
    is_hook = caption_data.get("is_hook", False)
    is_twist = caption_data.get("is_twist", False)
    color = caption_data.get("color", (212, 175, 55))
    
    font_size = HOOK_SIZE if is_hook else CAPTION_SIZE
    font = get_font(font_size)
    
    # Dynamic alpha for emphasis
    if is_hook:
        # Hook fades in quickly
        alpha = int(min(1.0, progress * 4.0) * 255)
    elif is_twist:
        # Twist has dramatic entrance
        alpha = int(min(1.0, progress * 5.0) * 255)
    else:
        # Normal fade in/out
        alpha = int(min(1.0, progress * 3.0) * 255) if progress < 0.33 else int(min(1.0, (1.0 - progress) * 3.0) * 255)
    
    alpha = max(0, min(255, alpha))
    
    # Text wrapping
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
    lines = lines[:4]  # Max 4 lines
    
    # Draw with shadow for professional look
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
        
        # Professional shadow effect
        for dx in range(-STROKE_W, STROKE_W + 1, 2):
            for dy in range(-STROKE_W, STROKE_W + 1, 2):
                if dx == 0 and dy == 0: continue
                o_draw.text((x_pos + dx, y_pos + dy), line, font=font, fill=(0, 0, 0, alpha))
        
        # Main text with dynamic color
        o_draw.text((x_pos, y_pos), line, font=font, fill=(*color, alpha))
    
    img = Image.alpha_composite(img.convert("RGBA"), overlay)
    return np.array(img.convert("RGB"))

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 6 — PREMIUM VIDEO ASSEMBLY (Professional Editing)
# ═══════════════════════════════════════════════════════════════════════════
def apply_premium_effects(frame, t, dur, intensity=1.0):
    """
    Apply professional cinematic effects:
    - Intelligent zoom with curve
    - Advanced color grading
    - Vignette with dynamic intensity
    - Subtle depth of field simulation
    """
    import cv2
    
    # Curved zoom (easing function for natural feel)
    progress = t / dur
    ease_progress = progress - math.sin(progress * 2 * math.pi) / (2 * math.pi)
    zoom_factor = 1.0 + (0.2 * ease_progress * intensity)
    
    h, w = frame.shape[:2]
    center_x, center_y = w // 2, h // 2
    
    zoom_matrix = cv2.getRotationMatrix2D((center_x, center_y), 0, zoom_factor)
    frame = cv2.warpAffine(frame, zoom_matrix, (w, h), borderMode=cv2.BORDER_REFLECT_101)
    
    # Advanced vignette with Gaussian blur
    kernel_x = cv2.getGaussianKernel(w, w * 0.4)
    kernel_y = cv2.getGaussianKernel(h, h * 0.4)
    kernel = kernel_y @ kernel_x.T
    mask = (kernel / kernel.max() * 255).astype(np.uint8)
    mask = np.dstack([mask] * 3)
    
    vignette = (frame.astype(float) * (mask.astype(float) / 255)).astype(np.uint8)
    frame = cv2.addWeighted(frame, 0.80, vignette, 0.20, 0)
    
    # Professional color grading (cinematic dark look)
    # Boost shadows, slightly cool tones
    frame[:, :, 0] = np.clip(frame[:, :, 0] * 1.15, 0, 255)  # Blue boost
    frame[:, :, 1] = np.clip(frame[:, :, 1] * 0.92, 0, 255)  # Green reduce
    frame[:, :, 2] = np.clip(frame[:, :, 2] * 0.85, 0, 255)  # Red reduce (cooler)
    
    # Subtle saturation boost for retention
    hsv = cv2.cvtColor(frame.astype(np.uint8), cv2.COLOR_BGR2HSV).astype(float)
    hsv[:, :, 1] = np.clip(hsv[:, :, 1] * 1.1, 0, 255)
    frame = cv2.cvtColor(hsv.astype(np.uint8), cv2.COLOR_HSV2BGR)
    
    return frame.astype(np.uint8)

def assemble_premium_short(assets, captions, dur, out_file):
    """
    Professional assembly with:
    - Intelligent clip transitions
    - Smart asset pacing
    - Retention-focused cuts
    """
    import cv2
    
    log.info("🎬 Assembling PREMIUM short with professional effects...")
    
    temp_video = "temp_video.mp4"
    writer = cv2.VideoWriter(temp_video, cv2.VideoWriter_fourcc(*"mp4v"), FPS, (SW, SH))
    
    total_frames = int(dur * FPS)
    videos = assets.get("videos", [])
    images = assets.get("images", [])
    
    # Smart pool: More videos than images for dynamic feel
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
    
    # Calculate smart pacing based on captions
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
            
            # Apply premium effects
            intensity = 1.2 if any(abs(t - cut) < 0.3 for cut in caption_cuts) else 1.0
            frame = apply_premium_effects(frame, t, dur, intensity)
            
            # Smooth transitions
            if path != last_path and transition_frame is not None:
                transition_progress = min(1.0, (frame_idx % transition_frames) / transition_frames)
                frame = cv2.addWeighted(transition_frame, 1.0 - transition_progress, frame, transition_progress, 0)
            
            transition_frame = frame.copy()
            last_path = path
            
            # Render captions
            for cap_data in captions:
                if cap_data["start"] <= t <= cap_data["end"]:
                    prog = (t - cap_data["start"]) / (cap_data["end"] - cap_data["start"]) if (cap_data["end"] - cap_data["start"]) > 0 else 0.5
                    frame = render_caption(frame, cap_data, prog)
                    break
            
            writer.write(frame)
            
            # Smart clip changes at caption breaks
            if any(abs(t - cut) < 0.1 for cut in caption_cuts):
                vi += 1
            elif frame_idx > 0 and frame_idx % int(FPS * random.uniform(4, 6)) == 0:
                vi += 1
                
        except Exception as e:
            log.warning(f"Frame {frame_idx}: {e}")
            writer.write(np.zeros((SH, SW, 3), dtype=np.uint8))
    
    writer.release()
    log.info(f"✅ Premium video assembled: {temp_video}")
    
    # Mix audio
    try:
        from moviepy.editor import VideoFileClip, AudioFileClip
        
        log.info("🎵 Mixing professional audio...")
        
        video = VideoFileClip(temp_video)
        audio = AudioFileClip("short_voice.mp3")
        
        final_video = video.set_audio(audio)
        
        final_video.write_videofile(
            out_file,
            codec='libx264',
            audio_codec='aac',
            verbose=False,
            logger=None
        )
        
        video.close()
        audio.close()
        final_video.close()
        
        if os.path.exists(temp_video):
            os.remove(temp_video)
        
        log.info(f"✅ PREMIUM SHORT COMPLETE: {out_file}")
        return os.path.exists(out_file)
        
    except Exception as e:
        log.error(f"❌ Audio mixing failed: {e}")
        if os.path.exists(temp_video):
            os.rename(temp_video, out_file)
            log.warning(f"⚠️  Using video without audio")
        return False

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 7 — VIRAL SEO OPTIMIZATION
# ═══════════════════════════════════════════════════════════════════════════
def generate_viral_seo(topic, script, hook):
    """
    Generate highly optimized SEO that drives viral reach:
    - Psychological triggers in title
    - Keyword-rich description
    - Strategic hashtags
    - Backend keywords for algorithm
    """
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)
        
        # Analyze content for viral hooks
        prompt = f"""You are a YouTube Shorts viral marketing expert.

Topic: {topic}
Hook: {hook}
Script: {script.get('full_script', '')[:300]}

Create VIRAL SEO package that will maximize reach and engagement:

Return ONLY valid JSON with NO explanations:
{{"title":"","description":"","primary_tags":"","hashtags":"","backend_keywords":""}}

RULES:
- title: 60-70 chars, include emotional trigger word, make people click/tap
- description: 150-180 chars, compelling, include 2-3 keywords, end with question or call-to-action
- primary_tags: 3-5 main tags separated by comma (most searchable keywords)
- hashtags: 15-20 hashtags for viral spread (mix viral+niche)
- backend_keywords: 20-30 keywords comma-separated for YouTube algorithm (hidden SEO)

Make it VIRAL focused, not generic. Use:
- Psychological triggers (mysterious, shocking, hidden, truth, etc.)
- Curiosity gaps (question formats)
- Emotional words (dark, revealed, haunting, etc.)
- Search volume keywords
- Trending words

Examples of VIRAL title format:
"[ADJECTIVE] [NOUN] They [VERB] [OBJECT]... *SHOCKING*"
"Wait Until You See What Happened To [SUBJECT]"
"This [NOUN] Will SHOCK YOU (Dark History)"

Examples of VIRAL description:
"We found [CLAIM] about [TOPIC]. This [ADJECTIVE] truth [VERB]... Watch till the end for the *SHOCKING* revelation. 🎥 #DarkHistory"
"""
        
        r = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.8,
            max_tokens=800
        )
        
        raw = r.choices[0].message.content.strip()
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if not match:
            log.warning("Could not parse SEO JSON, using defaults")
            return get_default_seo(topic, hook)
        
        seo = json.loads(match.group())
        
        log.info(f"✅ VIRAL SEO Generated:")
        log.info(f"   Title: {seo.get('title', '')[:60]}...")
        log.info(f"   Tags: {seo.get('primary_tags', '')}")
        
        return seo
        
    except Exception as e:
        log.warning(f"SEO generation failed: {e}, using defaults")
        return get_default_seo(topic, hook)

def get_default_seo(topic, hook):
    """Fallback professional SEO"""
    keywords = topic.split()[:2]
    return {
        "title": f"{hook} - {topic} | Dark History Revealed",
        "description": f"Discover the shocking truth about {topic}. What we found will SHOCK you. #DarkHistory #History #MustWatch",
        "primary_tags": f"{topic}, dark history, history facts",
        "hashtags": "#DarkHistory #HistoryRevealed #ShockingTruth #MustWatch #HistoryFacts #Conspiracy #Revealed #DarkSecrets #Historical #TruthUncovered #YouTube #Shorts #Viral #Mystery #Ancient",
        "backend_keywords": f"{topic}, dark history, history facts, shocking truth, hidden history, mysterious, revelation, ancient secrets, conspiracy, historical facts"
    }

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 8 — PROFESSIONAL THUMBNAIL
# ═══════════════════════════════════════════════════════════════════════════
def gen_premium_thumbnail(topic, hook):
    """Premium thumbnail with viral appeal"""
    from PIL import Image, ImageDraw
    
    img = Image.new("RGB", (1080, 1920), color=(12, 4, 8))
    d = ImageDraw.Draw(img)
    
    # Multiple text layers for dramatic effect
    font_hook = get_font(140)
    font_sub = get_font(60)
    
    # Main hook
    text = hook.upper()[:30]
    bb = d.textbbox((0, 0), text, font=font_hook)
    tw = bb[2] - bb[0]
    x = (1080 - tw) // 2
    
    # Outline effect
    for dx in range(-5, 6):
        for dy in range(-5, 6):
            if dx == 0 and dy == 0: continue
            d.text((x + dx, 800 + dy), text, fill=(0, 0, 0), font=font_hook)
    
    d.text((x, 800), text, fill=(255, 80, 80), font=font_hook)
    
    # Subtitle
    subtitle = "DARK HISTORY"
    d.text((100, 400), subtitle, fill=(212, 175, 55), font=font_sub)
    
    path = f"thumb_{int(time.time())}.jpg"
    img.save(path, quality=98)
    return path

# ═══════════════════════════════════════════════════════════════════════════
# AUTH & UPLOAD (unchanged from working version)
# ═══════════════════════════════════════════════════════════════════════════
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
                log.info(f"✅ Auth: {name}")
        except Exception as e:
            log.warning(f"Auth {name}: {e}")

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
    log.info(f"Slot {slot}: {t.strftime('%Y-%m-%d %H:%M')} UTC = {ist.strftime('%d %b %I:%M %p')} IST")
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

def upload(video_file, seo, thumbnail, offset=0):
    from googleapiclient.http import MediaFileUpload
    yt = get_yt()
    if not yt: return None, None
    pub = get_schedule(offset)
    
    description = f"""{seo['description']}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
{seo['hashtags']}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"""
    
    body = {
        "snippet": {
            "title": seo["title"],
            "description": description,
            "tags": seo.get("primary_tags", "").split(", "),
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
def get_related_topics(base, n=7):
    try:
        from groq import Groq
        client = Groq(api_key=GROQ_KEY)
        prompt = f"""Generate {n} VIRAL dark history topics related to: {base}
Each topic must be:
- Surprising/shocking
- Mysterious
- Lesser-known facts
- Engaging for YouTube Shorts

Return ONLY a JSON array of strings, no explanations.
Example: ["topic1","topic2",...]"""
        r = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.95,
            max_tokens=400
        )
        raw = r.choices[0].message.content.strip()
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if match:
            topics = json.loads(match.group())
            return topics[:n] if len(topics) >= n else topics + [base] * (n - len(topics))
    except Exception as e:
        log.warning(f"Topics failed: {e}")
    return [base] * n

# ═══════════════════════════════════════════════════════════════════════════
# MAIN RUN
# ═══════════════════════════════════════════════════════════════════════════
def run_premium_short(topic, num, offset=0):
    """Create one professional viral short"""
    log.info(f"\n{'='*60}\n🎬 PREMIUM SHORT #{num}: {topic}\n{'='*60}")
    
    # Step 1: Extended script (200-240 words)
    script = generate_extended_script(topic)
    if not script:
        log.error("❌ Script generation failed")
        return None
    
    # Step 2: Voice (45-50 seconds)
    if not generate_voice(script):
        log.error("❌ Voice generation failed")
        return None
    
    dur = get_voice_duration()
    if dur < 40 or dur > 55:
        log.warning(f"⚠️  Voice duration {dur:.1f}s is outside optimal range (40-55s)")
    
    # Step 3: Captions
    captions = build_captions(script, dur)
    
    # Step 4: Assets
    assets = fetch_premium_assets(topic, dur)
    if not assets:
        log.error("❌ No assets found")
        return None
    
    # Step 5: Assembly
    safe_topic = re.sub(r'[^\w\s]', '', topic).strip().replace(' ', '_')[:30]
    out_file = f"short_{num}_{safe_topic}.mp4"
    
    success = assemble_premium_short(assets, captions, dur, out_file)
    if not success or not os.path.exists(out_file):
        log.error("❌ Assembly failed")
        return None
    
    # Step 6: VIRAL SEO
    seo = generate_viral_seo(topic, script, script.get("hook", ""))
    
    # Step 7: Upload
    try:
        thumb = gen_premium_thumbnail(topic, script.get("hook", ""))
        _, url = upload(out_file, seo, thumb, offset)
        
        if url:
            update_sheet(topic, url, seo["title"], num)
            log.info(f"\n✅✅✅ SHORT #{num} VIRAL READY: {url}")
            log.info(f"   Duration: {dur:.1f}s | Title: {seo['title'][:60]}...")
            return url
    except Exception as e:
        log.error(f"❌ Upload failed: {e}")
    
    return None

def main():
    log.info("="*60)
    log.info("🚀 DARKHISTORYMIND: VIRAL SHORTS PIPELINE (PROFESSIONAL)")
    log.info("="*60)
    log.info("Duration: 45-50 seconds | SEO: VIRAL OPTIMIZED | Engagement: MAX")
    log.info("="*60)
    
    if not GROQ_KEY or not PEXELS_KEY:
        log.error("❌ Missing API Keys")
        sys.exit(1)

    download_fonts()
    setup_auth()
    
    if not os.path.exists("shorts_assets"):
        os.makedirs("shorts_assets")

    base_topic = LONG_VIDEO_TOPIC.strip() or "Darkest Secrets of History"
    log.info(f"\n📌 Base Topic: {base_topic}\n")

    topics = get_related_topics(base_topic, n=7)
    
    results = []
    for i, topic in enumerate(topics, 1):
        url = run_premium_short(topic, num=i, offset=i-1)
        results.append(url)
        
        if i < len(topics):
            log.info("⏳ Waiting 30s before next short...")
            time.sleep(30)

    log.info("\n" + "="*60)
    log.info("✅ ALL PREMIUM SHORTS PROCESSED")
    log.info("="*60)
    for i, url in enumerate(results, 1):
        status = "✅ VIRAL" if url else "❌ FAILED"
        log.info(f"Short #{i}: {status}")

if __name__ == "__main__":
    main()
