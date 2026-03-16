import requests
from bs4 import BeautifulSoup
import os
import subprocess
import sys
import time
import re
from datetime import datetime
import json
import urllib.parse

# --- Configuration (Default values, overridden by environment variables/workflow inputs) ---
DOODSTREAM_API_KEY = os.environ.get("DOODSTREAM_API_KEY", "")
HARDSUB_FOLDER_ID = os.environ.get("HARDSUB_FOLDER_ID", "thzdkzl93o") # Hardcoded as per request
SOFTSUB_FOLDER_ID = os.environ.get("SOFTSUB_FOLDER_ID", "5g3dhh9hmi") # Hardcoded as per request
BASE_EPISODE = int(os.environ.get("BASE_EPISODE", 1193))
BASE_DATE = os.environ.get("BASE_DATE", "2026-03-14") # The Saturday for episode 1193

# Workflow Inputs
EPISODE_INPUT = os.environ.get("EPISODE_INPUT", "") # e.g., "1100-1105", "1192", "1100,1103-1105"
MAGNET_LINKS_INPUT = os.environ.get("MAGNET_LINKS_INPUT", "") # Comma or newline separated magnet links
NYAA_USER = os.environ.get("NYAA_USER", "erai-raws") # Default to erai-raws as per initial request
SEARCH_QUERY_OVERRIDE = os.environ.get("SEARCH_QUERY_OVERRIDE", "") # Custom search term for Nyaa
SELECT_FILES = os.environ.get("SELECT_FILES", "") # e.g., "1,3,5" indices from aria2c
FORCE_MOVIE_MODE = os.environ.get("FORCE_MOVIE_MODE", "0").lower() == "1"

# --- Constants ---
NYAA_BASE_URL = "https://nyaa.si"
DOODSTREAM_API_BASE = "https://doodapi.co/api"

# --- Helper Functions ---

def parse_episode_input(episode_input_str):
    episodes = set()
    if not episode_input_str: # If empty, calculate latest
        base_dt = datetime.strptime(BASE_DATE, "%Y-%m-%d")
        now = datetime.now()
        weeks_passed = (now - base_dt).days // 7
        episodes.add(BASE_EPISODE + max(0, weeks_passed))
        return sorted(list(episodes))

    parts = episode_input_str.split(",")
    for part in parts:
        part = part.strip()
        if "-" in part:
            start, end = map(int, part.split("-"))
            episodes.update(range(start, end + 1))
        elif part.isdigit():
            episodes.add(int(part))
    return sorted(list(episodes))

def escape_ffmpeg_path(path):
    # ffmpeg subtitle filter requires special escaping for brackets and other characters
    # 1. Backslashes must be doubled
    # 2. Single quotes must be escaped
    # 3. Colons must be escaped
    # 4. Brackets must be escaped
    p = path.replace("\\", "\\\\")
    p = p.replace("\"", "\\\"") # Escape double quotes
    p = p.replace("\"", "\\\"") # Escape single quotes
    p = p.replace(":", "\\:")
    p = p.replace("[", "\\\\[").replace("]", "\\\\]")
    return p

def get_doodstream_server_url():
    try:
        for _ in range(3): # Retry 3 times
            response = requests.get(f"{DOODSTREAM_API_BASE}/upload/server?key={DOODSTREAM_API_KEY}", timeout=10)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == 200:
                return data["result"]
            print(f"DoodStream server URL error: {data.get("msg", "Unknown error")}")
            time.sleep(5)
    except Exception as e:
        print(f"Failed to get DoodStream server URL: {e}")
    return None

def doodstream_api_call(endpoint, params=None, files=None, method="GET"):
    url = f"{DOODSTREAM_API_BASE}{endpoint}"
    try:
        if method == "POST":
            response = requests.post(url, params=params, files=files, timeout=60)
        else:
            response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"DoodStream API call to {endpoint} failed: {e}")
        return None

def verify_doodstream_account():
    print("Verifying DoodStream account...")
    params = {"key": DOODSTREAM_API_KEY}
    response = doodstream_api_call("/account/info", params=params)
    if response and response.get("status") == 200:
        print("DoodStream account verified.")
        return True
    print("DoodStream account verification failed.")
    return False

# --- Nyaa Search Functions ---

def search_nyaa(query, user, preferred_quality="1080p", language="English-translated"):
    print(f"Searching Nyaa for \'{query}\' by user \'{user}\'...")
    search_url = f"{NYAA_BASE_URL}/user/{user}?f=0&c=0_0&q={urllib.parse.quote(query)}"
    
    try:
        response = requests.get(search_url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Prioritize 1080p and English
        rows = soup.select("tr.success, tr.default")
        
        # Filter by language and quality
        filtered_rows = []
        for row in rows:
            category_link = row.find("a", class_="torrent-category")
            if category_link and language in category_link.get_text():
                title_link = row.find("a", title=True)
                if title_link and preferred_quality in title_link["title"]:
                    filtered_rows.insert(0, row) # Prioritize 1080p
                else:
                    filtered_rows.append(row)
        
        for row in filtered_rows:
            links = row.find_all("a", href=True)
            for link in links:
                if link["href"].startswith("magnet:"):
                    title = row.find("a", title=True)["title"]
                    print(f"Found: {title}")
                    return link["href"], title
    except Exception as e:
        print(f"Nyaa search failed: {e}")
    return None, None

# --- Aria2c Download Functions ---

def get_aria2c_file_list(magnet_link):
    print("Getting file list from magnet link...")
    # Use --bt-metadata-only to get torrent info without downloading data
    # Then --show-files to list them
    try:
        # First, get metadata only to ensure aria2c has enough info to list files
        subprocess.run(["aria2c", "--bt-metadata-only=true", "--bt-save-metadata=true",
                        "--seed-time=0", magnet_link], check=True, capture_output=True, text=True)
        
        # Now list files
        result = subprocess.run(["aria2c", "--show-files", magnet_link], check=True, capture_output=True, text=True)
        print("--- Torrent File List ---")
        print(result.stdout)
        print("-------------------------")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Failed to get aria2c file list: {e.stderr}")
        return None

def download_with_aria2c(magnet_link, select_files_indices=None):
    print(f"Downloading with aria2c. Selected files: {select_files_indices if select_files_indices else 'All'}")
    cmd = ["aria2c", "--seed-time=0", "--bt-enable-dht=true", "--enable-peer-exchange=true", "--enable-lpd=true"]
    if select_files_indices:
        cmd.append(f"--select-file={select_files_indices}")
    cmd.append(magnet_link)
    
    try:
        subprocess.run(cmd, check=True)
        # Find downloaded MKV files
        downloaded_files = [f for f in os.listdir('.') if f.endswith('.mkv') or f.endswith('.mp4')]
        return downloaded_files
    except subprocess.CalledProcessError as e:
        print(f"Aria2c download failed: {e.stderr}")
        return []

# --- FFmpeg Processing Functions ---

def get_subtitle_track_index(video_file):
    print(f"Probing {video_file} for English subtitle tracks...")
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "s", "-show_entries", 
             "stream=index:stream_tags=language", "-of", "json", video_file
            ],
            capture_output=True, text=True, check=True
        )
        streams_info = json.loads(result.stdout)
        
        if "streams" in streams_info:
            for stream in streams_info["streams"]:
                if stream.get("tags", {}).get("language", "").lower() == "eng":
                    print(f"Found English subtitle track at index: {stream["index"]}")
                    return stream["index"]
        print("No English subtitle track found.")
        return None
    except Exception as e:
        print(f"Error probing for subtitles: {e}")
        return None

def process_video_file(input_file, base_name, is_movie=False):
    output_hs = f"{base_name}_HS.mp4"
    output_ss = f"{base_name}_SS.mp4"
    
    # Attempt to get English subtitle track index
    subtitle_index = get_subtitle_track_index(input_file)
    
    # --- Soft-sub (SS) processing ---
    print(f"Processing Soft-sub version for {input_file}...")
    try:
        # Stream copy, faststart, drop ASS subtitles if present
        cmd_ss = [
            "ffmpeg", "-y", "-i", input_file, 
            "-c", "copy", "-movflags", "faststart",
            "-map", "0:v", "-map", "0:a", 
            # Drop subtitle streams for SS version if they are ASS (to avoid burning issues)
            # This is a bit tricky with map, better to just copy video/audio and let ffprobe handle it
            output_ss
        ]
        subprocess.run(cmd_ss, check=True, capture_output=True, text=True)
        print(f"Soft-sub created: {output_ss}")
    except subprocess.CalledProcessError as e:
        print(f"Soft-sub remux failed (stream copy): {e.stderr}")
        # Fallback 1: Audio re-encode
        try:
            print("Retrying SS with audio re-encode...")
            cmd_ss_fallback1 = [
                "ffmpeg", "-y", "-i", input_file, 
                "-c:v", "copy", "-c:a", "aac", "-b:a", "192k", "-movflags", "faststart",
                output_ss
            ]
            subprocess.run(cmd_ss_fallback1, check=True, capture_output=True, text=True)
            print(f"Soft-sub created (audio re-encode): {output_ss}")
        except subprocess.CalledProcessError as e2:
            print(f"Soft-sub remux failed (audio re-encode): {e2.stderr}")
            # Fallback 2: Full re-encode
            try:
                print("Retrying SS with full re-encode...")
                cmd_ss_fallback2 = [
                    "ffmpeg", "-y", "-i", input_file, 
                    "-c:v", "libx264", "-preset", "veryfast", "-crf", "22", 
                    "-c:a", "aac", "-b:a", "192k", "-movflags", "faststart",
                    output_ss
                ]
                subprocess.run(cmd_ss_fallback2, check=True, capture_output=True, text=True)
                print(f"Soft-sub created (full re-encode): {output_ss}")
            except subprocess.CalledProcessError as e3:
                print(f"Soft-sub creation failed completely: {e3.stderr}")
                output_ss = None
    
    # --- Hard-sub (HS) processing ---
    print(f"Processing Hard-sub version for {input_file}...")
    if subtitle_index is None:
        print("No English subtitle track found for hard-subbing. Skipping HS.")
        output_hs = None
    else:
        try:
            escaped_input_file = escape_ffmpeg_path(input_file)
            # Attempt 1: Using si (stream index) for subtitles
            cmd_hs = [
                "ffmpeg", "-y", "-i", input_file, 
                "-vf", f"subtitles=\'{escaped_input_file}\':si={subtitle_index}", 
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "22", 
                "-c:a", "copy", 
                output_hs
            ]
            subprocess.run(cmd_hs, check=True, capture_output=True, text=True)
            print(f"Hard-sub created: {output_hs}")
        except subprocess.CalledProcessError as e:
            print(f"Hard-sub failed (attempt 1): {e.stderr}")
            # Attempt 2: Without si (let ffmpeg guess)
            try:
                print("Retrying HS without subtitle index...")
                cmd_hs_fallback1 = [
                    "ffmpeg", "-y", "-i", input_file, 
                    "-vf", f"subtitles=\'{escaped_input_file}\'", 
                    "-c:v", "libx264", "-preset", "veryfast", "-crf", "22", 
                    "-c:a", "copy", 
                    output_hs
                ]
                subprocess.run(cmd_hs_fallback1, check=True, capture_output=True, text=True)
                print(f"Hard-sub created (fallback 1): {output_hs}")
            except subprocess.CalledProcessError as e2:
                print(f"Hard-sub failed (attempt 2): {e2.stderr}")
                # Attempt 3: With external subtitle file (if any)
                # This part would require external subtitle detection and matching
                # For now, we'll just log and fail if no embedded subs work
                print("Hard-sub creation failed completely after multiple attempts.")
                output_hs = None
                
    return output_hs, output_ss

# --- DoodStream Upload Functions ---

def upload_file_to_doodstream(file_path, folder_id, title):
    print(f"Uploading \'{file_path}\' to DoodStream folder \'{folder_id}\' with title \'{title}\'...")
    upload_server_url = get_doodstream_server_url()
    if not upload_server_url:
        return None

    for attempt in range(3): # Retry 3 times
        try:
            with open(file_path, "rb") as f:
                files = {"file": (os.path.basename(file_path), f, "video/mp4")}
                data = {"api_key": DOODSTREAM_API_KEY}
                if folder_id: data["fld_id"] = folder_id
                
                # DoodStream API expects api_key in form data AND as query param
                response = requests.post(f"{upload_server_url}?{DOODSTREAM_API_KEY}", files=files, data=data, timeout=3600)
                response.raise_for_status()
                
                # DoodStream can return non-JSON HTML for errors, so check content type
                if "application/json" in response.headers.get("Content-Type", ""):
                    upload_data = response.json()
                else:
                    # Attempt to parse 'st' field from HTML if it's an error page
                    match = re.search(r"name=\"st\" value=\"(.*?)\"", response.text)
                    if match: 
                        print(f"DoodStream HTML error: {match.group(1)}")
                        continue # Retry
                    print(f"DoodStream unknown response: {response.text[:200]}")
                    continue # Retry

                if upload_data.get("status") == 200:
                    file_code = upload_data["result"][0]["filecode"]
                    print(f"Upload successful! Filecode: {file_code}")
                    
                    # Rename file
                    rename_params = {"key": DOODSTREAM_API_KEY, "file_code": file_code, "title": title}
                    rename_resp = doodstream_api_call("/file/rename", params=rename_params)
                    if rename_resp and rename_resp.get("status") == 200:
                        print(f"File renamed to: {title}")
                    else:
                        print(f"Failed to rename file: {rename_resp}")
                    
                    # Move to folder (if folder_id provided)
                    if folder_id:
                        move_params = {"key": DOODSTREAM_API_KEY, "file_code": file_code, "fld_id": folder_id}
                        move_resp = doodstream_api_call("/file/move", params=move_params)
                        if move_resp and move_resp.get("status") == 200:
                            print(f"File moved to folder: {folder_id}")
                        else:
                            print(f"Failed to move file to folder: {move_resp}")

                    return upload_data["result"][0]["download_url"]
                else:
                    print(f"DoodStream upload failed: {upload_data.get("msg", "Unknown error")}")
        except Exception as e:
            print(f"DoodStream upload attempt {attempt+1} failed: {e}")
        time.sleep(10) # Wait before retrying
    return None

# --- Main Logic ---

def main():
    if not verify_doodstream_account():
        print("Exiting due to DoodStream account verification failure.")
        sys.exit(1)

    episodes_to_process = parse_episode_input(EPISODE_INPUT)
    magnet_links_list = [m.strip() for m in MAGNET_LINKS_INPUT.replace("\n", ",").split(",") if m.strip()]

    # Process explicit magnet links first
    for magnet_link in magnet_links_list:
        print(f"Processing explicit magnet link: {magnet_link}")
        # For explicit magnets, we don't have an episode number easily, so we'll use a generic name
        # User will need to manually rename or provide better context
        base_name = "Unknown_Content"
        
        if SELECT_FILES:
            get_aria2c_file_list(magnet_link) # Show user the files for selection
            downloaded_files = download_with_aria2c(magnet_link, SELECT_FILES)
        else:
            downloaded_files = download_with_aria2c(magnet_link)

        for downloaded_file in downloaded_files:
            # Try to infer if it's a movie/OVA from filename
            if FORCE_MOVIE_MODE or re.search(r"movie|ova|film", downloaded_file, re.IGNORECASE):
                title_template_hs = "Detective Conan Movie - {num} HS"
                title_template_ss = "Detective Conan Movie - {num} SS"
                # Extract movie number if possible
                movie_num_match = re.search(r"(?:movie|ova|film)[^\d]*(\d+)", downloaded_file, re.IGNORECASE)
                num_placeholder = movie_num_match.group(1) if movie_num_match else "Unknown"
            else:
                title_template_hs = "Detective Conan - {ep} HS"
                title_template_ss = "Detective Conan - {ep} SS"
                ep_num_match = re.search(r"-(\s*)(\d+)", downloaded_file)
                num_placeholder = ep_num_match.group(2) if ep_num_match else "Unknown"

            final_base_name = downloaded_file.replace(".mkv", "").replace(".mp4", "")
            hs_output, ss_output = process_video_file(downloaded_file, final_base_name)

            if hs_output:
                upload_title_hs = title_template_hs.format(ep=num_placeholder, num=num_placeholder)
                upload_file_to_doodstream(hs_output, HARDSUB_FOLDER_ID, upload_title_hs)
                os.remove(hs_output)
            if ss_output:
                upload_title_ss = title_template_ss.format(ep=num_placeholder, num=num_placeholder)
                upload_file_to_doodstream(ss_output, SOFTSUB_FOLDER_ID, upload_title_ss)
                os.remove(ss_output)
            os.remove(downloaded_file) # Clean up original downloaded file

    # Process episodes from Nyaa.si
    for ep_num in episodes_to_process:
        print(f"\n--- Processing Episode {ep_num} ---")
        search_term = SEARCH_QUERY_OVERRIDE if SEARCH_QUERY_OVERRIDE else f"Detective Conan - {ep_num}"
        
        magnet = None
        nyaa_title = None
        # Retry loop for 7am-9am window (approx 2 hours) - 24 * 5 mins = 120 mins
        for attempt in range(24):
            magnet, nyaa_title = search_nyaa(search_term, NYAA_USER)
            if magnet: break
            print(f"Episode {ep_num} not found. Attempt {attempt+1}/24. Waiting 5 minutes...")
            time.sleep(300)
        
        if not magnet:
            print(f"Skipping Episode {ep_num} (not found after retries).")
            continue
        
        # If SELECT_FILES is provided, show file list and download selectively
        if SELECT_FILES:
            get_aria2c_file_list(magnet) # This will print the file list for user reference
            downloaded_files = download_with_aria2c(magnet, SELECT_FILES)
        else:
            downloaded_files = download_with_aria2c(magnet)

        for downloaded_file in downloaded_files:
            # Determine if it's an episode or movie based on title or FORCE_MOVIE_MODE
            is_movie = FORCE_MOVIE_MODE or ("movie" in nyaa_title.lower() or "ova" in nyaa_title.lower() or "film" in nyaa_title.lower())
            
            if is_movie:
                title_template_hs = "Detective Conan Movie - {num} HS"
                title_template_ss = "Detective Conan Movie - {num} SS"
                # Try to extract movie number from Nyaa title
                num_match = re.search(r"(?:movie|ova|film)[^\d]*(\d+)", nyaa_title, re.IGNORECASE)
                num_placeholder = num_match.group(1) if num_match else str(ep_num) # Fallback to ep_num
            else:
                title_template_hs = "Detective Conan - {ep} HS"
                title_template_ss = "Detective Conan - {ep} SS"
                num_placeholder = str(ep_num)

            # Process video (hard-sub and soft-sub)
            hs_output, ss_output = process_video_file(downloaded_file, f"DC_{num_placeholder}", is_movie)

            # Upload to DoodStream
            if hs_output:
                upload_title_hs = title_template_hs.format(ep=num_placeholder, num=num_placeholder)
                upload_file_to_doodstream(hs_output, HARDSUB_FOLDER_ID, upload_title_hs)
                os.remove(hs_output)
            if ss_output:
                upload_title_ss = title_template_ss.format(ep=num_placeholder, num=num_placeholder)
                upload_file_to_doodstream(ss_output, SOFTSUB_FOLDER_ID, upload_title_ss)
                os.remove(ss_output)
            
            # Clean up original downloaded file
            if os.path.exists(downloaded_file): os.remove(downloaded_file)

    print("\nAutomation run complete.")

if __name__ == "__main__":
    # Install ffprobe if not present (common in GitHub Actions runners)
    try:
        subprocess.run(["ffprobe", "-version"], check=True, capture_output=True)
    except FileNotFoundError:
        print("ffprobe not found, installing ffmpeg...")
        subprocess.run(["sudo", "apt-get", "update"], check=True)
        subprocess.run(["sudo", "apt-get", "install", "-y", "ffmpeg"], check=True)
    
    main()
