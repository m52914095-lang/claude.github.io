import requests
from bs4 import BeautifulSoup
import os
import subprocess
import sys
import time
import re
from datetime import datetime

# Configuration (Default values, overridden by environment variables)
DOODSTREAM_API_KEY = os.environ.get('DOODSTREAM_API_KEY', '554366xrjxeza9m7e4m02v')
HARDSUB_FOLDER_ID = os.environ.get('HARDSUB_FOLDER_ID', '')
SOFTSUB_FOLDER_ID = os.environ.get('SOFTSUB_FOLDER_ID', '')
BASE_EPISODE = int(os.environ.get('BASE_EPISODE', 1193))
BASE_DATE = os.environ.get('BASE_DATE', '2026-03-14')
EPISODE_RANGE = os.environ.get('EPISODE_RANGE', '') # e.g., "1192-1194"
NYAA_USER = os.environ.get('NYAA_USER', 'subsplease') # erai-raws, subsplease, etc.
SEARCH_QUERY = os.environ.get('SEARCH_QUERY', 'Detective Conan') # Custom search term
SELECT_FILES = os.environ.get('SELECT_FILES', '') # e.g., "1,3,5" indices from aria2c

def get_episodes_to_process():
    if EPISODE_RANGE:
        if '-' in EPISODE_RANGE:
            start, end = EPISODE_RANGE.split('-')
            return list(range(int(start), int(end) + 1))
        return [int(EPISODE_RANGE)]
    
    # Default: Calculate next episode based on date
    base_dt = datetime.strptime(BASE_DATE, '%Y-%m-%d')
    now = datetime.now()
    weeks_passed = (now - base_dt).days // 7
    return [BASE_EPISODE + max(0, weeks_passed)]

def find_magnet(query, user):
    url = f'https://nyaa.si/user/{user}?f=0&c=0_0&q={requests.utils.quote(query)}'
    print(f"Searching Nyaa: {url}")
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        for row in soup.select('tr.success, tr.default'):
            links = row.find_all('a', href=True)
            for link in links:
                if link['href'].startswith('magnet:'):
                    return link['href']
    except Exception as e:
        print(f"Nyaa search error: {e}")
    return None

def get_torrent_files(magnet):
    print("Fetching torrent file list...")
    # First, get metadata
    subprocess.run(['aria2c', '--bt-metadata-only=true', '--bt-save-metadata=true', '--seed-time=0', magnet], capture_output=True)
    # Metadata is usually saved as [info_hash].torrent
    # For simplicity, we'll use --show-files directly on the magnet
    result = subprocess.run(['aria2c', '--show-files', magnet], capture_output=True, text=True)
    return result.stdout

def download_files(magnet, indices=None):
    print(f"Downloading files (indices: {indices if indices else 'all'})...")
    cmd = ['aria2c', '--seed-time=0']
    if indices:
        cmd.append(f'--select-file={indices}')
    cmd.append(magnet)
    subprocess.run(cmd, check=True)
    
    # Find downloaded .mkv files
    return [f for f in os.listdir('.') if f.endswith('.mkv')]

def escape_ffmpeg_path(path):
    return path.replace('\\', '\\\\').replace("'", "\\'").replace(':', '\\:').replace('[', '\\[').replace(']', '\\]')

def process_video(input_file, episode_num):
    # Hard-sub version
    hs_title = f"Detective Conan Episode {episode_num} HS"
    hs_output = f"{hs_title}.mp4"
    print(f"Creating Hard-sub: {hs_output}")
    escaped_input = escape_ffmpeg_path(input_file)
    cmd_hs = ['ffmpeg', '-y', '-i', input_file, '-vf', f"subtitles='{escaped_input}'", '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '22', '-c:a', 'copy', hs_output]
    subprocess.run(cmd_hs, capture_output=True, check=True)
    
    # Soft-sub version (just remux/rename)
    ss_title = f"Detective Conan Episode {episode_num} SS"
    ss_output = f"{ss_title}.mp4"
    print(f"Creating Soft-sub: {ss_output}")
    cmd_ss = ['ffmpeg', '-y', '-i', input_file, '-c', 'copy', ss_output]
    subprocess.run(cmd_ss, capture_output=True, check=True)
    
    return hs_output, ss_output

def upload_to_dood(file_path, folder_id):
    print(f"Uploading {file_path} to DoodStream (Folder: {folder_id})...")
    try:
        server_url = requests.get(f'https://doodapi.co/api/upload/server?key={DOODSTREAM_API_KEY}').json()['result']
        params = {'api_key': DOODSTREAM_API_KEY}
        if folder_id: params['fld_id'] = folder_id
        
        with open(file_path, 'rb') as f:
            resp = requests.post(f"{server_url}?{DOODSTREAM_API_KEY}", files={'file': f}, data=params).json()
        if resp.get('status') == 200:
            return resp['result'][0]['download_url']
    except Exception as e:
        print(f"Upload error: {e}")
    return None

def main():
    episodes = get_episodes_to_process()
    print(f"Processing Episodes: {episodes}")
    
    for ep in episodes:
        query = f"{SEARCH_QUERY} {ep}"
        magnet = None
        # Retry loop for 7am-9am window (approx 2 hours)
        for attempt in range(24): # 24 * 5 mins = 120 mins
            magnet = find_magnet(query, NYAA_USER)
            if magnet: break
            print(f"Episode {ep} not found. Attempt {attempt+1}/24. Waiting 5 mins...")
            time.sleep(300)
        
        if not magnet:
            print(f"Skipping Episode {ep} (not found).")
            continue
        
        if SELECT_FILES:
            print("Selective download requested. File list:")
            print(get_torrent_files(magnet))
            # In automated mode, we use the SELECT_FILES indices
        
        mkv_files = download_files(magnet, SELECT_FILES)
        for mkv in mkv_files:
            try:
                hs_file, ss_file = process_video(mkv, ep)
                
                hs_link = upload_to_dood(hs_file, HARDSUB_FOLDER_ID)
                ss_link = upload_to_dood(ss_file, SOFTSUB_FOLDER_ID)
                
                print(f"Episode {ep} Complete!")
                print(f"HS: {hs_link}")
                print(f"SS: {ss_link}")
                
                # Cleanup
                os.remove(mkv)
                os.remove(hs_file)
                os.remove(ss_file)
            except Exception as e:
                print(f"Error processing {mkv}: {e}")

if __name__ == "__main__":
    main()
