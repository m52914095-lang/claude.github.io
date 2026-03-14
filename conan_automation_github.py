"""
conan_automation_github.py — Detective Conan automated downloader + uploader

Bulk-safe fixes (handles 30+ videos without failing):
  • NEVER calls sys.exit(1) mid-loop — failed episodes are logged and skipped
  • Git commit/push happens ONCE at the very end, not after every episode
  • Each HS output uses a unique filename (no overwrites during batch runs)
  • Upload server URL is fetched once and reused for the whole run
  • Upload retries up to 3 times before giving up on a file
  • git pull --rebase before pushing to avoid push rejections

Features:
  • Auto-calculates episode from BASE_DATE / BASE_EPISODE
  • Searches Nyaa.si (SubsPlease default, or custom uploader)
  • MAGNET_LINKS env var = batch magnets (newline or comma-separated)
  • Downloads via aria2c — no seeding
  • SS upload: original .mkv → DoodStream (SOFT_SUB_FOLDER_ID)
  • HS upload: ffmpeg hard-subbed .mp4 → DoodStream (HARD_SUB_FOLDER_ID)
  • Patches index.html and does a single git commit + push at the end
"""

import os
import re
import sys
import glob
import time
import subprocess
from datetime import datetime

import requests
from bs4 import BeautifulSoup

from conan_utils import xor_encrypt
from update import patch_hs, patch_ss, read_html, write_html

# ── Config ────────────────────────────────────────────────────────────────────
DOODSTREAM_API_KEY  = os.environ.get("DOODSTREAM_API_KEY", "554366xrjxeza9m7e4m02v")
HARD_SUB_FOLDER_ID  = os.environ.get("HARD_SUB_FOLDER_ID", "")
SOFT_SUB_FOLDER_ID  = os.environ.get("SOFT_SUB_FOLDER_ID", "")

BASE_EPISODE        = int(os.environ.get("BASE_EPISODE", "1193"))
BASE_DATE           = os.environ.get("BASE_DATE", "2026-03-14")

EPISODE_OVERRIDE    = os.environ.get("EPISODE_OVERRIDE",   "").strip()
MAGNET_LINKS        = os.environ.get("MAGNET_LINKS",        "").strip()
CUSTOM_SEARCH       = os.environ.get("CUSTOM_SEARCH",       "").strip()
NYAA_UPLOADER_URL   = os.environ.get("NYAA_UPLOADER_URL",   "").strip()

HS_TITLE_TPL        = os.environ.get("HS_TITLE_TPL", "Detective Conan - {ep} HS")
SS_TITLE_TPL        = os.environ.get("SS_TITLE_TPL", "Detective Conan - {ep} SS")

HTML_FILE           = os.environ.get("HTML_FILE", "index.html")

UPLOAD_RETRIES      = 3
RETRY_DELAY         = 10

_upload_server_url: str | None = None

# ── Episode helpers ───────────────────────────────────────────────────────────

def get_expected_episode() -> int:
    if EPISODE_OVERRIDE and EPISODE_OVERRIDE.isdigit():
        return int(EPISODE_OVERRIDE)
    base_dt = datetime.strptime(BASE_DATE, "%Y-%m-%d")
    weeks = max(0, (datetime.now() - base_dt).days // 7)
    return BASE_EPISODE + weeks


def parse_episode_from_filename(filename: str) -> int | None:
    m = re.search(r"Detective Conan\s*[-\u2013]\s*(\d{3,4})\b", filename, re.IGNORECASE)
    if m:
        return int(m.group(1))
    m = re.search(r"\b(\d{3,4})\b", os.path.basename(filename))
    if m:
        return int(m.group(1))
    return None


# ── Nyaa search ───────────────────────────────────────────────────────────────

def search_nyaa(episode: int) -> str | None:
    query = CUSTOM_SEARCH if CUSTOM_SEARCH else f"Detective Conan - {episode} 1080p"
    base  = NYAA_UPLOADER_URL.rstrip("/") if NYAA_UPLOADER_URL else "https://nyaa.si"
    url   = f"{base}?f=0&c=1_2&q={requests.utils.quote(query)}"
    print(f"  Searching Nyaa: {url}")

    try:
        soup = BeautifulSoup(requests.get(url, timeout=30).text, "html.parser")
    except Exception as e:
        print(f"  Nyaa search error: {e}", file=sys.stderr)
        return None

    for row in soup.select("tr.success, tr.default"):
        title_cell = row.find("td", {"colspan": "2"}) or row.find("a", title=True)
        title_text = title_cell.get_text() if title_cell else ""
        if "1080p" not in title_text:
            continue
        for link in row.find_all("a", href=True):
            if link["href"].startswith("magnet:"):
                return link["href"]

    for row in soup.select("tr.success, tr.default"):
        for link in row.find_all("a", href=True):
            if link["href"].startswith("magnet:"):
                return link["href"]

    return None


# ── Download ──────────────────────────────────────────────────────────────────

def download_magnet(magnet: str) -> list:
    before = set(glob.glob("**/*.mkv", recursive=True))
    print(f"  Downloading: {magnet[:90]}...")

    cmd = [
        "aria2c",
        "--seed-time=0",
        "--max-connection-per-server=4",
        "--split=4",
        "--file-allocation=none",
        "--bt-stop-timeout=300",
        magnet,
    ]
    try:
        subprocess.run(cmd, check=True, timeout=7200)
    except subprocess.TimeoutExpired:
        print("  aria2c timeout — checking for partial files", file=sys.stderr)
    except subprocess.CalledProcessError as e:
        print(f"  aria2c error: {e}", file=sys.stderr)

    after = set(glob.glob("**/*.mkv", recursive=True))
    new   = sorted(after - before, key=os.path.getmtime)
    print(f"  New .mkv files: {new or 'none'}")
    return new


# ── ffmpeg hard-sub ───────────────────────────────────────────────────────────

def _esc(path: str) -> str:
    p = path.replace("\\", "\\\\").replace("'", "\\'")
    p = p.replace(":", "\\:").replace("[", "\\[").replace("]", "\\]")
    return p


def hardsub(input_file: str, ep: int) -> str | None:
    output = f"conan_{ep}_hs.mp4"   # unique per episode — no overwrites
    print(f"  Hard-subbing -> {output}")

    for vf in [f"subtitles='{_esc(input_file)}'", f"subtitles={_esc(input_file)}"]:
        cmd = [
            "ffmpeg", "-y", "-i", input_file,
            "-vf", vf,
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "22",
            "-c:a", "aac", "-b:a", "192k",
            output,
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=7200)
            print(f"  Hard-sub complete: {output}")
            return output
        except subprocess.CalledProcessError as e:
            print(f"  ffmpeg attempt failed:\n{e.stderr[-800:]}", file=sys.stderr)

    print(f"  Hard-sub FAILED for episode {ep}", file=sys.stderr)
    return None


# ── DoodStream upload ─────────────────────────────────────────────────────────

def get_upload_server() -> str | None:
    global _upload_server_url
    if _upload_server_url:
        return _upload_server_url
    try:
        resp = requests.get(
            "https://doodapi.co/api/upload/server",
            params={"key": DOODSTREAM_API_KEY},
            timeout=20,
        ).json()
        if resp.get("status") == 200:
            _upload_server_url = resp["result"]
            return _upload_server_url
    except Exception as e:
        print(f"  DoodStream server lookup error: {e}", file=sys.stderr)
    return None


def upload_file(file_path: str, title: str, folder_id: str = "") -> str | None:
    size_mb = os.path.getsize(file_path) // (1024 * 1024)
    print(f"  Uploading '{title}' ({size_mb} MB)...")

    for attempt in range(1, UPLOAD_RETRIES + 1):
        global _upload_server_url
        _upload_server_url = None          # refresh server URL on every attempt
        server = get_upload_server()
        if not server:
            print(f"  [attempt {attempt}] No upload server", file=sys.stderr)
            time.sleep(RETRY_DELAY)
            continue

        try:
            with open(file_path, "rb") as fh:
                data = {"api_key": DOODSTREAM_API_KEY, "title": title}
                if folder_id:
                    data["fld_id"] = folder_id
                resp = requests.post(
                    server,
                    files={"file": (os.path.basename(file_path), fh)},
                    data=data,
                    timeout=7200,
                ).json()

            if resp.get("status") == 200:
                result = resp["result"][0]
                url = result.get("download_url") or result.get("embed_url") or ""
                print(f"  Uploaded! {url}")
                return url
            else:
                print(f"  [attempt {attempt}] Bad response: {resp}", file=sys.stderr)

        except Exception as e:
            print(f"  [attempt {attempt}] Exception: {e}", file=sys.stderr)

        if attempt < UPLOAD_RETRIES:
            print(f"  Retrying in {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)

    print(f"  All {UPLOAD_RETRIES} attempts failed for '{title}'", file=sys.stderr)
    return None


# ── Per-episode processing ────────────────────────────────────────────────────

def process_episode(mkv_file: str):
    """Process one .mkv. Returns (ep, hs_url, ss_url). Never raises."""
    ep = parse_episode_from_filename(mkv_file)
    if ep is None:
        ep = get_expected_episode()
        print(f"  Could not parse episode — using calculated: {ep}")
    else:
        print(f"\n-- Episode {ep} ({os.path.basename(mkv_file)}) --")

    hs_url = None
    ss_url = None

    try:
        ss_url = upload_file(mkv_file, SS_TITLE_TPL.format(ep=ep), SOFT_SUB_FOLDER_ID)
    except Exception as e:
        print(f"  SS upload exception: {e}", file=sys.stderr)

    hs_file = None
    try:
        hs_file = hardsub(mkv_file, ep)
        if hs_file:
            hs_url = upload_file(hs_file, HS_TITLE_TPL.format(ep=ep), HARD_SUB_FOLDER_ID)
    except Exception as e:
        print(f"  HS processing exception: {e}", file=sys.stderr)
    finally:
        if hs_file and os.path.exists(hs_file):
            try:
                os.remove(hs_file)
            except OSError:
                pass

    try:
        os.remove(mkv_file)
    except OSError:
        pass

    return ep, hs_url, ss_url


# ── Batch HTML patch + single git push ───────────────────────────────────────

def patch_html_batch(results: list) -> bool:
    if not any(hs or ss for _, hs, ss in results):
        print("\nNo URLs to patch — index.html unchanged.")
        return False

    html = read_html()
    for ep, hs_url, ss_url in results:
        if hs_url:
            html = patch_hs(html, ep, hs_url)
        if ss_url:
            html = patch_ss(html, ep, ss_url)
    write_html(html)
    return True


def git_commit_push(episodes: list) -> None:
    ep_list = ", ".join(str(e) for e in sorted(episodes))
    try:
        subprocess.run(["git", "config", "user.email", "github-actions@github.com"], check=True)
        subprocess.run(["git", "config", "user.name",  "GitHub Actions"], check=True)
        subprocess.run(["git", "add", HTML_FILE], check=True)
        subprocess.run(
            ["git", "commit", "-m", f"chore: add episode(s) {ep_list} SS+HS links"],
            check=True,
        )
        subprocess.run(["git", "pull", "--rebase"], check=False)   # avoid rejection
        subprocess.run(["git", "push"], check=True)
        print(f"\n  Git pushed for episodes: {ep_list}")
    except subprocess.CalledProcessError as e:
        print(f"  Git error: {e}", file=sys.stderr)


# ── Magnet list parser ────────────────────────────────────────────────────────

def parse_magnet_list(raw: str) -> list:
    sep = "\n" if "\n" in raw else ","
    return [m.strip() for m in raw.split(sep) if m.strip().startswith("magnet:")]


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    all_mkv = []

    if MAGNET_LINKS:
        magnets = parse_magnet_list(MAGNET_LINKS)
        print(f"Batch mode: {len(magnets)} magnet(s)")
        for i, magnet in enumerate(magnets, 1):
            print(f"\n[{i}/{len(magnets)}] Downloading...")
            new_files = download_magnet(magnet)
            if not new_files:
                print("  No .mkv files found — skipping this magnet", file=sys.stderr)
            else:
                all_mkv.extend(new_files)
    else:
        episode = get_expected_episode()
        print(f"Auto mode — targeting episode {episode}")
        magnet = search_nyaa(episode)
        if not magnet:
            print(f"Episode {episode} not on Nyaa yet — exiting cleanly.")
            sys.exit(0)
        new_files = download_magnet(magnet)
        if not new_files:
            print("Download produced no .mkv files", file=sys.stderr)
            sys.exit(1)
        all_mkv.extend(new_files)

    if not all_mkv:
        print("Nothing to process.")
        sys.exit(0)

    print(f"\nProcessing {len(all_mkv)} file(s)...")

    results = []
    for i, mkv in enumerate(all_mkv, 1):
        print(f"\n[{i}/{len(all_mkv)}] {os.path.basename(mkv)}")
        try:
            ep, hs_url, ss_url = process_episode(mkv)
            results.append((ep, hs_url, ss_url))
        except Exception as e:
            print(f"  FATAL ERROR processing {mkv}: {e}", file=sys.stderr)
            # Continue to next file — never bail on the whole batch

    if results:
        changed = patch_html_batch(results)
        if changed:
            successful_eps = [ep for ep, hs, ss in results if hs or ss]
            if successful_eps:
                git_commit_push(successful_eps)

    # Summary
    print("\n── Run summary ──")
    for ep, hs_url, ss_url in results:
        hs = "OK" if hs_url else "FAIL"
        ss = "OK" if ss_url else "FAIL"
        print(f"  EP {ep:>4}  SS:{ss}  HS:{hs}")

    failed = [ep for ep, hs, ss in results if not hs and not ss]
    if failed:
        print(f"\n  {len(failed)} episode(s) fully failed: {failed}")
        sys.exit(1)
    else:
        print(f"\n  All {len(results)} episode(s) done.")


if __name__ == "__main__":
    main()
