"""
Step 1: Extract episode metadata from all koreatrip JSON files.
Outputs: data/episodes_raw.json
"""
import json
import glob
import re
import sys
import os

sys.stdout.reconfigure(encoding='utf-8')

SRC_DIR = "../koreatrip"
OUT_FILE = "data/episodes_raw.json"

def extract_series_name(title):
    """
    Extract the series/region name from episode title.
    Patterns:
      '해남 1부, ...'          → '해남'
      '7번국도 1부 부산, ...'  → '7번국도'
      '팔당호 3부, ...'        → '팔당호'
      '옹진 겨울 섬 1부, ...' → '옹진 겨울 섬'
    """
    # Match everything before N부 (N=number)
    m = re.match(r'^(.+?)\s+\d+부', title)
    if m:
        return m.group(1).strip()
    # Fallback: first word before comma
    m2 = re.match(r'^([^,]+)', title)
    if m2:
        return m2.group(1).strip()
    return title.strip()

def extract_episode_number(title):
    m = re.search(r'(\d+)부', title)
    return int(m.group(1)) if m else None

def extract_subtitle(title):
    """Extract the subtitle after 'N부, '"""
    m = re.search(r'\d+부[,\s]+(.+)', title)
    return m.group(1).strip() if m else ""

def main():
    files = sorted(glob.glob(os.path.join(SRC_DIR, "*.json")))
    print(f"Processing {len(files)} files...", file=sys.stderr)

    episodes = []
    for i, fpath in enumerate(files):
        try:
            with open(fpath, encoding='utf-8') as f:
                d = json.load(f)
        except Exception as e:
            print(f"Error reading {fpath}: {e}", file=sys.stderr)
            continue

        meta = d.get("metadata", {})
        ep = meta.get("episode", {})

        title = meta.get("title", "")
        series_name = extract_series_name(title)
        ep_num = extract_episode_number(title)
        subtitle = extract_subtitle(title)

        episode = {
            "lect_id": meta.get("lect_id"),
            "title": title,
            "series_name": series_name,
            "episode_number": ep_num,
            "subtitle": subtitle,
            "broadcast_date": ep.get("broadcast_date"),
            "description": ep.get("description", ""),
            "keywords": ep.get("keywords", []),
            "service_url": meta.get("service_url"),
        }
        episodes.append(episode)

        if (i + 1) % 500 == 0:
            print(f"  {i+1}/{len(files)} processed", file=sys.stderr)

    # Sort by broadcast_date then lect_id
    episodes.sort(key=lambda x: (x.get("broadcast_date") or "", x.get("lect_id") or ""))

    os.makedirs("data", exist_ok=True)
    with open(OUT_FILE, "w", encoding='utf-8') as f:
        json.dump(episodes, f, ensure_ascii=False, indent=2)

    print(f"\nDone. {len(episodes)} episodes saved to {OUT_FILE}", file=sys.stderr)

    # Print unique series names for review
    series = sorted(set(e["series_name"] for e in episodes))
    print(f"\n{len(series)} unique series names:", file=sys.stderr)
    for s in series[:50]:
        print(f"  {s}", file=sys.stderr)

if __name__ == "__main__":
    main()
