import json, sys
sys.stdout.reconfigure(encoding='utf-8')

loc = json.load(open('data/locations.json', encoding='utf-8'))
ep = json.load(open('data/episodes.json', encoding='utf-8'))

print(f"locations.json: {len(loc)} entries")
print(f"episodes.json: {len(ep)} entries")
print(f"Episodes with coords: {sum(1 for e in ep if e['lat'])}")
print()
print("Sample locations:")
for l in sorted(loc, key=lambda x: -x['episode_count'])[:15]:
    print(f"  {l['display_name']:12} | {l['series_name']:20} | {l['episode_count']}편 | {l['lat']:.3f},{l['lng']:.3f}")

print()
# Check not-found series
raw = json.load(open('data/episodes_raw.json', encoding='utf-8'))
import collections
not_found = [e for e in ep if not e['lat']]
series_nf = collections.Counter(e['series_name'] for e in not_found)
print(f"Not found series (top 20):")
for s, c in series_nf.most_common(20):
    print(f"  {c:3d}편 | {s}")
