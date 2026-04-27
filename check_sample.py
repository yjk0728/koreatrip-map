import json, sys
sys.stdout.reconfigure(encoding='utf-8')
loc = json.load(open('data/locations.json', encoding='utf-8'))
# show first 3 entries structure
for l in loc[:3]:
    print("series_name:", l['series_name'])
    print("display_name:", l['display_name'])
    print("lat/lng:", l['lat'], l['lng'])
    print("source:", l['source'])
    print("episode_count:", l['episode_count'])
    if l['episodes']:
        ep = l['episodes'][0]
        print("  ep sample:", ep['title'], '|', ep['service_url'])
    print()
print("File size:", len(json.dumps(loc, ensure_ascii=False)), "chars")
