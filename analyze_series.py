"""Analyze series names to understand geocoding needs."""
import json, re, sys, collections
sys.stdout.reconfigure(encoding='utf-8')

episodes = json.load(open('data/episodes_raw.json', encoding='utf-8'))
series_counts = collections.Counter(e['series_name'] for e in episodes)

# Print all series sorted by count (descending)
print(f"Total series: {len(series_counts)}")
print(f"Total episodes: {sum(series_counts.values())}")
print()
print("All series (count | name):")
for name, count in series_counts.most_common():
    print(f"  {count:4d} | {name}")
