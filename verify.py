import json, sys
sys.stdout.reconfigure(encoding='utf-8')
d = json.load(open('data/episodes_raw.json', encoding='utf-8'))
print('Total episodes:', len(d))
print('Sample:')
for e in d[:5]:
    print(e['series_name'], '|', e['title'], '|', e['broadcast_date'])
print()
series = sorted(set(e['series_name'] for e in d))
print(f'Unique series: {len(series)}')
for s in series[:50]:
    print(' ', s)
