import json, sys, collections
sys.stdout.reconfigure(encoding='utf-8')

loc = json.load(open('data/locations.json', encoding='utf-8'))
ep = json.load(open('data/episodes.json', encoding='utf-8'))

total_eps = len(ep)
located = sum(1 for e in ep if e['lat'])
series_count = len(loc)

print(f"=== 최종 데이터 현황 ===")
print(f"총 에피소드: {total_eps}편")
print(f"위치 확인: {located}편 ({located/total_eps*100:.1f}%)")
print(f"위치 미확인: {total_eps-located}편 (테마형 시리즈)")
print(f"지도 포인트: {series_count}개 시리즈")
print()

# 지역별 분포
region_cnt = collections.Counter()
for l in loc:
    region_cnt[l['display_name']] += l['episode_count']

print("상위 20개 지역 (에피소드 수):")
for name, cnt in region_cnt.most_common(20):
    print(f"  {name}: {cnt}편")

print()
# 방영 연도 분포
year_cnt = collections.Counter()
for e in ep:
    if e['broadcast_date']:
        year_cnt[e['broadcast_date'][:4]] += 1
print("연도별 에피소드:")
for y in sorted(year_cnt):
    print(f"  {y}: {year_cnt[y]}편")

print()
# service_url 샘플
sample = next((e for e in ep if e['lat'] and e['service_url']), None)
if sample:
    print("서비스 URL 예시:")
    print(f"  {sample['service_url']}")
