"""
Step 2: Extract unique locations from series names, geocode them.
Uses Nominatim (OpenStreetMap) with 1s delay between requests.
Outputs: data/locations.json
"""
import json
import re
import sys
import time
import os
import urllib.request
import urllib.parse

sys.stdout.reconfigure(encoding='utf-8')

EPISODES_FILE = "data/episodes_raw.json"
LOCATIONS_FILE = "data/locations.json"

# Korean administrative region coordinates (pre-built fallback)
KOREA_PLACES = {
    "서울": (37.5665, 126.9780),
    "부산": (35.1796, 129.0756),
    "대구": (35.8714, 128.6014),
    "인천": (37.4563, 126.7052),
    "광주": (35.1595, 126.8526),
    "대전": (36.3504, 127.3845),
    "울산": (35.5384, 129.3114),
    "세종": (36.4800, 127.2890),
    "경기": (37.4138, 127.5183),
    "강원": (37.8228, 128.1555),
    "충북": (36.8, 127.7),
    "충남": (36.5184, 126.8000),
    "전북": (35.7175, 127.1530),
    "전남": (34.8679, 126.9910),
    "경북": (36.4919, 128.8889),
    "경남": (35.4606, 128.2132),
    "제주": (33.4996, 126.5312),
    "강릉": (37.7519, 128.8761),
    "춘천": (37.8813, 127.7298),
    "원주": (37.3422, 127.9202),
    "속초": (38.2070, 128.5919),
    "태백": (37.1640, 129.0022),
    "삼척": (37.4497, 129.1664),
    "동해": (37.5244, 129.1143),
    "고성": (38.3806, 128.4677),
    "양양": (38.0756, 128.6185),
    "인제": (38.0694, 128.1706),
    "평창": (37.3706, 128.3896),
    "횡성": (37.4914, 127.9847),
    "홍천": (37.6970, 127.8889),
    "양구": (38.1075, 127.9896),
    "화천": (38.1068, 127.7077),
    "철원": (38.1463, 127.3136),
    "파주": (37.7597, 126.7797),
    "연천": (38.0961, 127.0752),
    "가평": (37.8314, 127.5098),
    "양평": (37.4916, 127.4874),
    "여주": (37.2982, 127.6378),
    "이천": (37.2720, 127.4353),
    "용인": (37.2411, 127.1776),
    "수원": (37.2636, 127.0286),
    "안양": (37.3943, 126.9568),
    "성남": (37.4449, 127.1389),
    "안산": (37.3219, 126.8309),
    "화성": (37.1997, 126.8312),
    "평택": (36.9921, 127.1127),
    "오산": (37.1498, 127.0776),
    "시흥": (37.3799, 126.8028),
    "부천": (37.5035, 126.7660),
    "광명": (37.4785, 126.8648),
    "고양": (37.6584, 126.8320),
    "의정부": (37.7381, 127.0337),
    "남양주": (37.6360, 127.2163),
    "구리": (37.5943, 127.1297),
    "하남": (37.5392, 127.2148),
    "광주": (37.4296, 127.2558),  # 경기 광주
    "포천": (37.8947, 127.2002),
    "동두천": (37.9036, 127.0606),
    "의왕": (37.3449, 126.9688),
    "군포": (37.3614, 126.9352),
    "안성": (37.0078, 127.2796),
    "김포": (37.6152, 126.7155),
    "청주": (36.6424, 127.4890),
    "충주": (36.9910, 127.9259),
    "제천": (37.1322, 128.1909),
    "보은": (36.4896, 127.7292),
    "옥천": (36.3064, 127.5718),
    "영동": (36.1749, 127.7784),
    "증평": (36.7857, 127.5815),
    "진천": (36.8556, 127.4360),
    "음성": (36.9404, 127.6902),
    "괴산": (36.8157, 127.7867),
    "단양": (36.9846, 128.3657),
    "천안": (36.8151, 127.1139),
    "공주": (36.4465, 127.1190),
    "보령": (36.3333, 126.6128),
    "아산": (36.7898, 127.0022),
    "서산": (36.7848, 126.4503),
    "논산": (36.1873, 127.0987),
    "계룡": (36.2748, 127.2489),
    "당진": (36.8894, 126.6458),
    "금산": (36.1088, 127.4884),
    "부여": (36.2757, 126.9098),
    "서천": (36.0796, 126.6915),
    "청양": (36.4593, 126.8022),
    "홍성": (36.5999, 126.6600),
    "예산": (36.6800, 126.8467),
    "태안": (36.7454, 126.2983),
    "전주": (35.8242, 127.1480),
    "군산": (35.9676, 126.7370),
    "익산": (35.9483, 126.9577),
    "정읍": (35.5700, 126.8561),
    "남원": (35.4163, 127.3897),
    "김제": (35.8033, 126.8810),
    "완주": (35.9065, 127.1622),
    "진안": (35.7913, 127.4249),
    "무주": (36.0073, 127.6604),
    "장수": (35.6469, 127.5206),
    "임실": (35.6178, 127.2894),
    "순창": (35.3749, 127.1376),
    "고창": (35.4358, 126.7021),
    "부안": (35.7317, 126.7337),
    "목포": (34.8118, 126.3922),
    "여수": (34.7604, 127.6622),
    "순천": (34.9506, 127.4872),
    "나주": (35.0160, 126.7108),
    "광양": (34.9407, 127.6956),
    "담양": (35.3214, 126.9881),
    "곡성": (35.2817, 127.2921),
    "구례": (35.2032, 127.4628),
    "고흥": (34.6079, 127.2778),
    "보성": (34.7715, 127.0794),
    "화순": (35.0648, 126.9869),
    "장흥": (34.6821, 126.9073),
    "강진": (34.6418, 126.7700),
    "해남": (34.5736, 126.5990),
    "영암": (34.8004, 126.6963),
    "무안": (34.9900, 126.4817),
    "함평": (35.0672, 126.5174),
    "영광": (35.2771, 126.5120),
    "장성": (35.3026, 126.7848),
    "완도": (34.3110, 126.7547),
    "진도": (34.4864, 126.2633),
    "신안": (34.8316, 126.1081),
    "포항": (36.0190, 129.3434),
    "경주": (35.8562, 129.2247),
    "김천": (36.1398, 128.1133),
    "안동": (36.5684, 128.7294),
    "구미": (36.1196, 128.3441),
    "영주": (36.8057, 128.6240),
    "영천": (35.9733, 128.9382),
    "상주": (36.4110, 128.1590),
    "문경": (36.5866, 128.1860),
    "경산": (35.8251, 128.7411),
    "군위": (36.2400, 128.5720),
    "의성": (36.3527, 128.6974),
    "청송": (36.4359, 129.0573),
    "영양": (36.6672, 129.1127),
    "영덕": (36.4153, 129.3651),
    "청도": (35.6476, 128.7339),
    "고령": (35.7274, 128.2633),
    "성주": (35.9195, 128.2833),
    "칠곡": (35.9957, 128.4011),
    "예천": (36.6567, 128.2949),
    "봉화": (36.8932, 128.7322),
    "울진": (36.9930, 129.4017),
    "울릉": (37.4845, 130.9057),
    "창원": (35.2280, 128.6811),
    "진주": (35.1799, 128.1076),
    "통영": (34.8544, 128.4335),
    "사천": (35.0040, 128.0643),
    "김해": (35.2281, 128.8892),
    "밀양": (35.5036, 128.7461),
    "거제": (34.8799, 128.6211),
    "양산": (35.3351, 129.0376),
    "의령": (35.3224, 128.2619),
    "함안": (35.2728, 128.4065),
    "창녕": (35.5442, 128.4923),
    "고성": (34.9731, 128.3225),
    "남해": (34.8377, 127.8927),
    "하동": (35.0674, 127.7518),
    "산청": (35.4150, 127.8736),
    "함양": (35.5204, 127.7251),
    "거창": (35.6870, 127.9094),
    "합천": (35.5659, 128.1655),
    "제주시": (33.4996, 126.5312),
    "서귀포": (33.2541, 126.5600),
    # 특수 지명
    "설악산": (38.1191, 128.4658),
    "지리산": (35.3373, 127.7308),
    "한라산": (33.3617, 126.5292),
    "북한산": (37.6590, 126.9770),
    "소백산": (36.9612, 128.4867),
    "오대산": (37.7954, 128.5433),
    "태백산": (37.0980, 128.9181),
    "덕유산": (35.8572, 127.7519),
    "가야산": (35.8267, 128.1166),
    "내장산": (35.4829, 126.8883),
    "월악산": (36.9024, 128.0782),
    "치악산": (37.3729, 128.1029),
    "계룡산": (36.3423, 127.2003),
    "무등산": (35.1264, 126.9882),
    "팔공산": (35.9906, 128.6919),
    "청량산": (36.9079, 128.8892),
    "팔영산": (34.6097, 127.2876),
    "두륜산": (34.5074, 126.6034),
    "흑석산": (34.5462, 126.6831),
    "팔당호": (37.5394, 127.3047),
    "파로호": (38.0989, 127.7050),
    "소양강": (37.7833, 127.9097),
    "섬진강": (35.3000, 127.5000),
    "남한강": (37.3167, 127.9333),
    "북한강": (37.7667, 127.5000),
    "동강": (37.2453, 128.7217),
    "영산강": (35.0000, 126.7500),
    "금강": (36.0000, 127.0000),
    "낙동강": (35.5000, 128.5000),
    "임진강": (37.9800, 126.8500),
    "울릉도": (37.4845, 130.9057),
    "독도": (37.2425, 131.8660),
    "거문도": (34.0200, 127.3100),
    "흑산도": (34.6880, 125.4310),
    "홍도": (34.6880, 125.1880),
    "진도": (34.4864, 126.2633),
    "완도": (34.3110, 126.7547),
    "남해도": (34.8377, 127.8927),
    "거제도": (34.8799, 128.6211),
    "덕적도": (37.2333, 126.1500),
    "대청도": (37.5000, 124.7167),
    "백령도": (37.9667, 124.6833),
    "강화도": (37.7424, 126.4877),
    "교동도": (37.7822, 126.2981),
    "영종도": (37.4925, 126.4883),
    "안면도": (36.5833, 126.3667),
    "신지도": (34.4167, 126.8333),
    "청산도": (34.1892, 127.0000),
    "보길도": (34.2167, 126.5500),
    "추자도": (33.9167, 126.8333),
    "비양도": (33.4667, 126.2500),
    "우도": (33.5025, 126.9522),
    "마라도": (33.1167, 126.2667),
    "오가도": (36.9000, 126.1000),
    "주문도": (37.6333, 126.1167),
    "자월도": (37.2333, 126.2833),
    "이작도": (37.1700, 126.2700),
    "승봉도": (37.1500, 126.2333),
    "선미도": (37.1000, 126.5667),
    "팔당": (37.5394, 127.3047),
    "태안": (36.7454, 126.2983),
    "만리포": (36.7961, 126.2006),
    "변산": (35.7317, 126.7337),
    "장호항": (37.3842, 129.2583),
    "임진각": (37.8833, 126.7500),
    "문산": (37.8500, 126.7833),
    "강화": (37.7424, 126.4877),
    "용인": (37.2411, 127.1776),
    "수원": (37.2636, 127.0286),
    "홍천": (37.6970, 127.8889),
    "증도": (35.0000, 126.1167),
    "신안": (34.8316, 126.1081),
    "고흥반도": (34.6079, 127.2778),
    "다도해": (34.5000, 126.5000),
    "사량도": (34.7333, 128.1167),
    "한려수도": (34.9667, 128.1000),
    "통영": (34.8544, 128.4335),
    "아름다운나라": (37.5665, 126.9780),
    "북한": (39.0000, 125.7500),
    "금강산": (38.6500, 128.3000),
    "묘향산": (39.9167, 126.0000),
    "백두산": (42.0069, 128.0833),
    "압록강": (41.0000, 126.5000),
    "두만강": (42.3000, 130.6000),
    "수덕사": (36.6162, 126.6657),
    "마이산": (35.7427, 127.4073),
    "월출산": (34.7553, 126.7029),
    "칠갑산": (36.4048, 126.9129),
    "조계산": (34.9828, 127.3058),
    "선운산": (35.5089, 126.5923),
    "남산": (37.5511, 126.9882),
    "북악산": (37.5914, 126.9764),
    "속리산": (36.5437, 127.8595),
}

def clean_series_name(name):
    """Clean trailing punctuation and whitespace from series name."""
    name = name.strip()
    name = re.sub(r'[,\.]+$', '', name).strip()
    return name

def extract_main_place(series_name):
    """
    Try to extract the main geographic entity from a series name.
    Returns the best search query for geocoding.
    """
    name = clean_series_name(series_name)

    # Direct lookup first
    if name in KOREA_PLACES:
        return name, KOREA_PLACES[name]

    # Remove seasonal/descriptive prefixes
    seasonal = r'^(봄|여름|가을|겨울|초봄|초여름|초가을|초겨울|한여름|한겨울|늦봄|늦가을|눈 내리는|비 오는)\s+'
    name_stripped = re.sub(seasonal, '', name)
    if name_stripped in KOREA_PLACES:
        return name_stripped, KOREA_PLACES[name_stripped]

    # Check if any known place is in the name
    # Sort by length desc to match longer names first
    for place, coords in sorted(KOREA_PLACES.items(), key=lambda x: -len(x[0])):
        if place in name:
            return place, coords

    # Try the first word
    first_word = name.split()[0] if name.split() else name
    if first_word in KOREA_PLACES:
        return first_word, KOREA_PLACES[first_word]

    return name, None

def nominatim_geocode(query):
    """Geocode using Nominatim, restricted to South Korea."""
    url = "https://nominatim.openstreetmap.org/search?" + urllib.parse.urlencode({
        "q": query,
        "format": "json",
        "limit": 1,
        "countrycodes": "kr",
        "accept-language": "ko",
    })
    headers = {"User-Agent": "KoreaTripMapProject/1.0"}
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            results = json.loads(resp.read().decode('utf-8'))
            if results:
                return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as e:
        print(f"  Nominatim error for '{query}': {e}", file=sys.stderr)
    return None

def main():
    episodes = json.load(open(EPISODES_FILE, encoding='utf-8'))

    # Get unique series names
    series_names = sorted(set(e["series_name"] for e in episodes))
    print(f"Geocoding {len(series_names)} unique series names...", file=sys.stderr)

    # Load existing cache if any
    cache_file = "data/geocode_cache.json"
    cache = {}
    if os.path.exists(cache_file):
        cache = json.load(open(cache_file, encoding='utf-8'))
        print(f"Loaded {len(cache)} cached geocodes", file=sys.stderr)

    locations = {}
    nominatim_count = 0
    not_found = []

    for i, series in enumerate(series_names):
        clean = clean_series_name(series)

        if series in cache:
            locations[series] = cache[series]
            continue

        place_name, coords = extract_main_place(series)

        if coords:
            locations[series] = {
                "series_name": series,
                "display_name": place_name,
                "lat": coords[0],
                "lng": coords[1],
                "source": "lookup"
            }
            cache[series] = locations[series]
        else:
            # Try Nominatim
            print(f"  [{i+1}/{len(series_names)}] Nominatim: {clean}", file=sys.stderr)
            coords = nominatim_geocode(clean + " 한국")
            nominatim_count += 1
            time.sleep(1.1)  # Nominatim rate limit: 1 req/sec

            if coords:
                locations[series] = {
                    "series_name": series,
                    "display_name": clean,
                    "lat": coords[0],
                    "lng": coords[1],
                    "source": "nominatim"
                }
                cache[series] = locations[series]
            else:
                not_found.append(series)
                locations[series] = {
                    "series_name": series,
                    "display_name": clean,
                    "lat": None,
                    "lng": None,
                    "source": "not_found"
                }

        # Save cache periodically
        if (i + 1) % 50 == 0:
            with open(cache_file, "w", encoding='utf-8') as f:
                json.dump(cache, f, ensure_ascii=False, indent=2)

    # Final cache save
    with open(cache_file, "w", encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    # Build final output: attach location to each episode
    loc_list = []
    added = set()
    for series, loc in locations.items():
        if loc.get("lat") and series not in added:
            loc["episodes"] = [
                {
                    "lect_id": e["lect_id"],
                    "title": e["title"],
                    "subtitle": e["subtitle"],
                    "episode_number": e["episode_number"],
                    "broadcast_date": e["broadcast_date"],
                    "keywords": e["keywords"],
                    "service_url": e["service_url"],
                }
                for e in episodes if e["series_name"] == series
            ]
            loc_list.append(loc)
            added.add(series)

    os.makedirs("data", exist_ok=True)
    with open(LOCATIONS_FILE, "w", encoding='utf-8') as f:
        json.dump(loc_list, f, ensure_ascii=False, indent=2)

    print(f"\nDone.", file=sys.stderr)
    print(f"  Located: {len([l for l in loc_list if l['lat']])}", file=sys.stderr)
    print(f"  Nominatim queries: {nominatim_count}", file=sys.stderr)
    print(f"  Not found: {len(not_found)}", file=sys.stderr)
    if not_found[:20]:
        print(f"  Not found samples: {not_found[:20]}", file=sys.stderr)

if __name__ == "__main__":
    main()
