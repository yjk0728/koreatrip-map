"""
Step 2 (revised): Build location dataset by extracting geo-entities
from series names + episode keywords, using a comprehensive Korean
place lookup table. Falls back to Nominatim only for unresolved places.
Output: data/locations.json  (grouped by place, episodes attached)
        data/episodes.json   (each episode with lat/lng)
"""
import json, re, sys, os, time, urllib.request, urllib.parse, collections

sys.stdout.reconfigure(encoding='utf-8')

# ───────────────────────────────────────────────────────── place lookup ──
PLACES = {
    # 특별시·광역시·특별자치시
    "서울": (37.5665, 126.9780),
    "부산": (35.1796, 129.0756),
    "대구": (35.8714, 128.6014),
    "인천": (37.4563, 126.7052),
    "광주": (35.1595, 126.8526),
    "대전": (36.3504, 127.3845),
    "울산": (35.5384, 129.3114),
    "세종": (36.4800, 127.2890),
    # 도
    "경기": (37.4138, 127.5183),
    "강원": (37.8228, 128.1555),
    "충북": (36.8, 127.7),
    "충남": (36.5184, 126.8000),
    "전북": (35.7175, 127.1530),
    "전남": (34.8679, 126.9910),
    "경북": (36.4919, 128.8889),
    "경남": (35.4606, 128.2132),
    "제주": (33.4996, 126.5312),
    # 시·군·구
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
    "시흥": (37.3799, 126.8028),
    "부천": (37.5035, 126.7660),
    "고양": (37.6584, 126.8320),
    "의정부": (37.7381, 127.0337),
    "남양주": (37.6360, 127.2163),
    "포천": (37.8947, 127.2002),
    "동두천": (37.9036, 127.0606),
    "안성": (37.0078, 127.2796),
    "김포": (37.6152, 126.7155),
    "청주": (36.6424, 127.4890),
    "충주": (36.9910, 127.9259),
    "제천": (37.1322, 128.1909),
    "보은": (36.4896, 127.7292),
    "옥천": (36.3064, 127.5718),
    "영동": (36.1749, 127.7784),
    "괴산": (36.8157, 127.7867),
    "단양": (36.9846, 128.3657),
    "천안": (36.8151, 127.1139),
    "공주": (36.4465, 127.1190),
    "보령": (36.3333, 126.6128),
    "아산": (36.7898, 127.0022),
    "서산": (36.7848, 126.4503),
    "논산": (36.1873, 127.0987),
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
    "남해": (34.8377, 127.8927),
    "하동": (35.0674, 127.7518),
    "산청": (35.4150, 127.8736),
    "함양": (35.5204, 127.7251),
    "거창": (35.6870, 127.9094),
    "합천": (35.5659, 128.1655),
    "기장": (35.2446, 129.2223),
    "청원": (36.6424, 127.4890),
    "완주": (35.9065, 127.1622),
    "진천": (36.8556, 127.4360),
    "음성": (36.9404, 127.6902),
    "증평": (36.7857, 127.5815),
    # 특별 지명
    "제주시": (33.4996, 126.5312),
    "서귀포": (33.2541, 126.5600),
    "울릉도": (37.4845, 130.9057),
    "독도": (37.2425, 131.8660),
    # 산
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
    "속리산": (36.5437, 127.8595),
    "주왕산": (36.3927, 129.1670),
    "월출산": (34.7553, 126.7029),
    "선운산": (35.5089, 126.5923),
    "마이산": (35.7427, 127.4073),
    "칠갑산": (36.4048, 126.9129),
    "조계산": (34.9828, 127.3058),
    "남산": (37.5511, 126.9882),
    "팔당산": (37.5394, 127.3047),
    "소요산": (37.9315, 127.0743),
    "관악산": (37.4448, 126.9639),
    "도봉산": (37.6892, 127.0130),
    "수리산": (37.3639, 126.9219),
    "가지산": (35.5937, 129.0428),
    "영남알프스": (35.6000, 129.0000),
    "덕항산": (37.1927, 129.1192),
    "방태산": (38.0306, 128.2639),
    "점봉산": (38.0000, 128.4000),
    # 강·호수
    "팔당호": (37.5394, 127.3047),
    "충주호": (36.9177, 127.9881),
    "소양강": (37.7833, 127.9097),
    "섬진강": (35.3000, 127.5000),
    "남한강": (37.3167, 127.9333),
    "북한강": (37.7667, 127.5000),
    "동강": (37.2453, 128.7217),
    "영산강": (35.0000, 126.7500),
    "금강": (36.0000, 127.0000),
    "낙동강": (35.5000, 128.5000),
    "임진강": (37.9800, 126.8500),
    "만경강": (35.9500, 127.0000),
    "한강": (37.5200, 126.9900),
    "파로호": (38.0989, 127.7050),
    "대청호": (36.5000, 127.5500),
    "안동호": (36.5500, 128.9000),
    "합천호": (35.5000, 128.1000),
    "보령호": (36.3500, 126.6500),
    "여자만": (34.8500, 127.4000),
    "진해만": (35.1500, 128.7000),
    # 섬
    "거문도": (34.0200, 127.3100),
    "흑산도": (34.6880, 125.4310),
    "홍도": (34.6880, 125.1880),
    "증도": (35.0000, 126.1167),
    "소안도": (34.1833, 126.7167),
    "청산도": (34.1892, 127.0000),
    "보길도": (34.2167, 126.5500),
    "추자도": (33.9167, 126.8333),
    "우도": (33.5025, 126.9522),
    "마라도": (33.1167, 126.2667),
    "덕적도": (37.2333, 126.1500),
    "대청도": (37.5000, 124.7167),
    "백령도": (37.9667, 124.6833),
    "강화도": (37.7424, 126.4877),
    "교동도": (37.7822, 126.2981),
    "영종도": (37.4925, 126.4883),
    "안면도": (36.5833, 126.3667),
    "거제도": (34.8799, 128.6211),
    "남해도": (34.8377, 127.8927),
    "사량도": (34.7333, 128.1167),
    "선미도": (37.1000, 126.5667),
    "승봉도": (37.1500, 126.2333),
    "자월도": (37.2333, 126.2833),
    "이작도": (37.1700, 126.2700),
    "대이작도": (37.1700, 126.2700),
    "비양도": (33.4667, 126.2500),
    "가파도": (33.1721, 126.2658),
    "추자군도": (33.9167, 126.8333),
    "고군산군도": (35.7781, 126.4767),
    "조도군도": (34.4000, 125.9500),
    "흑산군도": (34.6000, 125.4000),
    "소안군도": (34.1833, 126.7167),
    "금오열도": (34.4500, 127.7000),
    "다도해": (34.5000, 126.5000),
    "통영의 섬": (34.8000, 128.2000),
    # 해안·반도·만
    "변산반도": (35.6833, 126.5667),
    "태안반도": (36.7454, 126.2983),
    "여수반도": (34.7500, 127.6000),
    "고흥반도": (34.6079, 127.2778),
    "한려수도": (34.9667, 128.1000),
    "서해": (36.0000, 126.0000),
    "남해": (34.5000, 127.5000),
    "동해": (37.5244, 129.1143),
    "칠산바다": (35.5000, 126.3000),
    "진주만": (35.1500, 128.0500),
    "해파랑길": (37.5000, 129.0000),
    # 기타 지역
    "팔당": (37.5394, 127.3047),
    "만리포": (36.7961, 126.2006),
    "강화": (37.7424, 126.4877),
    "임진각": (37.8833, 126.7500),
    "수덕사": (36.6162, 126.6657),
    "관동팔경": (37.8000, 128.9000),
    "관동별곡": (37.8000, 128.9000),
    "백두대간": (36.5000, 128.0000),
    "장항선": (36.7000, 126.5000),
    "경전선": (35.3000, 128.0000),
    "영동선": (37.5000, 128.7000),
    "동해남부선": (35.5000, 129.2000),
    "전라선": (35.5000, 127.3000),
    "7번국도": (36.5000, 129.0000),
    "2번국도": (35.5000, 127.5000),
    "진안고원": (35.7913, 127.4249),
    "순천만": (34.8833, 127.5000),
    "내포": (36.5000, 126.5000),
    "무진장": (35.8000, 127.5000),
    "하동포구": (35.0674, 127.7518),
    "외씨버선길": (36.7000, 128.5000),
    "은비령": (38.0000, 128.1000),
    "울주": (35.5384, 129.3114),
    "추풍령": (36.2194, 127.9844),
    "제주할망": (33.4996, 126.5312),
    "흑산군도를 가다": (34.6000, 125.4000),
    "서해 갯벌": (36.5000, 126.0000),
    "서해포구기행": (36.5000, 126.5000),
    "남도갯길": (34.7000, 126.9000),
    "남포": (36.2000, 126.6000),
    "경주": (35.8562, 129.2247),
    "안동고택": (36.5684, 128.7294),
    "모악산": (35.7000, 127.1000),
    "팔당호": (37.5394, 127.3047),
    "팔공산": (35.9906, 128.6919),
    "경주": (35.8562, 129.2247),
    "가야": (35.8267, 128.1166),
    "고령": (35.7274, 128.2633),
    "서울역사기행": (37.5665, 126.9780),
    "서울산책": (37.5665, 126.9780),
    "부산교향곡": (35.1796, 129.0756),
    "아리랑": (37.5665, 126.9780),
}

def clean_name(name):
    """Remove trailing punctuation/whitespace."""
    return re.sub(r'[\s,\.]+$', '', name.strip())

def find_place(text):
    """
    Search text for any known place name.
    Returns (place_name, (lat, lng)) or (None, None).
    Sort by length desc so longer names match first.
    """
    for place, coords in sorted(PLACES.items(), key=lambda x: -len(x[0])):
        if place in text:
            return place, coords
    return None, None

def get_location_for_series(series_name, sample_keywords):
    """
    Try multiple strategies to find coordinates for a series.
    Returns dict with display_name, lat, lng, source.
    """
    clean = clean_name(series_name)

    # 1. Direct match on cleaned name
    if clean in PLACES:
        lat, lng = PLACES[clean]
        return {"display_name": clean, "lat": lat, "lng": lng, "source": "direct"}

    # 2. Check if any known place is in the series name
    place, coords = find_place(clean)
    if place:
        return {"display_name": place, "lat": coords[0], "lng": coords[1], "source": "substring"}

    # 3. Try keywords from sample episodes
    for kw in sample_keywords:
        kw_clean = clean_name(kw)
        if kw_clean in PLACES:
            lat, lng = PLACES[kw_clean]
            return {"display_name": kw_clean, "lat": lat, "lng": lng, "source": "keyword_direct"}
        place, coords = find_place(kw_clean)
        if place:
            return {"display_name": place, "lat": coords[0], "lng": coords[1], "source": "keyword_sub"}

    # 4. First word
    first = clean.split()[0] if clean.split() else clean
    if first in PLACES:
        lat, lng = PLACES[first]
        return {"display_name": first, "lat": lat, "lng": lng, "source": "first_word"}

    return {"display_name": clean, "lat": None, "lng": None, "source": "not_found"}


def nominatim_geocode(query):
    """Geocode using Nominatim."""
    url = "https://nominatim.openstreetmap.org/search?" + urllib.parse.urlencode({
        "q": query,
        "format": "json",
        "limit": 1,
        "countrycodes": "kr",
        "accept-language": "ko",
    })
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "KoreaTripMapProject/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            results = json.loads(resp.read().decode('utf-8'))
            if results:
                return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as e:
        print(f"  Nominatim error: {e}", file=sys.stderr)
    return None


def main():
    episodes = json.load(open('data/episodes_raw.json', encoding='utf-8'))

    # Group episodes by series_name
    series_map = collections.defaultdict(list)
    for ep in episodes:
        series_map[ep["series_name"]].append(ep)

    print(f"Total series: {len(series_map)}", file=sys.stderr)

    # Load nominatim cache
    cache_file = "data/nominatim_cache.json"
    nom_cache = {}
    if os.path.exists(cache_file):
        nom_cache = json.load(open(cache_file, encoding='utf-8'))

    # Geocode each series
    located_series = {}
    not_found_series = []
    nominatim_queries = 0

    for series_name, ep_list in series_map.items():
        # Collect sample keywords from first 2 episodes
        sample_kw = []
        for ep in ep_list[:2]:
            sample_kw.extend(ep.get("keywords", []))

        loc = get_location_for_series(series_name, sample_kw)

        # Nominatim fallback for unresolved
        if loc["lat"] is None:
            clean = clean_name(series_name)
            if clean in nom_cache:
                cached = nom_cache[clean]
                if cached:
                    loc["lat"], loc["lng"] = cached
                    loc["source"] = "nominatim_cached"
            else:
                print(f"  Nominatim: {clean}", file=sys.stderr)
                result = nominatim_geocode(clean + " 대한민국")
                nominatim_queries += 1
                time.sleep(1.1)
                if result:
                    loc["lat"], loc["lng"] = result
                    loc["source"] = "nominatim"
                    nom_cache[clean] = result
                else:
                    nom_cache[clean] = None
                    not_found_series.append(series_name)

                # Save cache every 10 queries
                if nominatim_queries % 10 == 0:
                    with open(cache_file, "w", encoding='utf-8') as f:
                        json.dump(nom_cache, f, ensure_ascii=False, indent=2)

        located_series[series_name] = loc

    # Final cache save
    with open(cache_file, "w", encoding='utf-8') as f:
        json.dump(nom_cache, f, ensure_ascii=False, indent=2)

    # ── Build locations.json (one entry per unique lat/lng cluster) ──────
    # Attach episodes to each location
    loc_data = []
    for series_name, ep_list in series_map.items():
        loc = located_series[series_name]
        if loc["lat"] is None:
            continue

        loc_entry = {
            "series_name": series_name,
            "display_name": loc["display_name"],
            "lat": round(loc["lat"], 6),
            "lng": round(loc["lng"], 6),
            "source": loc["source"],
            "episode_count": len(ep_list),
            "episodes": [
                {
                    "lect_id": ep["lect_id"],
                    "title": ep["title"],
                    "episode_number": ep["episode_number"],
                    "subtitle": ep["subtitle"],
                    "broadcast_date": ep["broadcast_date"],
                    "keywords": ep["keywords"][:6],
                    "service_url": ep["service_url"],
                }
                for ep in ep_list
            ]
        }
        loc_data.append(loc_entry)

    # Also build flat episodes.json with lat/lng
    ep_data = []
    for series_name, ep_list in series_map.items():
        loc = located_series[series_name]
        for ep in ep_list:
            ep_data.append({
                "lect_id": ep["lect_id"],
                "title": ep["title"],
                "series_name": series_name,
                "display_name": loc.get("display_name", series_name),
                "episode_number": ep["episode_number"],
                "subtitle": ep["subtitle"],
                "broadcast_date": ep["broadcast_date"],
                "keywords": ep["keywords"],
                "service_url": ep["service_url"],
                "lat": round(loc["lat"], 6) if loc["lat"] else None,
                "lng": round(loc["lng"], 6) if loc["lng"] else None,
            })

    os.makedirs("data", exist_ok=True)
    with open("data/locations.json", "w", encoding='utf-8') as f:
        json.dump(loc_data, f, ensure_ascii=False, indent=2)
    with open("data/episodes.json", "w", encoding='utf-8') as f:
        json.dump(ep_data, f, ensure_ascii=False, indent=2)

    located_count = sum(1 for l in loc_data)
    print(f"\nLocated series: {located_count} / {len(series_map)}", file=sys.stderr)
    print(f"Total episodes with location: {sum(l['episode_count'] for l in loc_data)}", file=sys.stderr)
    print(f"Nominatim queries: {nominatim_queries}", file=sys.stderr)
    print(f"Not found: {len(not_found_series)}", file=sys.stderr)
    if not_found_series[:15]:
        print(f"Not found samples:", file=sys.stderr)
        for s in not_found_series[:15]:
            print(f"  {s}", file=sys.stderr)

    # Stats by source
    source_counts = collections.Counter(located_series[s]["source"] for s in located_series)
    print(f"\nSource breakdown:", file=sys.stderr)
    for src, cnt in source_counts.most_common():
        print(f"  {src}: {cnt}", file=sys.stderr)


if __name__ == "__main__":
    main()
