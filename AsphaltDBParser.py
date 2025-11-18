import re
import json
from dataclasses import dataclass
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed



MEI_BASE_URL = "https://www.mei-a9.info/cars?car={car_id}"


# ========== 공통 유틸 ==========

def fetch_mei_html(car_id: int, session: Optional[requests.Session] = None) -> str:
    """MEI 페이지의 텍스트 가져오기 (requests.Session 재사용 가능)."""
    url = MEI_BASE_URL.format(car_id=car_id)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/129.0.0.0 Safari/537.36"
        )
    }

    # 세션이 있으면 세션으로, 없으면 그냥 requests.get
    if session is None:
        resp = requests.get(url, headers=headers, timeout=10)
    else:
        resp = session.get(url, headers=headers, timeout=10)

    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # 1순위: <pre> 안의 텍스트
    pre = soup.find("pre")
    if pre is not None:
        return pre.get_text("\n")

    # 2순위: main / article
    main = soup.find("main") or soup.find("article")
    if main is not None:
        return main.get_text("\n")

    # 최후: 전체 텍스트
    return soup.get_text("\n")



def normalize_line(line: str) -> str:
    line = line.replace("\xa0", " ")
    return re.sub(r"\s+", " ", line).strip()


def parse_number(s: str):
    """천단위/소수점/쉼표 혼종 숫자 파서."""
    s = s.strip()

    # 1) 천 단위 점: 42.486.000 -> 42486000
    if re.fullmatch(r"\d{1,3}(\.\d{3})+", s):
        return int(s.replace(".", ""))

    # 2) 쉼표만 있는 경우 (천단위 or 소수점)
    if "," in s and "." not in s:
        # 단일 쉼표: 68,200 또는 42,2
        if re.fullmatch(r"\d+,\d+", s):
            left, right = s.split(",")
            if len(right) == 3:
                # 68,200 → 68200
                return int(left + right)
            else:
                # 42,2 → 42.2
                return float(left + "." + right)
        else:
            # 1,234,567 같은 다중 쉼표 → 정수
            parts = s.split(",")
            if all(len(p) <= 3 for p in parts[1:]):
                return int("".join(parts))

    # 3) 일반 소수점
    if re.fullmatch(r"\d+\.\d+", s):
        return float(s)

    # 4) 순수 숫자
    if re.fullmatch(r"\d+", s):
        return int(s)

    raise ValueError(f"parse_number 실패: {s!r}")


def extract_bp_from_two_lines(line1: str, line2: Optional[str]):
    """
    line1: '5/8/30' 또는 '🔑/40/45/60/70/85'
    line2: '(43)' 또는 '(🔑 + 300)' 등
    """
    # 🔑, + 같은 기호 제거 후 숫자/슬래시 패턴만 보고 req 추출
    l1_clean = line1.replace("🔑", "").replace("+", "")

    # 1) blueprint 요구량: 예) '40/45/60/70/85'
    m1 = re.search(r"(\d+(?:/\d+)+)", l1_clean)
    reqs = None
    if m1:
        req_str = m1.group(1)
        reqs = [int(x) for x in req_str.split("/")]

    # 2) 총합: 괄호 안에 있는 숫자를 느슨하게 추출
    #    '(🔑 + 300)' 같은 것도 처리 가능
    m2 = re.search(r"\((?:[^\d]*)(\d+)[^\d]*\)", line1) or re.search(
        r"\((?:[^\d]*)(\d+)[^\d]*\)", line2 or ""
    )
    total = int(m2.group(1)) if m2 else None

    if reqs is not None and total is not None:
        return reqs, total
    if reqs is not None:
        return reqs, None

    return None


# ========== Stat 파싱 ==========

@dataclass
class StatEntry:
    kind: str          # 'stock' | 'star' | 'max_wo_epics' | 'gold'
    label: str         # 'Stock' | '⭐' | '⭐⭐' | 'Gold' ...
    rank: int
    top_speed: float
    accel: float
    handling: float
    nitro: float


def parse_stat_block(lines: List[str], start_idx: int):
    """
    헤더 예:
      'Stock [467]'
      '⭐ [728]'
      '⭐⭐ [1031]'
      'Gold [1381]'
      'Max w/o epics [xxxx]'
    아래 4줄이 스탯.
    """
    header = lines[start_idx]
    m = re.match(r"^(Stock|Gold|Max w/o epics|\⭐+)\s*\[(\d+)\]$", header)
    if not m:
        return None, start_idx
    label, rank_str = m.groups()
    rank = int(rank_str)

    if label == "Stock":
        kind = "stock"
    elif label == "Gold":
        kind = "gold"
    elif label == "Max w/o epics":
        kind = "max_wo_epics"
    else:
        kind = "star"  # '⭐', '⭐⭐' ...

    stats = []
    i = start_idx + 1
    while i < len(lines) and len(stats) < 4:
        line = lines[i]
        # 다른 블록 시작이면 중단
        if re.match(r"^(Stock|Gold|Max w/o epics|\⭐+)\s*\[\d+\]$", line):
            break

        m_speed = re.search(r"([\d\.,]+)\s*km/h", line)
        if m_speed:
            stats.append(parse_number(m_speed.group(1)))
        else:
            m_val = re.search(r"([\d\.,]+)", line)
            if m_val:
                stats.append(parse_number(m_val.group(1)))
        i += 1

    if len(stats) != 4:
        return None, i

    entry = StatEntry(
        kind=kind,
        label=label,
        rank=rank,
        top_speed=float(stats[0]),
        accel=float(stats[1]),
        handling=float(stats[2]),
        nitro=float(stats[3]),
    )
    return entry, i


# ========== 메인 파서 ==========

def parse_mei_page(text: str, car_id: int) -> Dict:
    lines_raw = text.splitlines()
    lines = [normalize_line(l) for l in lines_raw if normalize_line(l)]

    car: Dict = {
        "id": car_id,
        "unlock_method": None,  # "bp" | "key" | None
    }

    # ----- 1) class / name -----
    idx_class = None
    for i, l in enumerate(lines):
        if re.fullmatch(r"[DCBAS]", l):
            idx_class = i
            break

    if idx_class is None or idx_class + 1 >= len(lines):
        raise ValueError(f"[car_id={car_id}] class/이름 라인을 찾지 못했습니다.")

    car_class = lines[idx_class]
    name = lines[idx_class + 1]
    car["class"] = car_class
    car["name"] = name.strip()

    # ----- 2) 별 개수 (⭐⭐⭐) -----
    star_line = next((l for l in lines if set(l) == {"⭐"}), None)
    if star_line is None:
        raise ValueError(f"[car_id={car_id}] 별(⭐) 줄을 찾지 못했습니다.")
    max_star = len(star_line)
    car["max_star"] = max_star

    # ----- 3) fuel (⛽ 6 fuels) -----
    fuel_line = next((l for l in lines if l.startswith("⛽")), None)
    if fuel_line:
        m = re.search(r"⛽\s+(\d+)\s+fuels", fuel_line)
        car["fuel"] = int(m.group(1)) if m else None
    else:
        car["fuel"] = None

    # ----- 4) BP (5/8/30 (43)) + 열쇠 차 처리 -----
    from itertools import accumulate

    bp_reqs: List[int] = []
    bp_total: Optional[int] = None
    uses_key = False

    for i, l in enumerate(lines):
        cand_clean = l.replace("🔑", "")

        # "숫자/숫자/..." 패턴이 있는 줄을 후보로 본다
        if re.search(r"\d+/\d+/", cand_clean) or re.fullmatch(r"\d+(?:/\d+)+", cand_clean):
            next_line = lines[i + 1] if i + 1 < len(lines) else None
            extracted = extract_bp_from_two_lines(l, next_line)
            if extracted:
                bp_reqs, bp_total = extracted
                # 이 BP 정보 블록에 🔑가 들어있으면 열쇠 차
                uses_key = ("🔑" in l) or ("🔑" in (next_line or ""))
                break

    if bp_reqs:
        car["bp_requirements"] = bp_reqs
        car["bp_cumulative"] = list(accumulate(bp_reqs))
        car["bp_all"] = bp_total
        car["unlock_method"] = "key" if uses_key else "bp"
    else:
        car["bp_requirements"] = []
        car["bp_cumulative"] = []
        car["bp_all"] = None
        # unlock_method는 None 유지

    # ----- 5) Parts 섹션 (epic_importparts_amount, epic_price) -----
    epic_per_stat = 0
    epic_price_total = 0

    for i, l in enumerate(lines):
        if l.startswith("Epics:"):
            # 바로 다음 줄: '2 x 240000 x 4=' 형태
            if i + 1 < len(lines):
                m = re.search(r"(\d+)", lines[i + 1])
                if m:
                    epic_per_stat = int(m.group(1))
            # 그 다음 줄: '1,920,000' 같은 총 크레딧
            if i + 2 < len(lines):
                try:
                    epic_price_total = parse_number(lines[i + 2])
                except ValueError:
                    pass
            break

    has_epic = epic_per_stat > 0
    car["epic_importparts_amount"] = {
        "top_speed": epic_per_stat,
        "accel": epic_per_stat,
        "handling": epic_per_stat,
        "nitro": epic_per_stat,
    }
    car["epic_price"] = epic_price_total

    # ----- 6) Stat 블록 (Stock / ⭐ / ⭐⭐ / Gold / Max w/o epics) -----
    stat_entries: List[StatEntry] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if re.match(r"^(Stock|Gold|Max w/o epics|\⭐+)\s*\[\d+\]$", line):
            entry, next_i = parse_stat_block(lines, i)
            if entry:
                stat_entries.append(entry)
                i = next_i
                continue
        i += 1

    if not stat_entries:
        raise ValueError(f"[car_id={car_id}] 스탯 블록을 하나도 찾지 못했습니다.")

    # kind별로 분류
    stock_entry = next((e for e in stat_entries if e.kind == "stock"), None)
    gold_entry = next((e for e in stat_entries if e.kind == "gold"), None)
    max_wo_entry = next((e for e in stat_entries if e.kind == "max_wo_epics"), None)
    star_entries = [e for e in stat_entries if e.kind == "star"]

    # 각 성별로 rank가 가장 높은 entry만 남기기
    best_by_star: Dict[int, StatEntry] = {}
    for e in star_entries:
        sc = len(e.label)  # '⭐' 개수 = 성수
        cur = best_by_star.get(sc)
        if cur is None or e.rank > cur.rank:
            best_by_star[sc] = e

    # === 새 구조: 각 성(★)마다 하나의 객체 ===
    stat_list = []

    # 1) 1성 무강 (Stock = 1★ 무강)
    if stock_entry:
        stat_list.append({
            "star": 1,
            "type": "stock",
            "rank": stock_entry.rank,
            "top_speed": stock_entry.top_speed,
            "accel": stock_entry.accel,
            "handling": stock_entry.handling,
            "nitro": stock_entry.nitro,
        })

    # 2) 1★ ~ (max★-1) 풀강 (각 성당 1개씩만, 최댓값)
    for star in range(1, max_star):
        e = best_by_star.get(star)
        if not e:
            continue
        stat_list.append({
            "star": star,
            "type": "full",
            "rank": e.rank,
            "top_speed": e.top_speed,
            "accel": e.accel,
            "handling": e.handling,
            "nitro": e.nitro,
        })

    # 3) max★ w/o epics (에픽 있는 차만)
    if has_epic and max_wo_entry:
        stat_list.append({
            "star": max_star,
            "type": "max_wo_epics",
            "rank": max_wo_entry.rank,
            "top_speed": max_wo_entry.top_speed,
            "accel": max_wo_entry.accel,
            "handling": max_wo_entry.handling,
            "nitro": max_wo_entry.nitro,
        })

    # 4) max★ full (Gold)
    if gold_entry:
        stat_list.append({
            "star": max_star,
            "type": "gold",
            "rank": gold_entry.rank,
            "top_speed": gold_entry.top_speed,
            "accel": gold_entry.accel,
            "handling": gold_entry.handling,
            "nitro": gold_entry.nitro,
        })

    car["stat"] = stat_list

    # ----- 7) 업글 비용 (upgrade_cumulative / per_star / upgrade_all) -----
    star_full_totals: List[int] = []
    in_block = False

    for l in lines:
        # 블록 시작: '⭐', '⭐⭐', ...
        if re.fullmatch(r"\⭐+", l):
            in_block = True
            continue
        if in_block:
            if l.startswith("="):
                # '= 68,200 ' 같은 줄
                m = re.search(r"=\s*([\d,]+)", l)
                if m:
                    total_full = parse_number(m.group(1))
                    star_full_totals.append(int(total_full))
                in_block = False
            # 그 외 줄은 무시 (From..., stage cost, parts cost 등)

    # Total: 전체 업글비 (Full)
    total_line = next((l for l in lines if l.startswith("Total:")), None)
    upgrade_all_full = None
    if total_line:
        m = re.search(r"Total:\s*([\d,]+)", total_line)
        if m:
            upgrade_all_full = parse_number(m.group(1))

    # upgrade_cumulative = 각 성 full 기준 누적값
    upgrade_cumulative: List[int] = []
    if star_full_totals:
        upgrade_cumulative = star_full_totals

    # per_star
    upgrade_per_star: List[int] = []
    for idx, val in enumerate(upgrade_cumulative):
        if idx == 0:
            upgrade_per_star.append(val)
        else:
            upgrade_per_star.append(val - upgrade_cumulative[idx - 1])

    car["upgrade_cumulative"] = upgrade_cumulative
    car["upgrade_per_star"] = upgrade_per_star
    if upgrade_all_full is not None:
        car["upgrade_all"] = int(upgrade_all_full)

    return car


def parse_mei_car(car_id: int, session: Optional[requests.Session] = None) -> Dict:
    text = fetch_mei_html(car_id, session=session)
    return parse_mei_page(text, car_id)


def process_one_car(cid: int):
    """
    스레드에서 실행할 worker 함수.
    성공 시 (cid, car_data, None)
    실패 시 (cid, None, 예외) 반환.
    """
    try:
        car_data = parse_mei_car(cid)
        return cid, car_data, None
    except Exception as e:
        return cid, None, e


if __name__ == "__main__":
    import time

    print("생성할 car_id 범위를 입력하세요.")
    print("예시:")
    print("  1-350         -> 1부터 350까지")
    print("  10,11,12      -> 10, 11, 12만")
    print("  1-5,10,20-22  -> 1~5, 10, 20~22 모두")
    raw = input("car_id 범위: ").strip()

    if not raw:
        print("입력이 비어 있습니다. 종료합니다.")
        raise SystemExit

    # --- 범위 파싱 ---
    id_set = set()

    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue

        if "-" in part:
            try:
                start_str, end_str = part.split("-", 1)
                start = int(start_str.strip())
                end = int(end_str.strip())
            except ValueError:
                print(f"[경고] 범위 파싱 실패: {part!r} → 건너뜀")
                continue

            if start > end:
                start, end = end, start

            for cid in range(start, end + 1):
                id_set.add(cid)
        else:
            try:
                cid = int(part)
                id_set.add(cid)
            except ValueError:
                print(f"[경고] car_id 정수 변환 실패: {part!r} → 건너뜀")

    car_ids = sorted(id_set)

    if not car_ids:
        print("유효한 car_id가 하나도 없습니다. 종료합니다.")
        raise SystemExit

    # --- JSON 파일 이름 입력 ---
    default_filename = "cars.json"
    filename = input(f"저장할 JSON 파일 이름을 입력하세요 (기본값: {default_filename}): ").strip()
    if not filename:
        filename = default_filename

    print(f"\n=== Asphalt MEI Scraper v8 (Session 모드) ===")
    print(f"총 {len(car_ids)}개의 car_id를 처리합니다: {car_ids}")
    print(f"출력 파일: {filename}\n")

    results = []
    errors = []

    start_all = time.perf_counter()

    # 세션 하나만 만들어서 끝까지 재사용
    with requests.Session() as session:
        for idx, cid in enumerate(car_ids, start=1):
            t0 = time.perf_counter()
            try:
                car_data = parse_mei_car(cid, session=session)
                t1 = time.perf_counter()
                elapsed_ms = (t1 - t0) * 1000

                # 와다다 로그 (바로바로 내려오게 flush=True)
                print(
                    f"[OK ] {idx:4d}/{len(car_ids):4d} | id={cid:4d} | {elapsed_ms:7.2f} ms",
                    flush=True
                )
                results.append(car_data)

            except Exception as e:
                t1 = time.perf_counter()
                elapsed_ms = (t1 - t0) * 1000
                print(
                    f"[ERR] {idx:4d}/{len(car_ids):4d} | id={cid:4d} | {elapsed_ms:7.2f} ms | {e}",
                    flush=True
                )
                errors.append((cid, e))

    total_elapsed = time.perf_counter() - start_all

    # id 기준 정렬 (혹시나 순서 꼬이는 것 방지용)
    results.sort(key=lambda c: c.get("id", 0))

    # --- JSON 파일로 저장 ---
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print("\n=== 완료 보고 ===")
        print(f"성공: {len(results)}개, 실패: {len(errors)}개")
        print(f"총 소요 시간: {total_elapsed:.2f} 초")
        if results:
            avg_per_car = total_elapsed / len(results)
            print(f"차량 1대당 평균: {avg_per_car*1000:.2f} ms")
        if errors:
            print("실패 car_id 목록:", [cid for cid, _ in errors])
    except Exception as e:
        print(f"[ERROR] JSON 파일 저장 실패: {e}")
