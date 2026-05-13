"""
커넥트웨이브 채용 인텔리전스 수집기 v12
─ Custom Search 완전 제거
─ Gemini Google Search Grounding 전용
─ 429 방지: 쿼리 간 3초 대기 + 지수 백오프 재시도
─ GEMINI_API_KEY 하나만 필요
"""
import os, json, datetime, requests, time, re
from urllib.parse import urlparse

GEMINI_KEY = os.environ["GEMINI_API_KEY"]
MS_TENANT  = os.environ.get("MS_TENANT_ID", "")
MS_CLIENT  = os.environ.get("MS_CLIENT_ID", "")
MS_SECRET  = os.environ.get("MS_CLIENT_SECRET", "")
EMAIL_TO   = os.environ.get("ALERT_EMAIL_TO", "")
EMAIL_FROM = os.environ.get("ALERT_EMAIL_FROM", "")

GEMINI_URL = (f"https://generativelanguage.googleapis.com/v1beta"
              f"/models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}")

CW_GROUP  = {'다나와','에누리','에누리닷컴','danawa','enuri',
             '메이크샵','makeshop','플레이오토','playauto','몰테일','malltail'}
MIN_ITEMS = 5


# ── Gemini 호출 — 429 지수 백오프 포함 ───────────────────────────────────────
def gemini_call(payload: dict) -> dict:
    wait = 10  # 초기 대기 10초
    for attempt in range(4):
        try:
            resp = requests.post(GEMINI_URL,
                headers={"Content-Type": "application/json"},
                json=payload, timeout=120)
            if resp.status_code == 429:
                print(f"  ⏳ 429 한도 초과 — {wait}초 대기 후 재시도 ({attempt+1}/4)")
                time.sleep(wait)
                wait *= 2  # 10 → 20 → 40 → 80초
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError:
            raise
        except Exception as e:
            print(f"  ⚠️  Gemini 오류 ({attempt+1}/4): {e}")
            if attempt < 3:
                time.sleep(wait)
                wait *= 2
    raise RuntimeError("Gemini 4회 연속 실패")


# ── 날짜 계산 ─────────────────────────────────────────────────────────────────
def get_target_dates():
    today = datetime.date.today()
    wd = today.weekday()
    if wd == 0:
        dates = [today - datetime.timedelta(days=2),
                 today - datetime.timedelta(days=1), today]
        print(f"📅 월요일 — 3일치: {dates[0]} ~ {dates[2]}")
    else:
        dates = [today]
        print(f"📅 수집일: {today} ({'월화수목금토일'[wd]}요일)")
    return dates


# ── URL 검증 ─────────────────────────────────────────────────────────────────
BAD = ['search.naver.com', 'teamblind.com/kr/search/',
       'jobplanet.co.kr/companies?', 'jobkorea.co.kr/Search/?']

def is_bad_url(url: str) -> bool:
    if not url:
        return True
    try:
        if urlparse(url).path.rstrip('/') == '':
            return True
    except Exception:
        return True
    return any(b in url for b in BAD)


# ── 검색 쿼리 (429 방지 위해 5개로 최소화) ───────────────────────────────────
def build_queries(date: datetime.date) -> list:
    ym   = date.strftime("%Y년 %m월")
    ds   = str(date)                    # YYYY-MM-DD (영문 검색용)
    ds_k = date.strftime("%-m월 %-d일") # M월 D일 (한국어 검색용)
    yr   = date.year
    return [
        # ① 리더급 퇴사 — 오늘 날짜 명시해서 최신 기사 우선 수집
        f"대표이사 CTO CPO CISO CHRO CAIO 사임 퇴임 퇴사 {ds_k} {yr}",
        f"임원 대표 사임 이직 IT 이커머스 핀테크 스타트업 {ym}",
        # ② 구조조정·권고사직·희망퇴직 — 오늘 날짜 포함
        f"권고사직 희망퇴직 구조조정 감원 {ds_k} {yr}",
        f"희망퇴직 구조조정 IT 플랫폼 유통 금융 {ym}",
        # ③ 채용 시장 큰 변동
        f"채용 중단 조직 축소 대규모 채용 공채 {ym}",
        # ④ HR NEWS
        f"고용노동부 채용 HR 노무 정책 지원사업 {ym}",
        # ⑤ 글로벌
        f"글로벌 빅테크 외국계 감원 채용동결 한국 {ym}",
        # ⑥ 당일 실시간
        f"채용 구조조정 임원 퇴사 인사 뉴스 {ds}",
    ]


# ── 1단계: Gemini 웹 검색으로 기사 수집 (쿼리당 5초 대기) ────────────────────
def collect_articles(queries: list) -> list:
    print(f"  🔍 Gemini 웹 검색 ({len(queries)}개 쿼리, 쿼리당 5초 간격)...")
    articles = []
    seen     = set()

    for idx, q in enumerate(queries):
        if idx > 0:
            time.sleep(5)  # 429 방지 — 쿼리 간 5초 대기

        prompt = (
            f"구글 검색으로 다음 주제의 최신 뉴스 기사 3~5개를 찾아주세요: {q}\n\n"
            f"JSON 배열로만 응답 (다른 텍스트 없이):\n"
            f'[{{"title":"제목","url":"실제기사URL","source":"매체","date":"YYYY-MM-DD","snippet":"내용150자"}}]\n\n'
            f"규칙: url은 실제 기사 URL (경로 포함), search.naver.com 금지, 홈페이지 루트 금지"
        )
        try:
            data = gemini_call({
                "contents": [{"parts": [{"text": prompt}]}],
                "tools": [{"google_search": {}}],
                "generationConfig": {"maxOutputTokens": 2000, "temperature": 0.1}
            })
            raw = data["candidates"][0]["content"]["parts"][0]["text"]
            raw = re.sub(r'^```(?:json)?\s*', '', raw.strip())
            raw = re.sub(r'\s*```$', '', raw.strip())
            items = json.loads(raw)
            if not isinstance(items, list):
                continue
            for item in items:
                url = item.get("url", "")
                if not url or url in seen or is_bad_url(url):
                    continue
                seen.add(url)
                articles.append({
                    "title":   item.get("title", ""),
                    "url":     url,
                    "source":  item.get("source", ""),
                    "date":    item.get("date", ""),
                    "snippet": item.get("snippet", "")[:200],
                })
        except json.JSONDecodeError:
            pass
        except Exception as e:
            print(f"    검색 오류 ({q[:30]}...): {e}")

    print(f"  ✅ {len(articles)}개 기사 수집")
    return articles


# ── 2단계: Gemini 분석 ───────────────────────────────────────────────────────
ANALYZE_PROMPT = """당신은 커넥트웨이브 채용 인텔리전스 분석가입니다.
커넥트웨이브: 가격비교(다나와·에누리)·셀러커머스(메이크샵·플레이오토)·몰테일 그룹

【완전 제외】다나와·에누리닷컴·메이크샵·플레이오토·몰테일 → 절대 포함 금지

【카테고리】
  leader  = 대표/임원/CTO/CPO/CISO/CHRO/CAIO/본부장급 이동·퇴임
  outflow = 권고사직·희망퇴직·구조조정·채용중단·조직축소
  hiring  = 공채·채용확대·투자유치 후 채용
  foreign = 해외·외국계 동향
  hr      = 채용법령·노무·급여·HR트렌드·정부지원

【신뢰도】A=공시·공식 / B=주요언론 / C=커뮤니티 (hr은 C 제외)

【출처 절대 원칙】
- sources.url은 [기사 목록]의 실제 URL만 사용
- search.naver.com 금지, URL 추측·생성 금지
- 출처 없는 항목 금지

【priority·urgency 부여 기준 — 즉시주목 패널에 노출됨】
priority=true + urgency=high → 즉시주목 최상단 노출 조건:
  - 대표이사·CTO·CPO·CISO·CHRO·CAIO·본부장급 퇴사·사임 (cat=leader)
  - 100명 이상 또는 전 직원 대상 구조조정·권고사직·희망퇴직 (cat=outflow)
  - 채용 시장에 큰 영향을 주는 규제·정책 변화 (모든 cat)

priority=true + urgency=mid → 주목:
  - 임원급(팀장·본부장급 이상) 이탈·조직개편
  - 50인 미만 구조조정·희망퇴직

priority=false → 참고·모니터링

【분량】최소 {MIN}개 items 필수. 동일 사건 기사 → 1개 통합, sources에 모두 열거.

【정렬 기준】items는 날짜 내림차순(최신 우선) → 같은 날짜면 중요도 높은 순

【해요체 필수】body·insight·action 전부 ~해요/~하세요 어미

순수 JSON만 출력 (마크다운 없이):
{"date":"DATE","summary":"요약70자","contact_targets":["기업1"],"items":[
  {"id":"DATENODASH-slug","date":"DATE","time":"09:00",
   "cat":"leader|outflow|hiring|foreign|hr","priority":true,"urgency":"high|mid|low",
   "company":"기업명","signal":"신호20자","level":"A|B|C","title":"제목60자",
   "body":"내용200자(해요체)","insight":"인사이트120자(해요체)","action":"액션120자(~하세요)",
   "tags":["태그"],"sources":[{"name":"매체","url":"실제URL","level":"B"}]}
]}"""

def analyze(articles: list, date: datetime.date) -> dict:
    print(f"  🧠 Gemini 분석 중... ({len(articles)}개 기사)")
    time.sleep(5)  # 이전 검색 호출 후 여유 시간
    ds = str(date)
    dn = ds.replace("-", "")
    prompt = (ANALYZE_PROMPT
              .replace("{MIN}", str(MIN_ITEMS))
              .replace("DATE", ds)
              .replace("DATENODASH", dn)
              + f"\n\n[기사 목록]\n{json.dumps(articles, ensure_ascii=False, indent=1)}")

    for attempt in range(3):
        try:
            data = gemini_call({
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 8000, "temperature": 0.2}
            })
            raw = data["candidates"][0]["content"]["parts"][0]["text"]
            raw = re.sub(r'^```(?:json)?\s*', '', raw.strip())
            raw = re.sub(r'\s*```$', '', raw.strip())
            report = json.loads(raw)
            report["date"] = ds
            break
        except json.JSONDecodeError:
            print(f"  ⚠️  JSON 파싱 실패 ({attempt+1}/3)")
            if attempt == 2:
                return {"date": ds, "summary": "분석 실패", "contact_targets": [], "items": []}
            time.sleep(10)
        except Exception as e:
            print(f"  ❌ 분석 실패: {e}")
            return {"date": ds, "summary": "분석 실패", "contact_targets": [], "items": []}

    # 후처리
    report["items"] = [
        i for i in report.get("items", [])
        if not any(g in i.get("company","").lower().replace(" ","") for g in CW_GROUP)
    ]
    for item in report.get("items", []):
        item["sources"] = [s for s in item.get("sources", [])
                           if not is_bad_url(s.get("url",""))]
    report["items"] = [i for i in report["items"] if i.get("sources")]

    def score(i):
        return ({"leader":90,"outflow":80,"hr":50,"hiring":40,"foreign":35
                 }.get(i.get("cat",""),30)
                + {"high":15,"mid":7,"low":0}.get(i.get("urgency","low"),0)
                + (20 if i.get("priority") else 0)
                + {"A":10,"B":5,"C":0}.get(i.get("level","C"),0))
    report["items"].sort(key=score, reverse=True)

    n = len(report["items"])
    print(f"  ✅ {n}건 확정 {'✅' if n >= MIN_ITEMS else '⚠️ 목표 미달'}")
    return report


# ── 전체 파이프라인 ───────────────────────────────────────────────────────────
def collect(date: datetime.date) -> dict:
    print(f"\n{'─'*48}\n📅 {date} 수집 시작")
    articles = collect_articles(build_queries(date))
    report   = analyze(articles, date)

    if len(report.get("items", [])) < MIN_ITEMS:
        print(f"  ⚠️  {len(report.get('items',[]))}건 — 추가 검색...")
        time.sleep(15)  # 429 방지
        extra = collect_articles([
            f"한국 채용 시장 HR 트렌드 {date}",
            f"글로벌 채용 감원 이슈 {date.year}",
        ])
        seen = {a["url"] for a in articles}
        articles += [a for a in extra if a["url"] not in seen]
        report = analyze(articles, date)

    return report


# ── 저장 ─────────────────────────────────────────────────────────────────────
def save(report: dict):
    os.makedirs("reports", exist_ok=True)
    with open(f"reports/{report['date']}.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    os.makedirs("web", exist_ok=True)
    try:
        with open("web/data.json", encoding="utf-8") as f:
            history = json.load(f)
    except FileNotFoundError:
        history = []
    history = [h for h in history if h.get("date") != report["date"]]
    history.insert(0, report)
    history.sort(key=lambda x: x.get("date",""), reverse=True)
    history = history[:90]
    with open("web/data.json", "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    print(f"  💾 저장: {len(report.get('items',[]))}건")


# ── 이메일 (선택) ─────────────────────────────────────────────────────────────
def ms_token():
    r = requests.post(
        f"https://login.microsoftonline.com/{MS_TENANT}/oauth2/v2.0/token",
        data={"grant_type":"client_credentials","client_id":MS_CLIENT,
              "client_secret":MS_SECRET,
              "scope":"https://graph.microsoft.com/.default"}, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]

def send_email(reports, token):
    if not EMAIL_TO or not EMAIL_FROM:
        return
    all_items = sorted(
        [i for r in reports for i in r.get("items", [])],
        key=lambda i: {"leader":90,"outflow":80,"hr":50,"hiring":40,"foreign":35
                       }.get(i.get("cat",""),30), reverse=True)
    CAT  = {"leader":"리더이탈","outflow":"인재유출","hiring":"채용확대",
            "foreign":"해외","hr":"HR NEWS"}
    ds   = ", ".join(r["date"] for r in reports)
    rows = "".join(
        f"<tr><td style='padding:5px;font-size:11px;color:#888'>{i.get('date','')}</td>"
        f"<td style='padding:5px'>{CAT.get(i.get('cat',''),'')}</td>"
        f"<td style='padding:5px;font-weight:700'>{i.get('company','')}</td>"
        f"<td style='padding:5px;font-size:11px'>{i.get('title','')[:45]}</td></tr>"
        for i in all_items)
    html = (f"<html><body style='font-family:sans-serif'>"
            f"<div style='background:#b92218;color:#fff;padding:14px 20px'>"
            f"<b>CW 채용 인텔리전스</b> · {ds} · {len(all_items)}건</div>"
            f"<table style='width:100%;border-collapse:collapse'>"
            f"<thead><tr style='background:#f5f5f5'>"
            f"<th style='padding:6px;font-size:10px;text-align:left'>날짜</th>"
            f"<th style='padding:6px;font-size:10px;text-align:left'>유형</th>"
            f"<th style='padding:6px;font-size:10px;text-align:left'>기업</th>"
            f"<th style='padding:6px;font-size:10px;text-align:left'>내용</th>"
            f"</tr></thead><tbody>{rows}</tbody></table></body></html>")
    recipients = [{"emailAddress":{"address":a.strip()}}
                  for a in EMAIL_TO.split(",") if a.strip()]
    requests.post(
        f"https://graph.microsoft.com/v1.0/users/{EMAIL_FROM}/sendMail",
        headers={"Authorization":f"Bearer {token}",
                 "Content-Type":"application/json"},
        json={"message":{"subject":f"[CW 채용인텔] {ds} — {len(all_items)}건",
              "body":{"contentType":"HTML","content":html},
              "from":{"emailAddress":{"address":EMAIL_FROM}},
              "toRecipients":recipients},"saveToSentItems":"false"},
        timeout=30).raise_for_status()
    print(f"  📧 이메일 → {EMAIL_TO}")


# ── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 48)
    print("🚀 커넥트웨이브 채용 인텔리전스 v12")
    print("=" * 48)
    target_dates = get_target_dates()
    reports = []
    for d in target_dates:
        report = collect(d)
        save(report)
        reports.append(report)
        time.sleep(2)
    if all([EMAIL_TO, EMAIL_FROM, MS_TENANT, MS_CLIENT, MS_SECRET]) and reports:
        try:
            send_email(reports, ms_token())
        except Exception as e:
            print(f"  ⚠️  이메일 실패: {e}")
    print(f"\n{'='*48}")
    for r in reports:
        n = len(r.get("items", []))
        print(f"{'✅' if n >= MIN_ITEMS else '⚠️ '} {r['date']}: {n}건")

if __name__ == "__main__":
    main()
