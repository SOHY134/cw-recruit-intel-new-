"""
커넥트웨이브 채용 인텔리전스 수집기 v7
═══════════════════════════════════════════════════════════
보장 원칙:
  - 하루 최소 5개 항목 확보 (부족 시 폴백 쿼리 자동 실행)
  - 주말 포함 매일 오전 10:00 KST 자동 실행
  - 월요일: 토·일·월 3일치 합산
  - 출처 없는 항목·검색 URL 항목 자동 제거
  - 그룹사(다나와·에누리·메이크샵·플레이오토·몰테일) 완전 제외
═══════════════════════════════════════════════════════════
"""
import os, json, datetime, requests, time
from urllib.parse import urlparse

API_KEY   = os.environ["ANTHROPIC_API_KEY"]
MS_TENANT = os.environ.get("MS_TENANT_ID","")
MS_CLIENT = os.environ.get("MS_CLIENT_ID","")
MS_SECRET = os.environ.get("MS_CLIENT_SECRET","")
EMAIL_TO  = os.environ.get("ALERT_EMAIL_TO","")
EMAIL_FROM= os.environ.get("ALERT_EMAIL_FROM","")

CW_GROUP = {'다나와','에누리','에누리닷컴','danawa','enuri',
            '메이크샵','makeshop','플레이오토','playauto','몰테일','malltail'}

MIN_ITEMS = 5   # 하루 최소 보장 건수

# ── 날짜 계산 ──────────────────────────────────────────────────────────────────
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

# ── 핵심 쿼리 (매일 실행) ──────────────────────────────────────────────────────
def core_queries(date: datetime.date):
    ym = date.strftime("%Y년 %m월")
    ds = str(date)
    return [
        # 리더 이탈
        f"대표이사 CTO CPO 사임 퇴임 퇴사 {ym} IT 플랫폼 이커머스",
        f"임원 경영진 리더십 변화 {ym} 스타트업 핀테크 게임",
        # 인재 유출
        f"권고사직 희망퇴직 구조조정 감원 {ym} IT 플랫폼",
        f"케이뱅크 야놀자 카카오 네이버 쿠팡 11번가 구조조정 채용 {ym}",
        f"이커머스 스타트업 유니콘 권고사직 희망퇴직 {ym}",
        f"은행 보험 금융 희망퇴직 채용 {ym}",
        # 채용 확대
        f"대규모 채용 공채 투자유치 IT 플랫폼 {ym}",
        # HR NEWS
        f"고용노동부 채용 지원사업 HR 정책 {ym}",
        f"HR 채용 트렌드 AI 인사 {ym} 리포트",
        f"최저임금 포괄임금 근로기준법 HR 노무 {ym}",
        # 글로벌
        f"글로벌 빅테크 감원 채용동결 외국계 한국 {ym}",
        f"tech layoffs hiring {date.year} Korea",
        # 당일 실시간
        f"채용 구조조정 권고사직 {ds}",
        f"HR 인사 채용 뉴스 {ds}",
    ]

# ── 폴백 쿼리 (최소 건수 미달 시 추가 실행) ────────────────────────────────────
def fallback_queries(date: datetime.date):
    ym = date.strftime("%Y년 %m월")
    return [
        f"채용 박람회 컨퍼런스 HR {ym}",
        f"스타트업 채용 확대 시리즈 투자 {ym}",
        f"정년연장 노동법 개정 채용 영향 {ym}",
        f"AI 채용 HR테크 솔루션 트렌드 {ym}",
        f"외국계 기업 한국 채용 감원 {ym}",
        f"LinkedIn 채용 트렌드 인재 시장 {date.year}",
        f"원티드 사람인 잡코리아 채용 시장 동향 {ym}",
    ]

# ── 검색 URL 검증 ──────────────────────────────────────────────────────────────
BAD_URL_PATTERNS = [
    'search.naver.com/search.naver',
    'teamblind.com/kr/search/',
    'jobplanet.co.kr/companies?search_by',
    'jobkorea.co.kr/Search/?stext',
]
def is_bad_url(url: str) -> bool:
    return not url or any(p in url for p in BAD_URL_PATTERNS) or urlparse(url).path.rstrip('/') == ''

# ── 1단계: 웹서치 기사 수집 ────────────────────────────────────────────────────
STEP1_SYS = """채용 인텔리전스 리서처. 쿼리로 web_search를 실행하고 결과를 JSON 배열로만 출력.
다른 텍스트 없이 순수 JSON만:
[{"query":"쿼리","articles":[{"title":"제목","url":"실제URL","source":"매체","date":"YYYY-MM-DD","snippet":"150자","company":"기업명","cat_hint":"leader|outflow|hiring|foreign|hr","relevance":"high|mid|low"}]}]
규칙: URL은 검색결과에서 직접 나온 것만, 추측·생성 금지, relevance=low 제외"""

def search_batch(queries: list[str], label: str = "") -> list[dict]:
    if not queries:
        return []
    prompt = f"다음 {len(queries)}개 쿼리 각각에 web_search를 실행하세요:\n" + \
             "\n".join(f"{i+1}. {q}" for i, q in enumerate(queries))
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key":API_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json={"model":"claude-sonnet-4-20250514","max_tokens":12000,
                  "tools":[{"type":"web_search_20250305","name":"web_search"}],
                  "system":STEP1_SYS,"messages":[{"role":"user","content":prompt}]},
            timeout=240)
        resp.raise_for_status()
        text = "".join(b.get("text","") for b in resp.json()["content"] if b.get("type")=="text")
        raw = json.loads(text.strip())
        articles, seen = [], set()
        for r in raw:
            for a in r.get("articles",[]):
                url = a.get("url","")
                if not url or is_bad_url(url) or url in seen:
                    continue
                seen.add(url)
                articles.append(a)
        print(f"    {label}→ {len(articles)}개 기사 수집")
        return articles
    except Exception as e:
        print(f"    ⚠️  검색 오류: {e}")
        return []

# ── 2단계: 분석 (최소 5건 보장 지시 포함) ──────────────────────────────────────
STEP2_SYS = """커넥트웨이브 채용 인텔리전스 분석가.
커넥트웨이브: 가격비교(다나와·에누리)·셀러커머스(메이크샵·플레이오토)·몰테일 그룹

【그룹사 완전 제외】다나와·에누리닷컴·메이크샵·플레이오토·몰테일 → items에 절대 포함 금지

【카테고리】leader(대표/임원/CTO급) | outflow(권고사직·희망퇴직·구조조정) |
            hiring(공채·채용확대) | foreign(해외·외국계) | hr(채용법령·노무·HR트렌드)

【신뢰도】A=공시·공식자료 / B=주요언론·전문지 / C=커뮤니티 (hr카테고리는 C 배제)

【출처 규칙】
- sources.url은 반드시 [기사 목록]에 있는 실제 URL만 사용
- search.naver.com 검색 URL 절대 금지
- 홈페이지 루트 URL 절대 금지
- 출처 없는 항목 생성 금지

【중복 통합】동일 사건 기사 여러 개 → items 1개로 통합, sources에 모두 열거

【분량 보장 — 핵심】
- 반드시 최소 5개 이상 항목 작성 (절대 5개 미만 불가)
- 기사가 부족해도: hr 카테고리 채용 트렌드·노무 실무 항목으로 채울 것
- foreign 카테고리 최소 1건 포함

【priority=true 조건】
leader: 즉시 컨택 가능한 리더급 이탈 / outflow: 대규모 인재 유출 / hr: 즉시 법령·정책 대응 필요

【인사이트·액션 작성 기준】소싱·컨택·운영 관점으로 구체적으로:
✅ "케이뱅크 결제/정산 백엔드 5년차 이상을 LinkedIn에서 즉시 서치. 커머스 정산 도메인 확장성 강조."
❌ "관련 인재를 확인하세요"

JSON만 출력 (마크다운 없이):
{"date":"YYYY-MM-DD","summary":"70자 이내 요약","contact_targets":["기업1","기업2"],"items":[
  {"id":"YYYYMMDD-slug","date":"YYYY-MM-DD","time":"HH:MM","cat":"...","priority":true,"urgency":"high|mid|low",
   "company":"기업명","signal":"20자이내","level":"A|B|C","title":"60자이내",
   "body":"200자이내 실제기사기반","insight":"120자이내 구체적","action":"120자이내 구체적",
   "tags":["태그"],"sources":[{"name":"매체","url":"실제URL","level":"A|B|C"}]}
]}"""

def analyze(articles: list[dict], date: datetime.date) -> dict:
    print(f"  🧠 분석 중... ({len(articles)}개 기사)")
    if not articles:
        return {"date":str(date),"summary":"수집 기사 없음","contact_targets":[],"items":[]}
    prompt = (f"오늘: {date}\n기사 {len(articles)}개\n\n[기사 목록]\n"
              + json.dumps(articles, ensure_ascii=False, indent=1)
              + "\n\n위 기사만 근거로 분석하세요. 최소 5개 항목 필수.")
    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key":API_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json={"model":"claude-sonnet-4-20250514","max_tokens":10000,
                  "system":STEP2_SYS,"messages":[{"role":"user","content":prompt}]},
            timeout=150)
        resp.raise_for_status()
        text = "".join(b.get("text","") for b in resp.json()["content"] if b.get("type")=="text")
        report = json.loads(text.strip())
        report["date"] = str(date)
    except Exception as e:
        print(f"  ❌ 분석 실패: {e}")
        return {"date":str(date),"summary":"분석 실패","contact_targets":[],"items":[]}

    # 그룹사 제거
    before = len(report.get("items",[]))
    report["items"] = [i for i in report.get("items",[])
                       if not any(g in i.get("company","").lower().replace(" ","") for g in CW_GROUP)]
    # 검색 URL 제거
    for item in report.get("items",[]):
        item["sources"] = [s for s in item.get("sources",[]) if not is_bad_url(s.get("url",""))]
    # 출처 없는 항목 제거
    report["items"] = [i for i in report["items"] if i.get("sources")]
    # 중요도 정렬
    report["items"].sort(key=lambda i: (
        {"leader":90,"outflow":80,"hr":50,"hiring":40,"foreign":35}.get(i.get("cat",""),30)
        + {"high":15,"mid":7,"low":0}.get(i.get("urgency","low"),0)
        + (20 if i.get("priority") else 0)
        + {"A":10,"B":5,"C":0}.get(i.get("level","C"),0)
    ), reverse=True)

    removed = before - len(report["items"])
    print(f"  ✅ {len(report['items'])}건 확정 (그룹사/불량URL {removed}건 제거)")
    return report

# ── 전체 수집 파이프라인 ────────────────────────────────────────────────────────
def collect(date: datetime.date) -> dict:
    print(f"\n{'─'*50}\n📅 {date} 수집 시작")

    # 1차 검색
    queries = core_queries(date)
    print(f"  🔍 1차 검색 ({len(queries)}개 쿼리)...")
    articles = search_batch(queries, "1차 ")

    # 1차 분석
    report = analyze(articles, date)

    # 최소 건수 미달 → 폴백 검색 후 재분석
    if len(report.get("items",[])) < MIN_ITEMS:
        print(f"  ⚠️  {len(report.get('items',[]))}건 — 최소 {MIN_ITEMS}건 미달, 폴백 검색 실행...")
        fallback = fallback_queries(date)
        extra = search_batch(fallback, "폴백 ")
        all_articles = articles + extra
        # 중복 URL 제거
        seen, deduped = set(), []
        for a in all_articles:
            if a.get("url","") not in seen:
                seen.add(a["url"])
                deduped.append(a)
        report = analyze(deduped, date)
        if len(report.get("items",[])) < MIN_ITEMS:
            print(f"  ⚠️  폴백 후에도 {len(report.get('items',[]))}건 — 최선 결과 사용")

    return report

# ── 저장 ──────────────────────────────────────────────────────────────────────
def save(report: dict):
    os.makedirs("reports", exist_ok=True)
    with open(f"reports/{report['date']}.json","w",encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    # web/data.json (90일 보관)
    os.makedirs("web", exist_ok=True)
    try:
        with open("web/data.json",encoding="utf-8") as f:
            history = json.load(f)
    except FileNotFoundError:
        history = []
    history = [h for h in history if h.get("date") != report["date"]]
    history.insert(0, report)
    history.sort(key=lambda x: x.get("date",""), reverse=True)
    history = history[:90]
    with open("web/data.json","w",encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    print(f"  💾 저장: reports/{report['date']}.json")

# ── 이메일 (MS Graph) ──────────────────────────────────────────────────────────
def ms_token():
    r = requests.post(
        f"https://login.microsoftonline.com/{MS_TENANT}/oauth2/v2.0/token",
        data={"grant_type":"client_credentials","client_id":MS_CLIENT,
              "client_secret":MS_SECRET,"scope":"https://graph.microsoft.com/.default"},
        timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]

def send_email(reports: list[dict], token: str):
    if not EMAIL_TO or not EMAIL_FROM:
        return
    all_items = [i for r in reports for i in r.get("items",[])]
    all_items.sort(key=lambda i:(
        {"leader":90,"outflow":80,"hr":50,"hiring":40,"foreign":35}.get(i.get("cat",""),30)
        +(20 if i.get("priority") else 0)), reverse=True)
    date_str = ", ".join(r["date"] for r in reports)
    subject = f"[CW 채용인텔] {date_str} — {len(all_items)}건 수집"
    CAT = {"leader":"리더이탈","outflow":"인재유출","hiring":"채용확대","foreign":"해외외국계","hr":"HR NEWS"}
    rows = "".join(f"<tr><td style='padding:5px;font-size:11px;color:#888'>{i.get('date','')}</td>"
                   f"<td style='padding:5px;font-size:11px'>{CAT.get(i.get('cat',''),'')}</td>"
                   f"<td style='padding:5px;font-size:12px;font-weight:700'>{i.get('company','')}</td>"
                   f"<td style='padding:5px;font-size:11px'>{i.get('title','')[:45]}</td></tr>"
                   for i in all_items)
    html = f"""<html><body style='font-family:sans-serif'>
    <div style='background:#b92218;color:#fff;padding:14px 20px;border-radius:8px 8px 0 0'>
      <b>커넥트웨이브 채용 인텔리전스</b> · {date_str} · {len(all_items)}건
    </div>
    <table style='width:100%;border-collapse:collapse;border:1px solid #eee'>
      <thead><tr style='background:#f5f5f5'>
        <th style='padding:6px;font-size:10px;text-align:left'>날짜</th>
        <th style='padding:6px;font-size:10px;text-align:left'>유형</th>
        <th style='padding:6px;font-size:10px;text-align:left'>기업</th>
        <th style='padding:6px;font-size:10px;text-align:left'>내용</th>
      </tr></thead><tbody>{rows}</tbody>
    </table></body></html>"""
    recipients = [{"emailAddress":{"address":a.strip()}} for a in EMAIL_TO.split(",") if a.strip()]
    requests.post(
        f"https://graph.microsoft.com/v1.0/users/{EMAIL_FROM}/sendMail",
        headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"},
        json={"message":{"subject":subject,"body":{"contentType":"HTML","content":html},
              "from":{"emailAddress":{"address":EMAIL_FROM}},"toRecipients":recipients},
              "saveToSentItems":"false"},
        timeout=30).raise_for_status()
    print(f"  📧 이메일 발송 → {EMAIL_TO}")

# ── 메인 ──────────────────────────────────────────────────────────────────────
def main():
    print("="*50)
    print("🚀 커넥트웨이브 채용 인텔리전스 v7")
    print("="*50)
    target_dates = get_target_dates()
    reports = []
    for d in target_dates:
        report = collect(d)
        save(report)
        reports.append(report)
        time.sleep(2)

    # 이메일
    if all([EMAIL_TO, EMAIL_FROM, MS_TENANT, MS_CLIENT, MS_SECRET]) and reports:
        try:
            token = ms_token()
            send_email(reports, token)
        except Exception as e:
            print(f"  ⚠️  이메일 실패: {e}")

    # 요약
    print(f"\n{'='*50}")
    total = sum(len(r.get("items",[])) for r in reports)
    for r in reports:
        n = len(r.get("items",[]))
        flag = "✅" if n >= MIN_ITEMS else "⚠️ "
        print(f"{flag} {r['date']}: {n}건 {'(목표 달성)' if n >= MIN_ITEMS else f'(목표 {MIN_ITEMS}건 미달)'}")
    print(f"총 {total}건 수집 완료")

if __name__ == "__main__":
    main()
