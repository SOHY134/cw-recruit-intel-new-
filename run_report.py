"""
커넥트웨이브 채용 인텔리전스 수집기 v8 — Gemini 무료 API
═══════════════════════════════════════════════════════════
- Google Gemini 2.0 Flash (무료, 카드 불필요)
- Google Custom Search API (무료 100회/일)
- 하루 최소 5개 항목 보장 (폴백 검색 자동 실행)
- 주말 포함 매일 오전 10:00 KST 자동 실행
- 월요일: 토·일·월 3일치 합산
- 그룹사(다나와·에누리·메이크샵·플레이오토·몰테일) 완전 제외

필요한 GitHub Secrets:
  GEMINI_API_KEY        ← aistudio.google.com에서 무료 발급
  GOOGLE_SEARCH_API_KEY ← console.cloud.google.com (무료 100회/일)
  GOOGLE_SEARCH_CX      ← programmablesearchengine.google.com (무료)

선택 Secrets (이메일 발송용):
  MS_TENANT_ID / MS_CLIENT_ID / MS_CLIENT_SECRET
  ALERT_EMAIL_TO / ALERT_EMAIL_FROM
═══════════════════════════════════════════════════════════
"""
import os, json, datetime, requests, time, re
from urllib.parse import urlparse

# ── 환경변수 ──────────────────────────────────────────────────────────────────
GEMINI_KEY  = os.environ["GEMINI_API_KEY"]
SEARCH_KEY  = os.environ.get("GOOGLE_SEARCH_API_KEY", "")
SEARCH_CX   = os.environ.get("GOOGLE_SEARCH_CX", "")
MS_TENANT   = os.environ.get("MS_TENANT_ID", "")
MS_CLIENT   = os.environ.get("MS_CLIENT_ID", "")
MS_SECRET   = os.environ.get("MS_CLIENT_SECRET", "")
EMAIL_TO    = os.environ.get("ALERT_EMAIL_TO", "")
EMAIL_FROM  = os.environ.get("ALERT_EMAIL_FROM", "")

GEMINI_URL  = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}"

CW_GROUP    = {'다나와','에누리','에누리닷컴','danawa','enuri',
               '메이크샵','makeshop','플레이오토','playauto','몰테일','malltail'}
MIN_ITEMS   = 5

# ── Gemini 호출 ──────────────────────────────────────────────────────────────
def gemini(prompt: str, max_tokens: int = 8000) -> str:
    """Gemini 2.0 Flash 호출 — JSON 문자열 반환"""
    resp = requests.post(
        GEMINI_URL,
        headers={"Content-Type": "application/json"},
        json={
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "maxOutputTokens": max_tokens,
                "temperature": 0.2,
            }
        },
        timeout=120
    )
    resp.raise_for_status()
    data = resp.json()
    return data["candidates"][0]["content"]["parts"][0]["text"]

# ── 날짜 계산 ────────────────────────────────────────────────────────────────
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

# ── 검색 쿼리 목록 ────────────────────────────────────────────────────────────
def core_queries(date: datetime.date):
    ym = date.strftime("%Y년 %m월")
    ds = str(date)
    return [
        f"대표이사 CTO CPO 사임 퇴임 퇴사 {ym} IT 플랫폼 이커머스",
        f"임원 경영진 리더십 변화 {ym} 스타트업 핀테크",
        f"권고사직 희망퇴직 구조조정 감원 {ym} IT 플랫폼",
        f"케이뱅크 야놀자 카카오 네이버 쿠팡 11번가 구조조정 {ym}",
        f"이커머스 스타트업 권고사직 희망퇴직 {ym}",
        f"은행 금융 희망퇴직 채용 {ym}",
        f"대규모 채용 공채 투자유치 IT 플랫폼 {ym}",
        f"고용노동부 채용 지원사업 HR 정책 {ym}",
        f"HR 채용 트렌드 AI 인사 {ym}",
        f"최저임금 포괄임금 근로기준법 노무 {ym}",
        f"글로벌 빅테크 감원 채용동결 외국계 {ym}",
        f"채용 구조조정 권고사직 {ds}",
        f"HR 인사 채용 뉴스 {ds}",
    ]

def fallback_queries(date: datetime.date):
    ym = date.strftime("%Y년 %m월")
    return [
        f"채용 박람회 HR 컨퍼런스 {ym}",
        f"스타트업 채용 투자유치 시리즈 {ym}",
        f"정년연장 노동법 개정 채용 {ym}",
        f"AI 채용 HR테크 트렌드 {ym}",
        f"외국계 기업 한국 감원 채용 {ym}",
    ]

# ── URL 검증 ─────────────────────────────────────────────────────────────────
BAD_PATTERNS = [
    'search.naver.com', 'teamblind.com/kr/search/',
    'jobplanet.co.kr/companies?', 'jobkorea.co.kr/Search/?',
]
def is_bad_url(url: str) -> bool:
    if not url:
        return True
    try:
        p = urlparse(url)
        if p.path.rstrip('/') == '':
            return True
    except Exception:
        return True
    return any(b in url for b in BAD_PATTERNS)

# ── 1단계: Google Custom Search로 기사 수집 ───────────────────────────────────
def search_articles(queries: list) -> list:
    """Google Custom Search API로 실제 기사 URL 수집"""
    if not SEARCH_KEY or not SEARCH_CX:
        print("  ⚠️  GOOGLE_SEARCH_API_KEY 또는 GOOGLE_SEARCH_CX 미설정 — Gemini 직접 검색 모드")
        return []

    articles, seen = [], set()
    for q in queries:
        try:
            r = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={"key": SEARCH_KEY, "cx": SEARCH_CX, "q": q,
                        "num": 5, "lr": "lang_ko", "sort": "date"},
                timeout=15
            )
            r.raise_for_status()
            for item in r.json().get("items", []):
                url = item.get("link", "")
                if not url or url in seen or is_bad_url(url):
                    continue
                seen.add(url)
                articles.append({
                    "title":   item.get("title", ""),
                    "url":     url,
                    "source":  item.get("displayLink", ""),
                    "snippet": item.get("snippet", "")[:200],
                    "query":   q,
                })
            time.sleep(0.3)
        except Exception as e:
            print(f"    검색 오류 ({q[:20]}...): {e}")
    print(f"  ✅ {len(articles)}개 기사 수집")
    return articles

# ── 2단계: Gemini로 분석 ──────────────────────────────────────────────────────
ANALYZE_PROMPT = """당신은 커넥트웨이브 채용 인텔리전스 분석가입니다.

커넥트웨이브: 가격비교(다나와·에누리)·셀러커머스(메이크샵·플레이오토)·몰테일 그룹

【완전 제외】다나와·에누리닷컴·메이크샵·플레이오토·몰테일 → 절대 포함 금지

【카테고리】
  leader  = 대표/임원/CTO/CPO급 이동·퇴임
  outflow = 권고사직·희망퇴직·구조조정·채용중단
  hiring  = 공채·채용확대·투자유치 후 채용
  foreign = 해외·외국계 동향
  hr      = 채용법령·노무·급여·HR트렌드·정부지원

【신뢰도】A=공시·공식자료 / B=주요언론 / C=커뮤니티 (hr은 C 제외)

【출처 규칙】
- sources.url은 반드시 아래 [기사 목록]에 있는 실제 URL만 사용
- search.naver.com 검색 URL 절대 금지
- 출처 없는 항목 생성 금지

【분량】최소 {min_items}개 이상 필수. 기사 부족 시 hr 트렌드 항목으로 채울 것.

【인사이트·액션】소싱/컨택/운영 관점으로 구체적으로:
✅ "케이뱅크 결제/정산 백엔드 5년차 이상을 LinkedIn에서 즉시 서치. 커머스 정산 도메인 확장성 강조."
❌ "관련 인재를 확인하세요"

아래 JSON 형식으로만 출력 (마크다운 코드블록 없이 순수 JSON):
{{
  "date": "{date}",
  "summary": "70자 이내 오늘 채용시장 요약",
  "contact_targets": ["기업1", "기업2"],
  "items": [
    {{
      "id": "{date_nodash}-slug",
      "date": "{date}",
      "time": "HH:MM",
      "cat": "leader|outflow|hiring|foreign|hr",
      "priority": true,
      "urgency": "high|mid|low",
      "company": "기업명",
      "signal": "20자이내 신호",
      "level": "A|B|C",
      "title": "60자이내 제목",
      "body": "200자이내 내용 (해요체)",
      "insight": "120자이내 채용담당자 인사이트 (해요체)",
      "action": "120자이내 추천액션 (해요체, ~하세요 어미)",
      "tags": ["태그1", "태그2"],
      "sources": [{{"name": "매체명", "url": "실제URL", "level": "A|B|C"}}]
    }}
  ]
}}"""

def analyze(articles: list, date: datetime.date) -> dict:
    print(f"  🧠 Gemini 분석 중... ({len(articles)}개 기사)")
    date_str = str(date)
    date_nodash = date_str.replace("-", "")

    if articles:
        articles_json = json.dumps(articles, ensure_ascii=False, indent=1)
        prompt = (ANALYZE_PROMPT
                  .replace("{min_items}", str(MIN_ITEMS))
                  .replace("{date}", date_str)
                  .replace("{date_nodash}", date_nodash)
                  + f"\n\n[기사 목록]\n{articles_json}")
    else:
        # 기사 없을 때: Gemini가 자체 지식으로 HR 트렌드 항목 작성
        prompt = (ANALYZE_PROMPT
                  .replace("{min_items}", str(MIN_ITEMS))
                  .replace("{date}", date_str)
                  .replace("{date_nodash}", date_nodash)
                  + f"\n\n[기사 목록]\n[]"
                  + f"\n\n기사가 없어요. {date_str} 기준으로 최근 한국 채용 시장 HR 트렌드,"
                  + " 노무 실무, 정부 지원사업 등 신뢰할 수 있는 공개 정보 기반으로"
                  + f" 최소 {MIN_ITEMS}개 항목을 작성하세요."
                  + " sources에는 실제 접근 가능한 공식 URL만 사용하세요.")

    try:
        raw = gemini(prompt, max_tokens=8000)
        # 코드블록 제거
        raw = re.sub(r'^```(?:json)?\s*', '', raw.strip())
        raw = re.sub(r'\s*```$', '', raw.strip())
        report = json.loads(raw)
        report["date"] = date_str
    except Exception as e:
        print(f"  ❌ 분석 실패: {e}")
        return {"date": date_str, "summary": "분석 실패", "contact_targets": [], "items": []}

    # 그룹사 제거
    report["items"] = [
        i for i in report.get("items", [])
        if not any(g in i.get("company", "").lower().replace(" ", "") for g in CW_GROUP)
    ]
    # 검색 URL 제거
    for item in report.get("items", []):
        item["sources"] = [s for s in item.get("sources", []) if not is_bad_url(s.get("url", ""))]
    # 출처 없는 항목 제거
    report["items"] = [i for i in report["items"] if i.get("sources")]

    # 중요도 정렬
    def score(i):
        return ({"leader":90,"outflow":80,"hr":50,"hiring":40,"foreign":35}.get(i.get("cat",""),30)
                + {"high":15,"mid":7,"low":0}.get(i.get("urgency","low"),0)
                + (20 if i.get("priority") else 0)
                + {"A":10,"B":5,"C":0}.get(i.get("level","C"),0))
    report["items"].sort(key=score, reverse=True)

    print(f"  ✅ {len(report['items'])}건 확정")
    return report

# ── 전체 파이프라인 ──────────────────────────────────────────────────────────
def collect(date: datetime.date) -> dict:
    print(f"\n{'─'*48}\n📅 {date} 수집 시작")

    # 1차 검색
    queries  = core_queries(date)
    articles = search_articles(queries)
    report   = analyze(articles, date)

    # 최소 건수 미달 → 폴백
    if len(report.get("items", [])) < MIN_ITEMS:
        print(f"  ⚠️  {len(report.get('items',[]))}건 — 폴백 검색 실행...")
        extra    = search_articles(fallback_queries(date))
        seen     = {a["url"] for a in articles}
        combined = articles + [a for a in extra if a["url"] not in seen]
        report   = analyze(combined, date)
        if len(report.get("items", [])) < MIN_ITEMS:
            print(f"  ⚠️  폴백 후에도 {len(report.get('items',[]))}건")

    return report

# ── 저장 ────────────────────────────────────────────────────────────────────
def save(report: dict):
    os.makedirs("reports", exist_ok=True)
    path = f"reports/{report['date']}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    os.makedirs("web", exist_ok=True)
    try:
        with open("web/data.json", encoding="utf-8") as f:
            history = json.load(f)
    except FileNotFoundError:
        history = []
    history = [h for h in history if h.get("date") != report["date"]]
    history.insert(0, report)
    history.sort(key=lambda x: x.get("date", ""), reverse=True)
    history = history[:90]
    with open("web/data.json", "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    print(f"  💾 저장: {path} ({len(report.get('items',[]))}건)")

# ── 이메일 (선택) ─────────────────────────────────────────────────────────────
def ms_token():
    r = requests.post(
        f"https://login.microsoftonline.com/{MS_TENANT}/oauth2/v2.0/token",
        data={"grant_type":"client_credentials","client_id":MS_CLIENT,
              "client_secret":MS_SECRET,"scope":"https://graph.microsoft.com/.default"},
        timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]

def send_email(reports: list, token: str):
    if not EMAIL_TO or not EMAIL_FROM:
        return
    all_items = sorted(
        [i for r in reports for i in r.get("items", [])],
        key=lambda i: {"leader":90,"outflow":80,"hr":50,"hiring":40,"foreign":35}.get(i.get("cat",""),30),
        reverse=True
    )
    CAT = {"leader":"리더이탈","outflow":"인재유출","hiring":"채용확대","foreign":"해외","hr":"HR NEWS"}
    date_str = ", ".join(r["date"] for r in reports)
    subject  = f"[CW 채용인텔] {date_str} — {len(all_items)}건"
    rows = "".join(
        f"<tr><td style='padding:5px;font-size:11px;color:#888'>{i.get('date','')}</td>"
        f"<td style='padding:5px;font-size:11px'>{CAT.get(i.get('cat',''),'')}</td>"
        f"<td style='padding:5px;font-size:12px;font-weight:700'>{i.get('company','')}</td>"
        f"<td style='padding:5px;font-size:11px'>{i.get('title','')[:45]}</td></tr>"
        for i in all_items
    )
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
    print(f"  📧 이메일 → {EMAIL_TO}")

# ── 메인 ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 48)
    print("🚀 커넥트웨이브 채용 인텔리전스 v8 (Gemini)")
    print("=" * 48)

    target_dates = get_target_dates()
    reports = []

    for d in target_dates:
        report = collect(d)
        save(report)
        reports.append(report)
        time.sleep(2)

    # 이메일 (선택)
    if all([EMAIL_TO, EMAIL_FROM, MS_TENANT, MS_CLIENT, MS_SECRET]) and reports:
        try:
            token = ms_token()
            send_email(reports, token)
        except Exception as e:
            print(f"  ⚠️  이메일 실패: {e}")

    # 최종 요약
    print(f"\n{'='*48}")
    for r in reports:
        n    = len(r.get("items", []))
        flag = "✅" if n >= MIN_ITEMS else "⚠️ "
        print(f"{flag} {r['date']}: {n}건 {'(목표 달성)' if n >= MIN_ITEMS else f'(목표 {MIN_ITEMS}건 미달)'}")

if __name__ == "__main__":
    main()
