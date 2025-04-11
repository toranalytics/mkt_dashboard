import math
import os
import traceback
import pandas as pd
import requests
from flask import Flask, jsonify, request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

app = Flask(__name__)

# CORS 허용
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

@app.route('/api', methods=['GET'])
def home():
    return jsonify({"message": "Facebook 광고 성과 보고서 API가 실행 중입니다."})

@app.route('/api/generate-report', methods=['POST'])
def generate_report():
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    try:
        data = request.get_json()

        # 패스워드 보호: POST 데이터의 'password'와 환경 변수 REPORT_PASSWORD 비교
        password = data.get('password')
        if not password or password != os.environ.get("REPORT_PASSWORD"):
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        # 시작/종료 날짜: 기본값은 오늘 전날 (YYYY-MM-DD)
        today = datetime.today()
        default_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = data.get('start_date') or default_date
        end_date = data.get('end_date') or default_date

        ver = "v19.0"
        account = os.environ.get("FACEBOOK_ACCOUNT_ID")
        token = os.environ.get("FACEBOOK_ACCESS_TOKEN")
        if not account or not token:
            print("Error: Facebook Account ID or Access Token not found in environment variables.")
            return jsonify({"error": "Server configuration error: Missing Facebook credentials."}), 500

        print(f"Fetching data for account: {account} from {start_date} to {end_date}")
        result = fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token)
        print("Data fetched and formatted successfully.")
        return jsonify(result)

    except requests.exceptions.RequestException as req_err:
        print(f"Error during Facebook API request: {str(req_err)}")
        return jsonify({"error": f"API request failed: {str(req_err)}"}), 500
    except KeyError as key_err:
        print(f"Error processing API response (KeyError): {str(key_err)}")
        return jsonify({"error": f"Error processing API data: {str(key_err)}"}), 500
    except Exception as e:
        error_details = traceback.format_exc()
        print(f"An unexpected error occurred: {str(e)}\nDetails:\n{error_details}")
        return jsonify({"error": "An internal server error occurred while generating the report."}), 500

# --------------------------------------------------------------------------------
# 크리에이티브 미디어 정보 가져오기 (이미지/동영상 및 콘텐츠 유형)
# --------------------------------------------------------------------------------
def get_creative_media(ad_id, ver, token):
    """
    광고 ID로부터 크리에이티브 정보를 조회하여,
    - 이미지 URL 또는 동영상 재생 URL (display_url)
    - 콘텐츠 유형: "사진" 또는 "동영상"
    를 딕셔너리로 반환합니다.
    """
    creative_details = {"url": "", "type": "사진"}
    try:
        # 1. 광고 ID로 크리에이티브 ID 가져오기
        creative_req_url = f"https://graph.facebook.com/{ver}/{ad_id}"
        creative_params = {'fields': 'creative', 'access_token': token}
        resp = requests.get(url=creative_req_url, params=creative_params)
        resp.raise_for_status()
        creative_data = resp.json()
        creative_id = creative_data.get('creative', {}).get('id')
        if not creative_id:
            return creative_details

        # 2. 크리에이티브 ID로 상세 정보 가져오기
        details_req_url = f"https://graph.facebook.com/{ver}/{creative_id}"
        fields = 'object_type,image_url,thumbnail_url,video_id,object_story_spec'
        details_params = {'fields': fields, 'access_token': token}
        resp = requests.get(url=details_req_url, params=details_params)
        resp.raise_for_status()
        details = resp.json()

        object_type = details.get('object_type')
        video_id = details.get('video_id')
        image_url = details.get('image_url')
        thumbnail_url = details.get('thumbnail_url')
        oss = details.get('object_story_spec', {})

        # 동영상이면 video_id가 있거나, object_story_spec 내 video_data가 있으면 동영상으로 판단
        if (object_type == 'VIDEO' or video_id or ('video_data' in oss)):
            creative_details["type"] = "동영상"
            # 우선 thumbnail이 있으면 표시, 없으면 image_url
            creative_details["url"] = thumbnail_url or image_url or ""
            # 동영상 소스 URL을 가져오는 로직은 추가 가능 (여기선 페이스북 Watch 링크 사용)
            if video_id:
                creative_details["url"] = f"https://www.facebook.com/watch/?v={video_id}"
        else:
            creative_details["type"] = "사진"
            creative_details["url"] = image_url or thumbnail_url or ""
    except Exception as e:
        print(f"Error fetching creative media for ad {ad_id}: {e}")
    return creative_details

def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_creative_media, ad_id, ver, token): ad_id for ad_id in ad_data.keys()}
        for future in as_completed(futures):
            ad_id = futures[future]
            try:
                creative_info = future.result()
            except Exception:
                creative_info = {"url": "", "type": "사진"}
            ad_data[ad_id]['display_url'] = creative_info["url"]
            ad_data[ad_id]['content_type'] = creative_info["type"]

# --------------------------------------------------------------------------------
# 메인 함수: 데이터 처리 및 포매팅
# --------------------------------------------------------------------------------
def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token):
    metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc,actions'
    insights_url = f"https://graph.facebook.com/{ver}/{account}/insights"
    params = {
        'fields': metrics,
        'access_token': token,
        'level': 'ad',
        'time_range[since]': start_date,
        'time_range[until]': end_date,
        'use_unified_attribution_setting': 'true'
    }
    resp = requests.get(url=insights_url, params=params)
    if resp.status_code != 200:
        raise Exception(f"성과 데이터 불러오기 오류: {resp.status_code} - {resp.text}")
    data = resp.json()
    records = data.get('data', [])
    
    # DataFrame으로 변환 전에, 각 record에서 link_clicks 및 purchase_count 계산
    for record in records:
        # actions에서 link_clicks와 purchase_count를 합산
        actions = record.get('actions')
        record["link_clicks"] = sum(int(a.get("value", 0)) for a in actions if a.get("action_type") == "link_click") if isinstance(actions, list) else 0
        record["purchase_count"] = sum(int(a.get("value", 0)) for a in actions if a.get("action_type") in ["purchase"] or (a.get("action_type") and a.get("action_type").startswith("omni_purchase"))) if isinstance(actions, list) else 0

    # DataFrame 생성
    df = pd.DataFrame(records)
    # 그룹화: 만약 동일 ad_id 여러 행 존재 시, 합산
    if 'ad_id' in df.columns:
        agg_dict = {
            'ad_name': 'first',
            'campaign_name': 'first',
            'adset_name': 'first',
            'spend': 'sum',
            'impressions': 'sum',
            'link_clicks': 'sum',
            'purchase_count': 'sum',
            'actions': 'first'  # 기타 필요 없으므로
        }
        df = df.groupby('ad_id', as_index=False).agg(agg_dict)

    # Convert numeric columns
    for col in ['spend', 'impressions', 'link_clicks', 'purchase_count']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(0).astype(int)

    # 계산: CTR, CPC, 구매당 비용
    df['CTR'] = df.apply(lambda r: f"{round(r['link_clicks'] / r['impressions'] * 100, 2)}%" if r['impressions'] > 0 else "0%", axis=1)
    df['CPC'] = df.apply(lambda r: round(r['spend'] / r['link_clicks']) if r['link_clicks'] > 0 else 0, axis=1)
    df['구매당 비용'] = df.apply(lambda r: round(r['spend'] / r['purchase_count']) if r['purchase_count'] > 0 else 0, axis=1)

    #  콘텐츠 관련: 크리에이티브 정보 추가 (병렬 처리)
    ad_data = df.set_index('ad_id').to_dict(orient='index')
    fetch_creatives_parallel(ad_data, ver, token, max_workers=10)
    # ad_data에 저장된 'display_url'와 'content_type'을 데이터프레임에 추가
    df['display_url'] = df['ad_id'].apply(lambda x: ad_data.get(x, {}).get('display_url', ''))
    df['콘텐츠 유형'] = df['ad_id'].apply(lambda x: ad_data.get(x, {}).get('content_type', '알 수 없음'))
    # "광고 콘텐츠"는 display_url; 나중에 HTML 테이블에서 사용
    df = df.rename(columns={
        'ad_name': '광고명',
        'campaign_name': '캠페인명',
        'adset_name': '광고세트명',
        'spend': 'FB 광고비용',
        'impressions': '노출',
        'link_clicks': 'Click',
        'purchase_count': '구매 수'
    })

    #  전체 합계 행 생성 (가중 평균 방식)
    total_spend = df['FB 광고비용'].sum()
    total_impressions = df['노출'].sum()
    total_clicks = df['Click'].sum()
    total_purchases = df['구매 수'].sum()
    total_ctr = f"{round((total_clicks / total_impressions * 100), 2)}%" if total_impressions > 0 else "0%"
    total_cpc = round(total_spend / total_clicks) if total_clicks > 0 else 0
    total_cpp = round(total_spend / total_purchases) if total_purchases > 0 else 0
    totals_row = pd.Series([
        '합계', '', '', total_spend, total_impressions, total_clicks,
        total_ctr, total_cpc, total_purchases, total_cpp, '', '', ''
    ], index=['광고명','캠페인명','광고세트명','FB 광고비용','노출','Click','CTR','CPC','구매 수','구매당 비용','광고 성과','콘텐츠 유형','display_url'])
    # display_url will be used to create "광고 콘텐츠" later

    #  데이터프레임에 합계 행 추가
    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)

    # 5) 테이블 정렬: "합계" 행은 맨 위, 나머지는 구매당 비용 기준(0은 최하단으로 처리)
    def custom_sort_key(row):
        if row['광고명'] == '합계':
            return -1
        cost = row.get('구매당 비용', 0)
        if cost == 0:
            return float('inf')
        return cost
    df_with_total['sort_key'] = df_with_total.apply(custom_sort_key, axis=1)
    df_sorted = df_with_total.sort_values(by='sort_key', ascending=True).drop(columns='sort_key')

    # 6) 광고 성과 재분류 (구매당 비용 기준)
    #  - 구매당 비용 0 → 빈 값 (이미 0이면 최하단)
    #  - cost >= 100000 → "개선 필요!"
    #  - 그 외, cost > 0 and < 100000: 낮은 순으로 top 3에 라벨 부여:
    #       최저: "위닝 콘텐츠" (초록)
    #       2순위: "고성과 콘텐츠" (노랑)
    #       3순위: "성과 콘텐츠" (주황)
    df_non_total = df_sorted[df_sorted['광고명'] != '합계'].copy()
    df_valid = df_non_total[df_non_total['구매당 비용'] > 0].copy()
    df_valid_low = df_valid[df_valid['구매당 비용'] < 100000].sort_values(by='구매당 비용', ascending=True)
    top_indices = df_valid_low.head(3).index.tolist()
    def categorize_performance(row):
        if row['광고명'] == '합계':
            return ''
        cost = row['구매당 비용']
        if cost == 0:
            return ''
        if cost >= 100000:
            return '개선 필요!'
        if row.name in top_indices:
            rank = top_indices.index(row.name)
            if rank == 0:
                return '위닝 콘텐츠'
            elif rank == 1:
                return '고성과 콘텐츠'
            elif rank == 2:
                return '성과 콘텐츠'
        return ''
    df_sorted['광고 성과'] = df_sorted.apply(categorize_performance, axis=1)

    # 7) HTML 테이블 생성
    #  "광고 콘텐츠"는 display_url, "콘텐츠 유형"은 그대로 df['콘텐츠 유형']
    def format_currency(amount): 
        return f"{int(amount):,} ₩" if pd.notna(amount) else "0 ₩"
    def format_number(num):
        return f"{int(num):,}" if pd.notna(num) else "0"
    html_table = """
    <style>
      table {border-collapse: collapse; width: 100%;}
      th, td {padding: 8px; border-bottom: 1px solid #ddd;}
      th {background-color: #f2f2f2; text-align: center; white-space: nowrap; vertical-align: middle;}
      td {text-align: right; white-space: nowrap; vertical-align: middle;}
      td:nth-child(1), td:nth-child(2), td:nth-child(3) { text-align: left; }
      td:nth-child(11), td:nth-child(12) { text-align: center; }
      tr:hover {background-color: #f5f5f5;}
      .total-row {background-color: #e6f2ff; font-weight: bold;}
      .winning-content {color: #009900; font-weight: bold;}       /* 초록: 위닝 콘텐츠 */
      .medium-performance {color: #E6B800; font-weight: bold;}      /* 노랑: 고성과 콘텐츠 */
      .third-performance {color: #FF9900; font-weight: bold;}         /* 주황: 성과 콘텐츠 */
      .needs-improvement {color: #FF0000; font-weight: bold;}         /* 빨강: 개선 필요! */
      a {text-decoration: none; color: inherit;}
      img.ad-content-thumbnail {max-width:100px; max-height:100px; vertical-align: middle; border-radius: 4px;}
      td.ad-content-cell { text-align: center; }
    </style>
    <table>
      <tr>
        <th>광고명</th> <th>캠페인명</th> <th>광고세트명</th> <th>FB 광고비용</th>
        <th>노출</th> <th>Click</th> <th>CTR</th> <th>CPC</th> <th>구매 수</th>
        <th>구매당 비용</th> <th>광고 성과</th> <th>콘텐츠 유형</th> <th>광고 콘텐츠</th>
      </tr>
    """
    for idx, row in df_sorted.iterrows():
        row_class = 'total-row' if row['광고명'] == '합계' else ''
        performance_text = row.get('광고 성과', '')
        if performance_text == '위닝 콘텐츠': 
            performance_class = 'winning-content'
        elif performance_text == '고성과 콘텐츠': 
            performance_class = 'medium-performance'
        elif performance_text == '성과 콘텐츠': 
            performance_class = 'third-performance'
        elif performance_text == '개선 필요!': 
            performance_class = 'needs-improvement'
        else:
            performance_class = ''
        display_url = row.get("display_url", "")
        target_url = row.get("target_url", "")
        content_cell = ""
        if display_url:
            # 광고 콘텐츠 열: 이미지 또는 동영상 썸네일을 <a> 태그로 감싸, 클릭 시 target_url 열기
            img_tag = f'<img src="{display_url}" alt="광고 콘텐츠" class="ad-content-thumbnail">'
            if target_url:
                content_cell = f'<a href="{target_url}" target="_blank">{img_tag}</a>'
            else:
                content_cell = img_tag
        elif row['광고명'] != '합계':
            content_cell = "-"
        html_table += f"""
        <tr class="{row_class}">
          <td>{row.get('광고명','')}</td>
          <td>{row.get('캠페인명','')}</td>
          <td>{row.get('광고세트명','')}</td>
          <td>{format_currency(row.get('FB 광고비용', 0))}</td>
          <td>{format_number(row.get('노출', 0))}</td>
          <td>{format_number(row.get('Click', 0))}</td>
          <td>{row.get('CTR','0%')}</td>
          <td>{format_currency(row.get('CPC', 0))}</td>
          <td>{format_number(row.get('구매 수', 0))}</td>
          <td>{format_currency(row.get('구매당 비용', 0))}</td>
          <td class="{performance_class}">{performance_text}</td>
          <td>{row.get('콘텐츠 유형','')}</td>
          <td class="ad-content-cell">{content_cell}</td>
        </tr>
        """
    html_table += "</table>"

    # 8) Infinity/NaN 방지
    def clean_numeric(data):
        if isinstance(data, dict):
            return {k: clean_numeric(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [clean_numeric(item) for item in data]
        elif isinstance(data, (float, int)):
            if math.isinf(data) or math.isnan(data):
                return 0
        return data

    records = df_sorted.to_dict(orient='records')
    cleaned_records = clean_numeric(records)
    return {"html_table": html_table, "data": cleaned_records}

# Flask 앱 실행 (로컬 테스트용; Vercel에서는 필요 없음)
# if __name__ == '__main__':
#     app.run(debug=True, port=5001)
