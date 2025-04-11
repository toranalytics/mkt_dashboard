from flask import Flask, request, jsonify
import requests
import json
import os
import traceback
import pandas as pd
import math
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
        
        # 패스워드 보호: POST 데이터에 'password' 필드가 있고, 환경 변수 REPORT_PASSWORD와 일치해야 함.
        password = data.get('password')
        if not password or password != os.environ.get("REPORT_PASSWORD"):
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        # 시작/종료 날짜 기본값 설정: 오늘의 전날 (YYYY-MM-DD)
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

        print(f"Attempting to fetch data for account: {account} from {start_date} to {end_date}")
        result = fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token)
        print("Successfully fetched and formatted data.")
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
# 광고 크리에이티브 이미지 URL 병렬 처리
# --------------------------------------------------------------------------------

def get_creative_image_url(ad_id, ver, token):
    creative_url = f"https://graph.facebook.com/{ver}/{ad_id}"
    creative_params = {
        'fields': 'creative',
        'access_token': token
    }
    creative_response = requests.get(url=creative_url, params=creative_params)
    if creative_response.status_code == 200:
        creative_data = creative_response.json()
        creative_id = creative_data.get('creative', {}).get('id')
        if creative_id:
            image_req_url = f"https://graph.facebook.com/{ver}/{creative_id}"
            image_params = {
                'fields': 'image_url,thumbnail_url,object_story_spec',
                'access_token': token
            }
            image_response = requests.get(url=image_req_url, params=image_params)
            if image_response.status_code == 200:
                image_data = image_response.json()
                image_url = image_data.get('image_url')
                if not image_url and 'object_story_spec' in image_data:
                    story_spec = image_data.get('object_story_spec', {})
                    if 'photo_data' in story_spec:
                        image_url = story_spec.get('photo_data', {}).get('image_url')
                    elif 'link_data' in story_spec and 'image_url' in story_spec.get('link_data', {}):
                        image_url = story_spec.get('link_data', {}).get('image_url')
                if not image_url:
                    image_url = image_data.get('thumbnail_url')
                return image_url
    return ""

def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for ad_id in ad_data.keys():
            futures[executor.submit(get_creative_image_url, ad_id, ver, token)] = ad_id
        for future in as_completed(futures):
            ad_id = futures[future]
            try:
                img_url = future.result()
            except Exception:
                img_url = ""
            ad_data[ad_id]['image_url'] = img_url


# --------------------------------------------------------------------------------
# 메인 함수: 구매 수와 구매당 비용 및 광고 성과 (구매당 비용 기준) 추가
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
    response = requests.get(url=insights_url, params=params)
    if response.status_code != 200:
        raise Exception(f"성과 데이터 불러오기 오류: {response.text}")
    data = response.json()
    records = data.get('data', [])
    ad_data = {}

    # 1) actions에서 구매 수 (purchase_count) 및 링크 클릭(link_clicks) 추출
    for record in records:
        ad_id = record.get('ad_id')
        if not ad_id:
            continue
        
        link_clicks = 0
        purchase_count = 0
        actions = record.get('actions')
        if actions and isinstance(actions, list):
            for action in actions:
                if action.get("action_type") == "link_click":
                    try:
                        link_clicks += int(action.get("value", 0))
                    except ValueError:
                        link_clicks += 0
                # purchase: 필요 시 여러 purchase 유형(omni_purchase 등) 추가
                if action.get("action_type") == "purchase":
                    try:
                        purchase_count += int(action.get("value", 0))
                    except ValueError:
                        purchase_count += 0
        
        record["link_clicks"] = link_clicks
        record["purchase_count"] = purchase_count

        # 중복 데이터 합산 (광고 ID별)
        if ad_id not in ad_data:
            ad_data[ad_id] = record
        else:
            for key in ['spend', 'impressions', 'link_clicks', 'purchase_count']:
                if key in record:
                    ad_data[ad_id][key] = str(
                        float(ad_data[ad_id].get(key, '0')) + float(record.get(key, '0'))
                    )

    # 2) 병렬로 크리에이티브 이미지 URL 가져오기
    fetch_creatives_parallel(ad_data, ver, token, max_workers=10)

    # 3) DataFrame 변환 및 숫자형 컬럼 정리
    result_list = list(ad_data.values())
    df = pd.DataFrame(result_list)
    for col in ['spend', 'impressions', 'link_clicks', 'purchase_count']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col]).round(0).astype(int)

    # CTR 및 CPC 계산 (링크 클릭 기준)
    if 'link_clicks' in df.columns and 'impressions' in df.columns:
        df['ctr'] = df.apply(
            lambda r: f"{round(r['link_clicks'] / r['impressions'] * 100, 2)}%" if r['impressions'] > 0 else "0%",
            axis=1
        )
    if 'spend' in df.columns and 'link_clicks' in df.columns:
        df['cpc'] = df.apply(
            lambda r: round(r['spend'] / r['link_clicks']) if r['link_clicks'] > 0 else 0,
            axis=1
        )
    # 구매당 비용 = spend / purchase_count (구매 수가 0이면 0)
    if 'purchase_count' in df.columns and 'spend' in df.columns:
        df['cost_per_purchase'] = df.apply(
            lambda r: round(r['spend'] / r['purchase_count']) if r['purchase_count'] > 0 else 0,
            axis=1
        )
    else:
        df['cost_per_purchase'] = 0

    # 컬럼명 한글화 및 재명명
    df = df.rename(columns={
        'ad_name': '광고명',
        'campaign_name': '캠페인명',
        'adset_name': '광고세트명',
        'spend': 'FB 광고비용',
        'impressions': '노출',
        'link_clicks': 'Click',
        'ctr': 'CTR',
        'cpc': 'CPC',
        'purchase_count': '구매 수',
        'cost_per_purchase': '구매당 비용'
    })

    # 4) 합계 행 처리 (전체 값 가중 평균 방식)
    total_spend = df['FB 광고비용'].sum()
    total_clicks = df['Click'].sum()
    total_impressions = df['노출'].sum()
    total_purchases = df['구매 수'].sum()
    total_ctr = round((total_clicks / total_impressions) * 100, 2) if total_impressions > 0 else 0
    total_cpc = round(total_spend / total_clicks) if total_clicks > 0 else 0
    total_cpp = round(total_spend / total_purchases) if total_purchases > 0 else 0

    totals_row = pd.Series([
        '합계', '', '', total_spend, total_impressions, total_clicks,
        f"{total_ctr}%", total_cpc, total_purchases, total_cpp, ''
    ], index=['광고명','캠페인명','광고세트명','FB 광고비용','노출','Click','CTR','CPC','구매 수','구매당 비용','image_url'])
    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)

    # 5) 테이블 정렬: 합계 행은 맨 위, 그 외는 구매당 비용 기준으로 정렬
    #    구매당 비용이 0인 경우는 매우 큰 값으로 취급해 맨 아래로 배치
    def custom_sort_key(row):
        if row['광고명'] == '합계':
            return -1
        cost = row.get('구매당 비용', 0)
        if cost == 0:
            return 999999999
        return cost
    df_with_total['sort_key'] = df_with_total.apply(custom_sort_key, axis=1)
    df_sorted = df_with_total.sort_values(by='sort_key', ascending=True).drop('sort_key', axis=1)

    # 6) 광고 성과 컬럼 재생성 (구매당 비용 기준)
    #    - 구매당 비용이 0이면 빈 값
    #    - cost >= 100000이면 "개선 필요!" (빨간색)
    #    - 그 외, 0이 아닌 구매당 비용 중 3순위 (낮은 순)만 라벨 부여:
    #         최저(0 제외): "위닝 콘텐츠" (초록색)
    #         2순위: "고성과 콘텐츠" (노랑색)
    #         3순위: "성과 콘텐츠" (주황색)
    #    - 나머지는 빈 값
    df_non_total = df_sorted[df_sorted['광고명'] != '합계']
    # 필터: 구매당 비용 > 0
    df_valid = df_non_total[df_non_total['구매당 비용'] > 0]
    # 구매당 비용 < 100000인 경우만 순위 부여; cost >=100000는 바로 "개선 필요!"
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
        # cost < 100000인 경우: 3순위만 라벨 부여 (top_indices는 구매당 비용 낮은 순)
        if row.name in top_indices:
            # top_indices[0] → 최저 → "위닝 콘텐츠", top_indices[1] → "고성과 콘텐츠", top_indices[2] → "성과 콘텐츠"
            if row.name == top_indices[0]:
                return '위닝 콘텐츠'
            elif len(top_indices) > 1 and row.name == top_indices[1]:
                return '고성과 콘텐츠'
            elif len(top_indices) > 2 and row.name == top_indices[2]:
                return '성과 콘텐츠'
        return ''
        
    df_sorted['광고 성과'] = df_sorted.apply(categorize_performance, axis=1)

    # 7) HTML 테이블 생성 (숫자 포매팅 및 광고 성과 CSS 적용)
    def format_currency(amount):
        return f"{int(amount):,} ₩"
    def format_number(num):
        return f"{int(num):,}"
    
    html_table = """
    <style>
    table {border-collapse: collapse; width: 100%;}
    th, td {padding: 8px; border-bottom: 1px solid #ddd;}
    th {background-color: #f2f2f2; text-align: center; white-space: nowrap;}
    td {text-align: right; white-space: nowrap; vertical-align: middle;}
    tr:hover {background-color: #f5f5f5;}
    .total-row {background-color: #e6f2ff; font-weight: bold;}
    .winning-content {color: #009900; font-weight: bold;}       /* 초록: 위닝 콘텐츠 */
    .medium-performance {color: #FFD700; font-weight: bold;}      /* 노랑: 고성과 콘텐츠 */
    .third-performance {color: #FF9900; font-weight: bold;}         /* 주황: 성과 콘텐츠 */
    .needs-improvement {color: #FF0000; font-weight: bold;}         /* 빨강: 개선 필요! */
    a {text-decoration: none; color: inherit;}
    </style>
    <table>
      <tr>
        <th>광고명</th>
        <th>캠페인명</th>
        <th>광고세트명</th>
        <th>FB 광고비용</th>
        <th>노출</th>
        <th>Click</th>
        <th>CTR</th>
        <th>CPC</th>
        <th>구매 수</th>
        <th>구매당 비용</th>
        <th>광고 성과</th>
        <th>광고 이미지</th>
      </tr>
    """
    for _, row in df_sorted.iterrows():
        row_class = 'total-row' if row['광고명'] == '합계' else ''
        performance_text = row.get('광고 성과', '')
        # 광고 성과에 따라 CSS 클래스 결정
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
        img_url = row.get("image_url", "")
        if pd.notna(img_url) and img_url != "":
            img_tag = f'<a href="{img_url}" target="_blank"><img src="{img_url}" style="max-width:100px; max-height:100px;"></a>'
        else:
            img_tag = ""
        html_table += f"""
        <tr class="{row_class}">
          <td>{row.get('광고명','')}</td>
          <td>{row.get('캠페인명','')}</td>
          <td>{row.get('광고세트명','')}</td>
          <td>{format_currency(row.get('FB 광고비용',0))}</td>
          <td>{format_number(row.get('노출',0))}</td>
          <td>{format_number(row.get('Click',0))}</td>
          <td>{row.get('CTR','0%')}</td>
          <td>{format_number(row.get('CPC',0))}</td>
          <td>{format_number(row.get('구매 수',0))}</td>
          <td>{format_number(row.get('구매당 비용',0))}</td>
          <td class="{performance_class}">{performance_text}</td>
          <td>{img_tag}</td>
        </tr>
        """
    html_table += "</table>"

    # Infinity/NaN 방지 클리닝
    def clean_numeric(data):
        if isinstance(data, dict):
            return {k: clean_numeric(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [clean_numeric(item) for item in data]
        elif isinstance(data, float):
            if math.isinf(data) or math.isnan(data):
                return 0
        return data

    records = df_sorted.to_dict(orient='records')
    cleaned_records = clean_numeric(records)
    return {"html_table": html_table, "data": cleaned_records}

# Flask 앱 실행 (로컬 테스트용, Vercel에서는 필요 없음)
# if __name__ == '__main__':
#    app.run(debug=True)
