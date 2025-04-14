import math
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd
import requests
from flask import Flask, jsonify, request
import re # 계정 로드를 위해 추가

app = Flask(__name__)

# --- 계정 설정 로드 ---
def load_account_configs():
    accounts = {}
    i = 1
    while True:
        name_key = f"ACCOUNT_CONFIG_{i}_NAME"
        id_key = f"ACCOUNT_CONFIG_{i}_ID"
        token_key = f"ACCOUNT_CONFIG_{i}_TOKEN"

        name = os.environ.get(name_key)
        account_id = os.environ.get(id_key)
        token = os.environ.get(token_key)

        if name and account_id and token:
            # 계정 이름을 식별키로 사용
            account_key = name
            accounts[account_key] = {"id": account_id, "token": token, "name": name} # 이름도 저장
            print(f"Loaded account: {name} (ID: {account_id})")
            i += 1
        else:
            break
    if not accounts:
         print("Warning: No account configurations found in environment variables (e.g., ACCOUNT_CONFIG_1_NAME/ID/TOKEN).")
    return accounts

ACCOUNT_CONFIGS = load_account_configs()
# --- 계정 설정 로드 끝 ---

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

# --- (선택 사항) 계정 목록 제공 API ---
@app.route('/api/accounts', methods=['POST']) # POST로 비밀번호 받기
def get_accounts():
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    try:
        data = request.get_json()
        password = data.get('password')
        if not password or password != os.environ.get("REPORT_PASSWORD"):
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        # 비밀번호가 맞으면 계정 이름 목록 반환
        # ACCOUNT_CONFIGS의 key가 계정 이름임
        account_names = list(ACCOUNT_CONFIGS.keys())
        return jsonify(account_names)

    except Exception as e:
        print(f"Error getting account list: {e}")
        return jsonify({"error": "Failed to retrieve account list."}), 500
# --- 계정 목록 제공 API 끝 ---

# --- 보고서 생성 API 수정 ---
@app.route('/api/generate-report', methods=['POST'])
def generate_report():
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    try:
        data = request.get_json()

        # 비밀번호 확인 (기존과 동일)
        password = data.get('password')
        if not password or password != os.environ.get("REPORT_PASSWORD"):
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        # 날짜 처리 (기존과 동일)
        today = datetime.today()
        default_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = data.get('start_date') or default_date
        end_date = data.get('end_date') or default_date

        # --- 계정 정보 처리 수정 ---
        selected_account_key = data.get('selected_account_key') # 클라이언트에서 보낸 계정 이름
        if not selected_account_key:
            # 만약 설정된 계정이 하나 뿐이라면, 그것을 기본값으로 사용하도록 시도
            if len(ACCOUNT_CONFIGS) == 1:
                selected_account_key = list(ACCOUNT_CONFIGS.keys())[0]
                print(f"No selected_account_key provided, defaulting to the only configured account: {selected_account_key}")
            else:
                return jsonify({"error": "요청에 'selected_account_key'가 필요합니다. (사용 가능한 계정: " + ", ".join(ACCOUNT_CONFIGS.keys()) + ")"}), 400

        # 로드된 계정 설정에서 정보 가져오기
        account_config = ACCOUNT_CONFIGS.get(selected_account_key)
        if not account_config:
            return jsonify({"error": f"선택한 계정 키 '{selected_account_key}'에 대한 설정을 찾을 수 없습니다. 사용 가능한 계정: " + ", ".join(ACCOUNT_CONFIGS.keys())}), 404

        account = account_config.get('id')
        token = account_config.get('token')

        if not account or not token:
            print(f"Error: Missing ID or Token for account key '{selected_account_key}' in server configuration.")
            return jsonify({"error": "Server configuration error: Incomplete account credentials."}), 500
        # --- 계정 정보 처리 끝 ---

        ver = "v19.0" # API 버전

        print(f"Attempting to fetch data for account: {selected_account_key} (ID: {account}) from {start_date} to {end_date}")
        # 조회된 account와 token을 전달
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

# --- 나머지 함수들 (get_creative_details, fetch_and_format_facebook_ads_data 등) ---
# 아래 함수들은 파라미터로 account, token을 이미 받고 있으므로 수정 불필요
def get_creative_details(ad_id, ver, token):
    # ... (기존 로직 유지) ...
    """
    광고 ID를 사용하여 크리에이티브 상세 정보(콘텐츠 유형, 표시 URL, 대상 URL)를 가져옵니다.
    object_type 및 SHARE 유형을 고려하여 가능한 정확하게 분류합니다.
    """
    creative_details = {
        'content_type': '알 수 없음',
        'display_url': '',
        'target_url': ''
    }
    try:
        # 광고 ID로 크리에이티브 ID 가져오기
        creative_req_url = f"https://graph.facebook.com/{ver}/{ad_id}"
        creative_params = {'fields': 'creative', 'access_token': token}
        creative_response = requests.get(url=creative_req_url, params=creative_params)
        creative_response.raise_for_status()
        creative_data = creative_response.json()
        creative_id = creative_data.get('creative', {}).get('id')

        if creative_id:
            # 크리에이티브 상세 정보 가져오기
            details_req_url = f"https://graph.facebook.com/{ver}/{creative_id}"
            fields = 'object_type,image_url,thumbnail_url,video_id,effective_object_story_id,object_story_spec,instagram_permalink_url,asset_feed_spec'
            details_params = {'fields': fields, 'access_token': token}
            details_response = requests.get(url=details_req_url, params=details_params)
            details_response.raise_for_status()
            details_data = details_response.json()

            object_type = details_data.get('object_type')
            video_id = details_data.get('video_id')
            image_url = details_data.get('image_url')
            thumbnail_url = details_data.get('thumbnail_url')
            instagram_permalink_url = details_data.get('instagram_permalink_url')
            story_spec = details_data.get('object_story_spec', {})
            asset_feed_spec = details_data.get('asset_feed_spec', {})

            videos_from_feed = asset_feed_spec.get('videos', [])
            first_video = videos_from_feed[0] if videos_from_feed else {}
            feed_video_id = first_video.get('video_id')
            feed_thumbnail_url = first_video.get('thumbnail_url')

            link_data = story_spec.get('link_data', {})
            oss_image_url = link_data.get('image_url') or link_data.get('picture')
            oss_link = link_data.get('link')

            actual_video_id = video_id or feed_video_id

            # 유형 결정 로직 (이전 버전 사용 - 필요시 SHARE 처리 강화된 버전으로 교체)
            if object_type == 'VIDEO' or actual_video_id:
                creative_details['content_type'] = '동영상'
                creative_details['display_url'] = thumbnail_url or feed_thumbnail_url or image_url or ""
                if actual_video_id:
                    video_source_url = get_video_source_url(actual_video_id, ver, token)
                    creative_details['target_url'] = video_source_url if video_source_url else f"https://www.facebook.com/watch/?v={actual_video_id}"
                else:
                    creative_details['target_url'] = creative_details['display_url']
            elif object_type == 'PHOTO' or image_url or oss_image_url:
                creative_details['content_type'] = '사진'
                creative_details['display_url'] = image_url or oss_image_url or thumbnail_url or ""
                creative_details['target_url'] = creative_details['display_url']
            elif object_type == 'SHARE':
                 if videos_from_feed:
                     creative_details['content_type'] = '동영상'
                     creative_details['display_url'] = feed_thumbnail_url or thumbnail_url or ""
                     if feed_video_id:
                         video_source_url = get_video_source_url(feed_video_id, ver, token)
                         creative_details['target_url'] = video_source_url if video_source_url else f"https://www.facebook.com/watch/?v={feed_video_id}"
                     else:
                         creative_details['target_url'] = creative_details['display_url']
                 elif link_data and (link_data.get('image_hash') or link_data.get('image_url')):
                     creative_details['content_type'] = '사진'
                     creative_details['display_url'] = image_url or oss_image_url or thumbnail_url or ""
                     creative_details['target_url'] = oss_link or creative_details['display_url']
                 elif instagram_permalink_url:
                     # 썸네일이 있으면 비디오, 없으면 사진으로 가정 (heuristic)
                     creative_details['content_type'] = '동영상' if thumbnail_url else '사진'
                     creative_details['display_url'] = thumbnail_url or image_url or ""
                     creative_details['target_url'] = instagram_permalink_url
                 elif thumbnail_url: # image_url 없이 thumbnail만 있는 SHARE (비디오 추정)
                     creative_details['content_type'] = '동영상'
                     creative_details['display_url'] = thumbnail_url
                     story_id = details_data.get('effective_object_story_id')
                     # effective_object_story_id 를 사용하여 링크 생성 시도
                     if story_id and "_" in story_id:
                          creative_details['target_url'] = f"https://www.facebook.com/{story_id}" # 페이스북 링크 시도
                     else:
                          creative_details['target_url'] = thumbnail_url # 폴백
                 else: # SHARE인데 이미지도 비디오도 특정 어려움 -> 사진으로 기본 설정 또는 알수없음
                     creative_details['content_type'] = '사진' # 기본값을 사진으로 설정
                     creative_details['display_url'] = image_url or thumbnail_url or ""
                     creative_details['target_url'] = oss_link or creative_details['display_url']

    except requests.exceptions.RequestException as e:
        print(f"Error fetching creative details for ad {ad_id}: {e}")
    except Exception as e:
        print(f"Error processing creative details for ad {ad_id}: {e}")

    return creative_details


def get_video_source_url(video_id, ver, token):
    # ... (기존 로직 유지) ...
    """
    비디오 ID를 사용하여 재생 가능한 비디오 소스 URL을 가져옵니다.
    """
    try:
        video_req_url = f"https://graph.facebook.com/{ver}/{video_id}"
        video_params = {'fields': 'source', 'access_token': token}
        video_response = requests.get(url=video_req_url, params=video_params)
        video_response.raise_for_status()
        video_data = video_response.json()
        return video_data.get('source')
    except Exception as e:
        print(f"Error fetching video source for video {video_id}: {e}")
        return None


def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
    # ... (기존 로직 유지) ...
    """
    여러 광고의 크리에이티브 정보를 병렬로 가져옵니다.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_creative_details, ad_id, ver, token): ad_id for ad_id in ad_data.keys()}
        for future in as_completed(futures):
            ad_id = futures[future]
            try:
                creative_info = future.result()
            except Exception:
                creative_info = {'content_type': '오류', 'display_url': '', 'target_url': ''}
            ad_data[ad_id]['creative_details'] = creative_info


def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token):
    # ... (기존 로직 대부분 유지, account와 token을 파라미터로 받음) ...
    """
    Facebook API에서 데이터를 가져와 처리한 후 HTML 테이블과 JSON 데이터를 생성합니다.
    """
    metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc,actions'
    insights_url = f"https://graph.facebook.com/{ver}/{account}/insights" # 파라미터 account 사용
    params = {
        'fields': metrics,
        'access_token': token, # 파라미터 token 사용
        'level': 'ad',
        'time_range[since]': start_date,
        'time_range[until]': end_date,
        'use_unified_attribution_setting': 'true'
    }
    response = requests.get(url=insights_url, params=params)
    if response.status_code != 200:
        raise Exception(f"성과 데이터 불러오기 오류: {response.status_code} - {response.text}")
    data = response.json()
    records = data.get('data', [])
    ad_data = {}

    # 데이터 집계
    for record in records:
        ad_id = record.get('ad_id')
        if not ad_id: continue
        try: record["link_clicks"] = int(record.get("clicks", 0))
        except Exception: record["link_clicks"] = 0
        purchase_count = 0
        actions = record.get('actions')
        if actions and isinstance(actions, list):
            for action in actions:
                if action.get("action_type") == "purchase":
                    try: purchase_count += int(action.get("value", 0))
                    except ValueError: purchase_count += 0
        record["purchase_count"] = purchase_count
        ad_data[ad_id] = record

    # 크리에이티브 정보 병렬 가져오기 (token 전달 확인)
    fetch_creatives_parallel(ad_data, ver, token, max_workers=10)

    # DataFrame 변환 및 정리
    result_list = list(ad_data.values())
    df = pd.DataFrame(result_list)
    df['creative_details'] = df['ad_id'].map(lambda ad_id: ad_data.get(ad_id, {}).get('creative_details', {}))
    df['콘텐츠 유형'] = df['creative_details'].apply(lambda x: x.get('content_type', '알 수 없음'))
    df['display_url'] = df['creative_details'].apply(lambda x: x.get('display_url', ''))
    df['target_url'] = df['creative_details'].apply(lambda x: x.get('target_url', ''))
    df = df.drop(columns=['creative_details'])
    numeric_cols = ['spend', 'impressions', 'link_clicks', 'purchase_count']
    for col in numeric_cols:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(0).astype(int)
        else: df[col] = 0
    df['ctr_val'] = df.apply(lambda r: (r['link_clicks'] / r['impressions'] * 100) if r['impressions'] > 0 else 0, axis=1)
    df['CTR'] = df['ctr_val'].apply(lambda x: f"{round(x, 2)}%")
    df['cpc_val'] = df.apply(lambda r: (r['spend'] / r['link_clicks']) if r['link_clicks'] > 0 else 0, axis=1)
    df['CPC'] = df['cpc_val'].apply(lambda x: round(x) if pd.notna(x) else 0).astype(int)
    df['cost_per_purchase_val'] = df.apply(lambda r: (r['spend'] / r['purchase_count']) if r['purchase_count'] > 0 else 0, axis=1)
    df['구매당 비용'] = df['cost_per_purchase_val'].apply(lambda x: round(x) if pd.notna(x) else 0).astype(int)
    df = df.drop(columns=['ctr_val', 'cpc_val', 'cost_per_purchase_val', 'actions', 'clicks'])
    df = df.rename(columns={
        'ad_name': '광고명', 'campaign_name': '캠페인명', 'adset_name': '광고세트명',
        'spend': 'FB 광고비용', 'impressions': '노출', 'link_clicks': 'Click',
        'purchase_count': '구매 수'
    })

    # 합계 행 처리
    total_spend = df['FB 광고비용'].sum()
    total_impressions = df['노출'].sum()
    total_clicks = df['Click'].sum()
    total_purchases = df['구매 수'].sum()
    total_ctr_val = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
    total_ctr = f"{round(total_ctr_val, 2)}%"
    total_cpc = round(total_spend / total_clicks) if total_clicks > 0 else 0
    total_cpp = round(total_spend / total_purchases) if total_purchases > 0 else 0
    totals_row = pd.Series([
        '합계', '', '', total_spend, total_impressions, total_clicks,
        total_ctr, total_cpc, total_purchases, total_cpp,
        '', '', '', ''
    ], index=[
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click',
        'CTR', 'CPC', '구매 수', '구매당 비용', '광고 성과', '콘텐츠 유형',
        'display_url', 'target_url'
    ])
    column_order = [
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click',
        'CTR', 'CPC', '구매 수', '구매당 비용', '광고 성과', '콘텐츠 유형', 'display_url', 'target_url'
    ]
    df['광고 성과'] = ''
    df = df[column_order]
    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)

    # 테이블 정렬
    def custom_sort_key(row):
        if row['광고명'] == '합계': return -1
        cost = row.get('구매당 비용', 0)
        return float('inf') if cost == 0 else cost
    df_with_total['sort_key'] = df_with_total.apply(custom_sort_key, axis=1)
    df_sorted = df_with_total.sort_values(by='sort_key', ascending=True).drop(columns='sort_key')

    # 광고 성과 컬럼 재생성
    df_non_total = df_sorted[df_sorted['광고명'] != '합계'].copy()
    df_valid_cost = df_non_total[df_non_total['구매당 비용'] > 0].copy()
    df_rank_candidates = df_valid_cost[df_valid_cost['구매당 비용'] < 100000].sort_values(by='구매당 비용', ascending=True)
    top_indices = df_rank_candidates.head(3).index.tolist()
    def categorize_performance(row):
        if row['광고명'] == '합계': return ''
        cost = row['구매당 비용']
        if cost == 0: return ''
        if cost >= 100000: return '개선 필요!'
        if row.name in top_indices:
            rank = top_indices.index(row.name)
            if rank == 0: return '위닝 콘텐츠'
            if rank == 1: return '고성과 콘텐츠'
            if rank == 2: return '성과 콘텐츠'
        return ''
    df_sorted['광고 성과'] = df_sorted.apply(categorize_performance, axis=1)

    # HTML 테이블 생성
    def format_currency(amount): return f"{int(amount):,} ₩" if pd.notna(amount) else "0 ₩"
    def format_number(num): return f"{int(num):,}" if pd.notna(num) else "0"
    html_table = """
    <style>
    /* ... CSS 스타일 ... */
    table {border-collapse: collapse; width: 100%;}
    th, td {padding: 8px; border-bottom: 1px solid #ddd;}
    th {background-color: #f2f2f2; text-align: center; white-space: nowrap; vertical-align: middle;}
    td {text-align: right; white-space: nowrap; vertical-align: middle;}
    td:nth-child(1), td:nth-child(2), td:nth-child(3) { text-align: left; }
    td:nth-child(11), td:nth-child(12) { text-align: center; }
    tr:hover {background-color: #f5f5f5;}
    .total-row {background-color: #e6f2ff; font-weight: bold;}
    .winning-content {color: #009900; font-weight: bold;}
    .medium-performance {color: #E69900; font-weight: bold;}
    .third-performance {color: #FF9900; font-weight: bold;}
    .needs-improvement {color: #FF0000; font-weight: bold;}
    a {text-decoration: none; color: inherit;}
    img.ad-content-thumbnail {max-width:100px; max-height:100px; vertical-align: middle;}
    td.ad-content-cell { text-align: center; }
    </style>
    <table>
      <tr>
        <th>광고명</th> <th>캠페인명</th> <th>광고세트명</th> <th>FB 광고비용</th>
        <th>노출</th> <th>Click</th> <th>CTR</th> <th>CPC</th> <th>구매 수</th>
        <th>구매당 비용</th> <th>광고 성과</th> <th>콘텐츠 유형</th> <th>광고 콘텐츠</th>
      </tr>
    """
    for index, row in df_sorted.iterrows():
        row_class = 'total-row' if row['광고명'] == '합계' else ''
        performance_text = row.get('광고 성과', '')
        if performance_text == '위닝 콘텐츠': performance_class = 'winning-content'
        elif performance_text == '고성과 콘텐츠': performance_class = 'medium-performance'
        elif performance_text == '성과 콘텐츠': performance_class = 'third-performance'
        elif performance_text == '개선 필요!': performance_class = 'needs-improvement'
        else: performance_class = ''
        display_url = row.get("display_url", "")
        target_url = row.get("target_url", "")
        content_tag = ""
        if display_url:
            img_tag = f'<img src="{display_url}" class="ad-content-thumbnail" alt="광고 콘텐츠">'
            # 유효한 target_url (문자열이고 http로 시작)인 경우에만 링크 생성
            content_tag = f'<a href="{target_url}" target="_blank">{img_tag}</a>' if isinstance(target_url, str) and target_url.startswith('http') else img_tag
        elif row['광고명'] != '합계': content_tag = "-"
        html_table += f"""
        <tr class="{row_class}">
          <td>{row.get('광고명','')}</td> <td>{row.get('캠페인명','')}</td> <td>{row.get('광고세트명','')}</td>
          <td>{format_currency(row.get('FB 광고비용',0))}</td> <td>{format_number(row.get('노출',0))}</td>
          <td>{format_number(row.get('Click',0))}</td> <td>{row.get('CTR','0%')}</td>
          <td>{format_currency(row.get('CPC',0))}</td> <td>{format_number(row.get('구매 수',0))}</td>
          <td>{format_currency(row.get('구매당 비용',0))}</td> <td class="{performance_class}">{performance_text}</td>
          <td>{row.get('콘텐츠 유형','')}</td> <td class="ad-content-cell">{content_tag}</td>
        </tr>
        """
    html_table += "</table>"

    # 최종 결과 준비
    df_for_json = df_sorted.drop(columns=['display_url', 'target_url'])
    def clean_numeric(data):
        if isinstance(data, dict): return {k: clean_numeric(v) for k, v in data.items()}
        elif isinstance(data, list): return [clean_numeric(item) for item in data]
        elif isinstance(data, (float, int)):
            if math.isinf(data) or math.isnan(data): return 0
        return data
    records = df_for_json.to_dict(orient='records')
    cleaned_records = clean_numeric(records)

    return {"html_table": html_table, "data": cleaned_records}


# Flask 앱 실행 (로컬 테스트 시 주석 해제)
# if __name__ == '__main__':
#     # 로컬 테스트 시 환경 변수 로드 확인 필요
#     # 예: from dotenv import load_dotenv
#     #     load_dotenv()
#     #     ACCOUNT_CONFIGS = load_account_configs() # .env 로드 후 재확인
#     #     print(f"Loaded account configurations: {list(ACCOUNT_CONFIGS.keys())}")
#     app.run(debug=True, port=5001)
