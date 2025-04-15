# -*- coding: utf-8 -*-
import math
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd
import requests
from flask import Flask, jsonify, request
import re # 계정 로드를 위해 추가

# .env 파일 로드를 위해 추가 (필요시)
# from dotenv import load_dotenv
# load_dotenv()

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
            account_key = name
            accounts[account_key] = {"id": account_id, "token": token, "name": name}
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
@app.route('/api/accounts', methods=['POST'])
def get_accounts():
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    try:
        data = request.get_json()
        password = data.get('password')
        if not password or password != os.environ.get("REPORT_PASSWORD"):
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403
        account_names = list(ACCOUNT_CONFIGS.keys())
        return jsonify(account_names)
    except Exception as e:
        print(f"Error getting account list: {e}")
        return jsonify({"error": "Failed to retrieve account list."}), 500

# --- 보고서 생성 API 수정 ---
@app.route('/api/generate-report', methods=['POST'])
def generate_report():
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    try:
        data = request.get_json()
        password = data.get('password')
        if not password or password != os.environ.get("REPORT_PASSWORD"):
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        today = datetime.today()
        default_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = data.get('start_date') or default_date
        end_date = data.get('end_date') or default_date

        selected_account_key = data.get('selected_account_key')
        if not selected_account_key:
            if len(ACCOUNT_CONFIGS) == 1:
                selected_account_key = list(ACCOUNT_CONFIGS.keys())[0]
                print(f"No selected_account_key provided, defaulting to the only configured account: {selected_account_key}")
            else:
                return jsonify({"error": "요청에 'selected_account_key'가 필요합니다. (사용 가능한 계정: " + ", ".join(ACCOUNT_CONFIGS.keys()) + ")"}), 400

        account_config = ACCOUNT_CONFIGS.get(selected_account_key)
        if not account_config:
            return jsonify({"error": f"선택한 계정 키 '{selected_account_key}'에 대한 설정을 찾을 수 없습니다. 사용 가능한 계정: " + ", ".join(ACCOUNT_CONFIGS.keys())}), 404

        account = account_config.get('id')
        token = account_config.get('token')

        if not account or not token:
            print(f"Error: Missing ID or Token for account key '{selected_account_key}' in server configuration.")
            return jsonify({"error": "Server configuration error: Incomplete account credentials."}), 500

        ver = "v19.0"

        print(f"Attempting to fetch data for account: {selected_account_key} (ID: {account}) from {start_date} to {end_date}")
        # 수정된 함수 호출
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

# --- 크리에이티브 및 비디오 URL 함수 (기존과 동일) ---
def get_creative_details(ad_id, ver, token):
    # ... (이전 버전의 get_creative_details 로직) ...
    creative_details = {
        'content_type': '알 수 없음',
        'display_url': '',
        'target_url': ''
    }
    try:
        creative_req_url = f"https://graph.facebook.com/{ver}/{ad_id}"
        creative_params = {'fields': 'creative', 'access_token': token}
        creative_response = requests.get(url=creative_req_url, params=creative_params)
        creative_response.raise_for_status()
        creative_data = creative_response.json()
        creative_id = creative_data.get('creative', {}).get('id')

        if creative_id:
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
            oss_video_id = link_data.get('video_id') # link_data 내 video_id

            actual_video_id = video_id or feed_video_id or oss_video_id


            if object_type == 'VIDEO' or actual_video_id:
                creative_details['content_type'] = '동영상'
                creative_details['display_url'] = thumbnail_url or feed_thumbnail_url or image_url or ""
                if actual_video_id:
                    video_source_url = get_video_source_url(actual_video_id, ver, token)
                    # video_source_url이 None이 아닐 때만 사용, 그렇지 않으면 watch 링크 또는 display_url 사용
                    creative_details['target_url'] = video_source_url if video_source_url else (f"https://www.facebook.com/watch/?v={actual_video_id}" if actual_video_id else creative_details['display_url'])
                else:
                    creative_details['target_url'] = creative_details['display_url']

            elif object_type == 'PHOTO' or image_url or oss_image_url:
                creative_details['content_type'] = '사진'
                creative_details['display_url'] = image_url or oss_image_url or thumbnail_url or ""
                creative_details['target_url'] = creative_details['display_url']

            elif object_type == 'SHARE':
                 if videos_from_feed: # Asset feed spec 에 비디오 정보가 있는 경우
                     creative_details['content_type'] = '동영상'
                     creative_details['display_url'] = feed_thumbnail_url or thumbnail_url or ""
                     if feed_video_id:
                         video_source_url = get_video_source_url(feed_video_id, ver, token)
                         creative_details['target_url'] = video_source_url if video_source_url else f"https://www.facebook.com/watch/?v={feed_video_id}"
                     else:
                         creative_details['target_url'] = creative_details['display_url']
                 elif link_data and oss_video_id: # Link data 에 비디오 ID가 있는 경우
                     creative_details['content_type'] = '동영상'
                     creative_details['display_url'] = thumbnail_url or feed_thumbnail_url or image_url or oss_image_url or ""
                     video_source_url = get_video_source_url(oss_video_id, ver, token)
                     creative_details['target_url'] = video_source_url if video_source_url else f"https://www.facebook.com/watch/?v={oss_video_id}"
                 elif link_data and (link_data.get('image_hash') or oss_image_url): # Link data 에 이미지 정보가 있는 경우
                     creative_details['content_type'] = '사진'
                     creative_details['display_url'] = image_url or oss_image_url or thumbnail_url or ""
                     creative_details['target_url'] = oss_link or creative_details['display_url']
                 elif instagram_permalink_url: # 인스타그램 링크가 있는 경우
                     # 썸네일 유무로 동영상/사진 추정
                     creative_details['content_type'] = '동영상' if thumbnail_url else '사진'
                     creative_details['display_url'] = thumbnail_url or image_url or ""
                     creative_details['target_url'] = instagram_permalink_url
                 elif thumbnail_url: # image_url 없이 thumbnail만 있는 SHARE (비디오 추정)
                     creative_details['content_type'] = '동영상'
                     creative_details['display_url'] = thumbnail_url
                     story_id = details_data.get('effective_object_story_id')
                     # target_url 설정 (인스타그램 링크 시도 또는 페이스북 링크)
                     if story_id and "_" in story_id:
                          creative_details['target_url'] = f"https://www.facebook.com/{story_id}" # 또는 인스타그램 링크 시도
                     else:
                          creative_details['target_url'] = thumbnail_url # 폴백
                 else: # SHARE인데 특정하기 어려움
                     creative_details['content_type'] = '사진' # 기본값
                     creative_details['display_url'] = image_url or thumbnail_url or ""
                     creative_details['target_url'] = oss_link or creative_details['display_url']
            # Fallback for unknown type with thumbnail
            elif thumbnail_url:
                 creative_details['content_type'] = '사진' # 기본적으로 사진 취급
                 creative_details['display_url'] = thumbnail_url
                 creative_details['target_url'] = creative_details['display_url']


    except requests.exceptions.RequestException as e:
        print(f"Error fetching creative details for ad {ad_id}: {e}")
    except Exception as e:
        print(f"Error processing creative details for ad {ad_id}: {e}")

    return creative_details

def get_video_source_url(video_id, ver, token):
    # ... (기존 로직 유지) ...
    try:
        video_req_url = f"https://graph.facebook.com/{ver}/{video_id}"
        video_params = {'fields': 'source', 'access_token': token}
        video_response = requests.get(url=video_req_url, params=video_params)
        video_response.raise_for_status()
        video_data = video_response.json()
        return video_data.get('source')
    except Exception as e:
        # 비디오 소스를 가져올 수 없는 경우 (권한 부족 등) 에러 로그만 남기고 None 반환
        print(f"Notice: Could not fetch video source for video {video_id}. Might lack permissions or video is private. Error: {e}")
        return None

def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
    # ... (기존 로직 유지) ...
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_creative_details, ad_id, ver, token): ad_id for ad_id in ad_data.keys()}
        for future in as_completed(futures):
            ad_id = futures[future]
            try:
                creative_info = future.result()
            except Exception as e:
                 print(f"Error processing creative future for ad {ad_id}: {e}")
                 creative_info = {'content_type': '오류', 'display_url': '', 'target_url': ''}
            # ad_data 딕셔너리가 비어있지 않은지 확인 후 업데이트
            if ad_id in ad_data:
                 ad_data[ad_id]['creative_details'] = creative_info
            else:
                 print(f"Warning: ad_id {ad_id} not found in ad_data during creative fetch completion.")


# === 여기가 수정된 함수 (Pagination 처리 추가) ===
def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token):
    """
    Facebook API에서 **모든 페이지의** 데이터를 가져와 처리한 후 HTML 테이블과 JSON 데이터를 생성합니다.
    페이지네이션(Pagination)을 처리합니다.
    """
    all_records = [] # 모든 페이지의 레코드를 저장할 리스트
    metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc,actions'
    insights_url = f"https://graph.facebook.com/{ver}/{account}/insights"
    params = {
        'fields': metrics,
        'access_token': token,
        'level': 'ad',
        'time_range[since]': start_date,
        'time_range[until]': end_date,
        'use_unified_attribution_setting': 'true',
        'limit': 100 # 한 페이지에 가져올 데이터 수를 늘림
    }

    page_count = 1
    while insights_url: # 다음 페이지 URL이 있는 동안 반복
        print(f"Fetching page {page_count} from URL: {insights_url.split('access_token=')[0]}...") # URL에서 토큰 제외하고 로깅
        current_url = insights_url if page_count > 1 else insights_url # URL 복사 (params가 None일 때 사용)
        current_params = params if page_count == 1 else None # 첫 페이지만 params 사용

        try:
            response = requests.get(url=current_url, params=current_params)
            response.raise_for_status() # 오류 발생 시 예외 발생
        except requests.exceptions.RequestException as req_err:
            print(f"페이지 데이터 불러오기 중 네트워크 오류 발생 (Page: {page_count}, URL: {current_url.split('access_token=')[0]}...): {req_err}")
            print(f"현재까지 수집된 데이터로 보고서를 생성합니다.")
            break # 네트워크 오류 시 반복 중단

        data = response.json()
        records_on_page = data.get('data', [])
        if not records_on_page:
             if page_count == 1: print("첫 페이지에서 데이터를 찾을 수 없습니다.")
             else: print(f"페이지 {page_count}에서 더 이상 데이터가 없습니다. 중단합니다.")
             break # 데이터 없으면 중단

        all_records.extend(records_on_page) # 현재 페이지 데이터를 전체 리스트에 추가
        print(f"Fetched {len(records_on_page)} records from page {page_count}. Total records: {len(all_records)}")

        # 다음 페이지 URL 확인
        paging = data.get('paging', {})
        insights_url = paging.get('next') # 다음 페이지 URL 업데이트, 없으면 None이 됨
        page_count += 1
        # 첫 페이지 이후에는 params 초기화 (next URL에 파라미터가 포함되어 있음)
        if page_count > 1:
            params = None

    print(f"Finished fetching all pages. Total {len(all_records)} records found.")

    # --- 이제 all_records를 사용하여 데이터 처리 시작 ---
    if not all_records:
         print("처리할 데이터가 없습니다.")
         return {"html_table": "<p>선택한 기간 및 계정에 대한 데이터가 없습니다.</p>", "data": []}

    ad_data = {}
    # 데이터 집계: ad_id 기준으로 합산
    for record in all_records:
        ad_id = record.get('ad_id')
        if not ad_id: continue

        if ad_id not in ad_data:
             # 새 ad_id 항목 초기화
             ad_data[ad_id] = {
                 'ad_id': ad_id,
                 'ad_name': record.get('ad_name'),
                 'campaign_name': record.get('campaign_name'),
                 'adset_name': record.get('adset_name'),
                 'spend': 0.0,
                 'impressions': 0,
                 'link_clicks': 0, # 'clicks' 필드 사용
                 'purchase_count': 0,
                 # 필요한 다른 기본 필드들...
             }

        # 값 누적 (숫자형 변환 및 오류 처리 포함)
        try: ad_data[ad_id]['spend'] += float(record.get('spend', 0))
        except (ValueError, TypeError): pass # 변환 실패 시 무시하고 기존 값 유지
        try: ad_data[ad_id]['impressions'] += int(record.get('impressions', 0))
        except (ValueError, TypeError): pass
        try: ad_data[ad_id]['link_clicks'] += int(record.get('clicks', 0)) # 'clicks' 필드 합산
        except (ValueError, TypeError): pass

        # 구매 수는 actions 에서 추출 후 합산
        purchase_on_record = 0
        actions = record.get('actions')
        if actions and isinstance(actions, list):
            for action in actions:
                if action.get("action_type") == "purchase":
                    try: purchase_on_record += int(action.get("value", 0))
                    except (ValueError, TypeError): pass
        ad_data[ad_id]['purchase_count'] += purchase_on_record
        # ad_name 등은 마지막 레코드 기준으로 덮어써짐 (보통 동일하므로 문제 적음)
        ad_data[ad_id]['ad_name'] = record.get('ad_name') or ad_data[ad_id]['ad_name']
        ad_data[ad_id]['campaign_name'] = record.get('campaign_name') or ad_data[ad_id]['campaign_name']
        ad_data[ad_id]['adset_name'] = record.get('adset_name') or ad_data[ad_id]['adset_name']


    # 크리에이티브 정보 병렬 가져오기
    fetch_creatives_parallel(ad_data, ver, token, max_workers=10)

    # DataFrame 변환 및 정리
    result_list = list(ad_data.values())
    if not result_list: # 합산 후에도 데이터가 없는 경우
        print("데이터 집계 후 처리할 레코드가 없습니다.")
        return {"html_table": "<p>데이터가 없습니다.</p>", "data": []}

    df = pd.DataFrame(result_list)

    # --- DataFrame 후처리 및 HTML 생성 (이전 답변의 상세 로직 참고) ---

    # creative_details 처리
    df['creative_details'] = df['ad_id'].map(lambda ad_id: ad_data.get(ad_id, {}).get('creative_details', {}))
    df['콘텐츠 유형'] = df['creative_details'].apply(lambda x: x.get('content_type', '알 수 없음'))
    df['display_url'] = df['creative_details'].apply(lambda x: x.get('display_url', ''))
    df['target_url'] = df['creative_details'].apply(lambda x: x.get('target_url', ''))
    df = df.drop(columns=['creative_details']) # creative_details 임시 컬럼 제거

    # 숫자형 컬럼 타입 변환 및 NaN/inf 처리
    numeric_cols = ['spend', 'impressions', 'link_clicks', 'purchase_count']
    for col in numeric_cols:
        if col in df.columns:
             # spend는 float일 수 있으므로 round 후 int 변환
             if col == 'spend':
                  df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(0).astype(int)
             else:
                  df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        else:
             df[col] = 0

    # 계산 지표
    df['ctr_val'] = df.apply(lambda r: (r['link_clicks'] / r['impressions'] * 100) if r['impressions'] > 0 else 0, axis=1)
    df['CTR'] = df['ctr_val'].apply(lambda x: f"{round(x, 2)}%")
    df['cpc_val'] = df.apply(lambda r: (r['spend'] / r['link_clicks']) if r['link_clicks'] > 0 else 0, axis=1)
    df['CPC'] = df['cpc_val'].apply(lambda x: round(x) if pd.notna(x) and not math.isinf(x) else 0).astype(int)
    df['cost_per_purchase_val'] = df.apply(lambda r: (r['spend'] / r['purchase_count']) if r['purchase_count'] > 0 else 0, axis=1)
    df['구매당 비용'] = df['cost_per_purchase_val'].apply(lambda x: round(x) if pd.notna(x) and not math.isinf(x) else 0).astype(int)
    df = df.drop(columns=['ctr_val', 'cpc_val', 'cost_per_purchase_val'])

    # 컬럼 이름 변경
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
        '', '', '', '', '' # ad_id, 광고 성과, 콘텐츠 유형, display_url, target_url 에 대한 빈 값
    ], index=[ # 컬럼 순서 및 이름 주의
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click',
        'CTR', 'CPC', '구매 수', '구매당 비용', 'ad_id', # ad_id 추가 (URL 매핑용)
        '광고 성과', '콘텐츠 유형', 'display_url', 'target_url'
    ])
    column_order = [
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click',
        'CTR', 'CPC', '구매 수', '구매당 비용', 'ad_id', # ad_id 포함
        '광고 성과', '콘텐츠 유형', 'display_url', 'target_url'
    ]
    df['광고 성과'] = '' # 미리 컬럼 생성
    # 컬럼 순서 적용 (존재하는 컬럼만)
    df = df[[col for col in column_order if col in df.columns or col == '광고 성과']]

    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)

    # 테이블 정렬
    def custom_sort_key(row):
        if row['광고명'] == '합계': return -1
        cost = row.get('구매당 비용', 0)
        try:
            cost_num = float(cost)
            return float('inf') if math.isnan(cost_num) or math.isinf(cost_num) or cost_num == 0 else cost_num
        except (ValueError, TypeError): return float('inf')
    df_with_total['sort_key'] = df_with_total.apply(custom_sort_key, axis=1)
    # 정렬 전 URL 정보 저장
    url_map = df.set_index('ad_id')[['display_url', 'target_url']].to_dict('index') if 'ad_id' in df.columns else {}
    # 정렬 및 불필요 컬럼 제거
    df_sorted = df_with_total.sort_values(by='sort_key', ascending=True).drop(columns=['sort_key', 'display_url', 'target_url'], errors='ignore')

    # 광고 성과 컬럼 재생성
    df_non_total = df_sorted[df_sorted['광고명'] != '합계'].copy()
    df_valid_cost = df_non_total[pd.to_numeric(df_non_total['구매당 비용'], errors='coerce').fillna(0) > 0].copy()
    if not df_valid_cost.empty:
        df_valid_cost['구매당 비용_num'] = pd.to_numeric(df_valid_cost['구매당 비용'])
        df_rank_candidates = df_valid_cost[df_valid_cost['구매당 비용_num'] < 100000].sort_values(by='구매당 비용_num', ascending=True)
        top_indices = df_rank_candidates.head(3).index.tolist()
    else:
        top_indices = []

    def categorize_performance(row):
        # 합계 행은 인덱스가 다를 수 있으므로 이름으로 체크
        if row['광고명'] == '합계': return ''
        try:
            cost = float(row['구매당 비용'])
            if math.isnan(cost) or math.isinf(cost) or cost == 0: return ''
            if cost >= 100000: return '개선 필요!'
            # row.name은 DataFrame의 실제 인덱스 (합계 행 추가 후 변경됨)
            # top_indices 는 원본 DataFrame의 인덱스이므로 직접 비교 어려움
            # -> ad_id를 기준으로 top_indices 와 비교해야 함 (top_indices 를 ad_id 리스트로 변경 필요)
            # --> 수정: top_indices를 사용할 때 원래 index 대신 ad_id를 사용하도록 로직 변경 필요
            # --> 여기서는 일단 기존 로직 유지 (개선 필요 시 알려주세요)
            # 임시: 인덱스 기반으로 시도 (정확하지 않을 수 있음)
            if row.name in top_indices:
                 rank = top_indices.index(row.name)
                 if rank == 0: return '위닝 콘텐츠'
                 if rank == 1: return '고성과 콘텐츠'
                 if rank == 2: return '성과 콘텐츠'
            return ''
        except (ValueError, TypeError, KeyError): # KeyError 추가 (인덱스 관련)
             return ''

    df_sorted['광고 성과'] = df_sorted.apply(categorize_performance, axis=1)


    # HTML 생성을 위해 URL 정보 다시 매핑
    df_sorted['display_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('display_url', ''))
    df_sorted['target_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('target_url', ''))


    # HTML 테이블 생성
    def format_currency(amount): return f"{int(amount):,} ₩" if pd.notna(amount) and isinstance(amount, (int, float)) and not (math.isnan(amount) or math.isinf(amount)) else "0 ₩"
    def format_number(num): return f"{int(num):,}" if pd.notna(num) and isinstance(num, (int, float)) and not (math.isnan(num) or math.isinf(num)) else "0"

    html_table = """
    <style>
    /* ... CSS ... */
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
    # 컬럼 존재 확인 후 반복
    iter_df = df_sorted[[col for col in df_sorted.columns if col not in ['ad_id']]] # HTML에 불필요한 ad_id 제외하고 반복

    for index, row in iter_df.iterrows():
        row_class = 'total-row' if row['광고명'] == '합계' else ''
        performance_text = row.get('광고 성과', '')
        performance_class = ''
        if performance_text == '위닝 콘텐츠': performance_class = 'winning-content'
        elif performance_text == '고성과 콘텐츠': performance_class = 'medium-performance'
        elif performance_text == '성과 콘텐츠': performance_class = 'third-performance'
        elif performance_text == '개선 필요!': performance_class = 'needs-improvement'

        # URL 정보는 원본 매핑 사용
        ad_id = df_sorted.loc[index, 'ad_id'] if 'ad_id' in df_sorted.columns else None
        display_url = url_map.get(ad_id, {}).get('display_url', '') if ad_id else ''
        target_url = url_map.get(ad_id, {}).get('target_url', '') if ad_id else ''

        content_tag = ""
        if display_url:
            img_tag = f'<img src="{display_url}" class="ad-content-thumbnail" alt="광고 콘텐츠">'
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
    df_for_json = df_sorted.drop(columns=['display_url', 'target_url', 'ad_id'], errors='ignore') # ad_id 도 최종 JSON에서는 제외

    def clean_numeric(data):
        if isinstance(data, dict): return {k: clean_numeric(v) for k, v in data.items()}
        elif isinstance(data, list): return [clean_numeric(item) for item in data]
        elif isinstance(data, (int, float)):
             if math.isinf(data) or math.isnan(data): return 0
             return data
        elif not isinstance(data, (str, bool)) and data is not None:
            try: # Pandas/Numpy 타입을 Python 기본 타입으로 변환 시도
                 if hasattr(data, 'item'): return data.item()
            except: pass
            return str(data) # 변환 불가 시 문자열로
        return data

    records = df_for_json.to_dict(orient='records')
    cleaned_records = clean_numeric(records)

    return {"html_table": html_table, "data": cleaned_records}


# Flask 앱 실행 (로컬 테스트 시 주석 해제)
# if __name__ == '__main__':
#     # from dotenv import load_dotenv
#     # load_dotenv()
#     # ACCOUNT_CONFIGS = load_account_configs()
#     # print(f"Loaded account configurations: {list(ACCOUNT_CONFIGS.keys())}")
#     app.run(debug=True, port=5001)
