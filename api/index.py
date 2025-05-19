# -*- coding: utf-8 -*-
import math
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import time # 시간 로깅을 위해 추가

import pandas as pd
import requests
from flask import Flask, jsonify, request

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
            accounts[name] = {"id": account_id, "token": token, "name": name}
            i += 1
        else:
            break
    if not accounts:
        print("Warning: No account configurations found in environment variables (e.g., ACCOUNT_CONFIG_1_NAME/ID/TOKEN).")
    return accounts

ACCOUNT_CONFIGS = load_account_configs()

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

@app.route('/api', methods=['GET'])
def home():
    return jsonify({"message": "Facebook 광고 성과 보고서 API가 실행 중입니다."})

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

@app.route('/api/generate-report', methods=['POST'])
def generate_report():
    if request.method == 'OPTIONS':
        return jsonify({}), 200
    try:
        start_time_total = time.time() # 전체 요청 처리 시간 측정 시작
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

        result = fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token)
        
        end_time_total = time.time()
        print(f"[Performance] Total report generation time: {end_time_total - start_time_total:.2f} seconds")
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

# --- 크리에이티브 및 미디어 식별 함수 ---
def get_creative_details(ad_id, ver, token): #
    creative_details = {
        'content_type': '알 수 없음',
        'display_url': '',
        'target_url': ''
    }
    try:
        # Facebook Graph API v19.0 이상에서는 adcreative 엔드포인트에서 creative_id 없이 바로 필드를 요청할 수 있습니다.
        # creative_id를 얻기 위한 첫 번째 호출을 줄일 수 있는지 검토 필요. (현재 코드는 creative ID를 먼저 가져옴)
        # 문서: developers.facebook.com/docs/marketing-api/reference/ad-creative/
        # 다만, 현재 코드는 creative ID를 얻은 후 해당 ID로 상세 정보를 요청하는 방식입니다.
        # 이 구조를 유지한다면, 요청 필드를 최소화하는 것이 중요합니다.

        creative_req_url = f"https://graph.facebook.com/{ver}/{ad_id}"
        creative_params = {'fields': 'creative{id}', 'access_token': token} # creative ID만 요청
        creative_response = requests.get(url=creative_req_url, params=creative_params)
        creative_response.raise_for_status()
        creative_data = creative_response.json()
        creative_id = creative_data.get('creative', {}).get('id')

        if creative_id:
            details_req_url = f"https://graph.facebook.com/{ver}/{creative_id}"
            # 필요한 필드만 명시적으로 요청합니다.
            # 현재 코드에서 사용되는 필드: object_type, image_url, thumbnail_url, video_id, 
            # effective_object_story_id, object_story_spec, instagram_permalink_url,
            # asset_feed_spec, effective_instagram_media_id
            # 이 필드들이 모두 사용되는지 확인하고, 사용되지 않는 필드는 제거합니다.
            fields = 'object_type,image_url,thumbnail_url,video_id,effective_object_story_id,object_story_spec{link_data,video_data},instagram_permalink_url,asset_feed_spec{videos,images},effective_instagram_media_id'
            details_params = {'fields': fields, 'access_token': token}
            details_response = requests.get(url=details_req_url, params=details_params)
            details_response.raise_for_status()
            details_data = details_response.json()

            # --- Instagram media_id 우선 처리 ---
            effective_instagram_media_id = details_data.get('effective_instagram_media_id')
            if effective_instagram_media_id:
                ig_api_url = f"https://graph.facebook.com/v22.0/{effective_instagram_media_id}" # API 버전을 최신으로 유지하거나 프로젝트 버전에 맞춤
                # 여기도 필요한 필드만 요청합니다: media_url, media_type, permalink, thumbnail_url
                ig_params = {
                    'fields': 'media_url,media_type,permalink,thumbnail_url',
                    'access_token': token
                }
                ig_resp = requests.get(ig_api_url, params=ig_params)
                ig_resp.raise_for_status()
                ig_data = ig_resp.json()
                media_url = ig_data.get('media_url')
                media_type = ig_data.get('media_type')
                permalink = ig_data.get('permalink')
                thumbnail_url = ig_data.get('thumbnail_url', media_url)

                if media_type == 'VIDEO':
                    creative_details['content_type'] = '동영상'
                elif media_type == 'IMAGE':
                    creative_details['content_type'] = '사진'
                else:
                    creative_details['content_type'] = media_type or '알 수 없음'

                creative_details['display_url'] = thumbnail_url or media_url or ""
                creative_details['target_url'] = media_url or "" 
                return creative_details

            # --- Facebook 광고 로직 (기존과 동일) ---
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
            oss_video_id = link_data.get('video_id') # link_data에서 video_id 가져오기
            
            # object_story_spec.video_data 에서도 video_id 가져오기 시도
            if not oss_video_id and 'video_data' in story_spec:
                 oss_video_id = story_spec.get('video_data', {}).get('video_id')


            actual_video_id = video_id or feed_video_id or oss_video_id

            if object_type == 'VIDEO' or actual_video_id:
                creative_details['content_type'] = '동영상'
                creative_details['display_url'] = thumbnail_url or feed_thumbnail_url or image_url or ""
                if actual_video_id:
                    video_source_url = get_video_source_url(actual_video_id, ver, token)
                    creative_details['target_url'] = video_source_url if video_source_url else (f"https://www.facebook.com/watch/?v={actual_video_id}" if actual_video_id else creative_details['display_url'])
                else:
                    creative_details['target_url'] = creative_details['display_url']

            elif object_type == 'PHOTO' or image_url or oss_image_url:
                creative_details['content_type'] = '사진'
                creative_details['display_url'] = image_url or oss_image_url or thumbnail_url or ""
                creative_details['target_url'] = creative_details['display_url']

            elif object_type == 'SHARE':
                if videos_from_feed: # asset_feed_spec.videos 가 우선
                    creative_details['content_type'] = '동영상'
                    creative_details['display_url'] = feed_thumbnail_url or thumbnail_url or ""
                    if feed_video_id:
                        video_source_url = get_video_source_url(feed_video_id, ver, token)
                        creative_details['target_url'] = video_source_url if video_source_url else f"https://www.facebook.com/watch/?v={feed_video_id}"
                    else:
                        creative_details['target_url'] = creative_details['display_url']
                elif link_data and oss_video_id: # 그 다음 object_story_spec.link_data.video_id
                    creative_details['content_type'] = '동영상'
                    creative_details['display_url'] = thumbnail_url or feed_thumbnail_url or image_url or oss_image_url or ""
                    video_source_url = get_video_source_url(oss_video_id, ver, token)
                    creative_details['target_url'] = video_source_url if video_source_url else f"https://www.facebook.com/watch/?v={oss_video_id}"
                elif link_data and (link_data.get('image_hash') or oss_image_url): # 그 다음 object_story_spec.link_data 이미지
                    creative_details['content_type'] = '사진'
                    creative_details['display_url'] = image_url or oss_image_url or thumbnail_url or ""
                    creative_details['target_url'] = oss_link or creative_details['display_url']
                elif instagram_permalink_url: # 인스타그램 퍼머링크
                    creative_details['content_type'] = '동영상' if thumbnail_url else '사진' # 썸네일 유무로 판단
                    creative_details['display_url'] = thumbnail_url or image_url or ""
                    creative_details['target_url'] = instagram_permalink_url
                elif thumbnail_url: # 썸네일이 있는 경우 (가장 일반적인 케이스)
                    creative_details['content_type'] = '사진'
                    creative_details['display_url'] = thumbnail_url
                    story_id = details_data.get('effective_object_story_id')
                    if story_id and "_" in story_id:
                        creative_details['target_url'] = f"https://www.facebook.com/{story_id.replace('_', '/posts/')}" # 좀 더 정확한 URL
                    else:
                        creative_details['target_url'] = thumbnail_url # Fallback
                else: # 위 모든 조건에 해당하지 않는 SHARE (최후의 보루)
                    creative_details['content_type'] = '사진' # 기본값
                    creative_details['display_url'] = image_url or thumbnail_url or ""
                    creative_details['target_url'] = oss_link or creative_details['display_url']
            elif thumbnail_url: # object_type 이 명확하지 않으나 썸네일이 있는 경우
                creative_details['content_type'] = '사진'
                creative_details['display_url'] = thumbnail_url
                creative_details['target_url'] = creative_details['display_url']


    except requests.exceptions.RequestException as e:
        print(f"Error fetching creative details for ad {ad_id}: {e}")
    except Exception as e:
        print(f"Error processing creative details for ad {ad_id}: {e}")
        # traceback.print_exc() # 디버깅 시 상세 오류 출력

    return creative_details


def get_video_source_url(video_id, ver, token): #
    try:
        video_req_url = f"https://graph.facebook.com/{ver}/{video_id}"
        video_params = {'fields': 'source', 'access_token': token} # 'source' 필드만 요청
        video_response = requests.get(url=video_req_url, params=video_params)
        video_response.raise_for_status()
        video_data = video_response.json()
        return video_data.get('source')
    except Exception as e:
        print(f"Notice: Could not fetch video source for video {video_id}. Might lack permissions or video is private. Error: {e}")
        return None

def fetch_creatives_parallel(ad_ids_with_spend, ver, token, max_workers=10): #
    # ad_ids_with_spend: 지출이 있는 광고 ID 리스트를 받도록 수정
    creatives_data = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 지출이 있는 ad_id에 대해서만 크리에이티브 정보 요청
        futures = {executor.submit(get_creative_details, ad_id, ver, token): ad_id for ad_id in ad_ids_with_spend}
        for future in as_completed(futures):
            ad_id = futures[future]
            try:
                creative_info = future.result()
                creatives_data[ad_id] = creative_info
            except Exception as e:
                print(f"Error processing creative future for ad {ad_id}: {e}")
                creatives_data[ad_id] = {'content_type': '오류', 'display_url': '', 'target_url': ''}
    return creatives_data


def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token): #
    s_time_func = time.time()
    all_records = []
    # metrics 필드에서 actions 필드는 다양한 하위 유형을 가질 수 있어 응답이 커질 수 있음.
    # 필요한 action_type만 명시적으로 요청하는 것을 고려 (예: 'actions{action_type,value}')
    # 현재는 'purchase'만 사용하므로, 'actions.action_type(purchase)' 와 같이 필터링 가능 여부 확인 필요
    # (FB API 문서 참조: filtering on subfields)
    # 일단 현재 구조 유지하되, action 처리 부분에서 필요한 action만 추출하도록 함.
    metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc,actions' #
    insights_url = f"https://graph.facebook.com/{ver}/{account}/insights"
    params = {
        'fields': metrics,
        'access_token': token,
        'level': 'ad',
        'time_range[since]': start_date,
        'time_range[until]': end_date,
        'use_unified_attribution_setting': 'true', # 권장 설정
        'limit': 20 # 페이지당 요청 레코드 수를 약간 늘려 API 호출 횟수 줄이기 시도 (최대 500 또는 1000, 테스트 필요)
    }

    s_time_insights = time.time()
    page_count = 1
    while insights_url:
        s_time_page = time.time()
        current_url = insights_url if page_count > 1 else insights_url
        current_params = params if page_count == 1 else None # 두 번째 페이지부터는 next url에 파라미터 포함됨
        try:
            response = requests.get(url=current_url, params=current_params)
            response.raise_for_status()
        except requests.exceptions.RequestException as req_err:
            print(f"페이지 데이터 불러오기 중 네트워크 오류 발생 (Page: {page_count}, URL: {current_url.split('access_token=')[0]}...): {req_err}")
            print(f"현재까지 수집된 데이터로 보고서를 생성합니다.")
            break

        data = response.json()
        records_on_page = data.get('data', [])
        if not records_on_page:
            if page_count == 1: print("첫 페이지에서 데이터를 찾을 수 없습니다.")
            else: print(f"페이지 {page_count}에서 더 이상 데이터가 없습니다. 중단합니다.")
            break

        all_records.extend(records_on_page)
        e_time_page = time.time()
        print(f"[Performance] Fetched {len(records_on_page)} records from page {page_count} in {e_time_page - s_time_page:.2f}s. Total records: {len(all_records)}")

        paging = data.get('paging', {})
        insights_url = paging.get('next') # 다음 페이지 URL 사용
        page_count += 1
        # if page_count > 1: params = None # 이미 위에서 처리

    e_time_insights = time.time()
    print(f"[Performance] Finished fetching all insights ({len(all_records)} records) in {e_time_insights - s_time_insights:.2f} seconds.")

    if not all_records:
        print("처리할 데이터가 없습니다.")
        return {"html_table": "<p>선택한 기간 및 계정에 대한 데이터가 없습니다.</p>", "data": []}

    s_time_process_records = time.time()
    ad_data = {}
    ad_ids_with_spend = set() # 지출이 있는 광고 ID만 수집 (크리에이티브 요청 대상)

    for record in all_records:
        ad_id = record.get('ad_id')
        if not ad_id:
            continue

        spend = float(record.get('spend', 0) or 0)
        if spend == 0: # 광고비용(Spend)이 0이면 크리에이티브 요청/집계 모두 제외
            continue
        
        ad_ids_with_spend.add(ad_id)

        if ad_id not in ad_data:
            ad_data[ad_id] = {
                'ad_id': ad_id,
                'ad_name': record.get('ad_name'),
                'campaign_name': record.get('campaign_name'),
                'adset_name': record.get('adset_name'),
                'spend': 0.0,
                'impressions': 0,
                'link_clicks': 0, # 'clicks' 필드는 다양한 유형의 클릭을 포함할 수 있음. 'link_clicks'가 더 정확할 수 있으나 API에서 해당 필드를 지원하는지 확인 필요. 현재는 'clicks' 사용.
                'purchase_count': 0,
            }

        ad_data[ad_id]['spend'] += spend
        try: ad_data[ad_id]['impressions'] += int(record.get('impressions', 0))
        except (ValueError, TypeError): pass
        try: ad_data[ad_id]['link_clicks'] += int(record.get('clicks', 0)) # API에서 'clicks'가 어떤 클릭을 의미하는지 명확히 할 필요 (예: link_click, outbound_click 등)
        except (ValueError, TypeError): pass
        
        # 'actions' 필드에서 'purchase' 값 집계
        purchase_on_record = 0
        actions = record.get('actions')
        if actions and isinstance(actions, list):
            for action in actions:
                if action.get("action_type") == "purchase": #
                    try: purchase_on_record += int(action.get("value", 0)) # 'value'는 구매 건수일 수도 있고, 구매 금액일 수도 있음. API 응답 확인 필요. 여기서는 건수로 가정.
                    except (ValueError, TypeError): pass
        ad_data[ad_id]['purchase_count'] += purchase_on_record
        
        # 이름 필드는 최신 레코드로 덮어쓰거나, 최초 발견된 이름을 유지할 수 있음. 현재는 덮어쓰기.
        ad_data[ad_id]['ad_name'] = record.get('ad_name') or ad_data[ad_id]['ad_name']
        ad_data[ad_id]['campaign_name'] = record.get('campaign_name') or ad_data[ad_id]['campaign_name']
        ad_data[ad_id]['adset_name'] = record.get('adset_name') or ad_data[ad_id]['adset_name']
    
    e_time_process_records = time.time()
    print(f"[Performance] Processing {len(all_records)} records into ad_data dict took {e_time_process_records - s_time_process_records:.2f} seconds. Unique ads with spend: {len(ad_ids_with_spend)}")


    if not ad_data: # 지출이 있는 광고가 없는 경우
        print("데이터 집계 후 처리할 레코드가 없습니다 (지출이 있는 광고 없음).")
        return {"html_table": "<p>데이터가 없습니다.</p>", "data": []}

    s_time_fetch_creatives = time.time()
    # fetch_creatives_parallel 함수를 호출하여 ad_data에 creative_details를 직접 추가하는 대신,
    # 별도의 creatives_map을 받고 나중에 DataFrame에 병합하는 방식도 고려 가능.
    # 현재는 ad_data (이제는 DataFrame)를 직접 수정하도록 함.
    creative_info_map = fetch_creatives_parallel(list(ad_ids_with_spend), ver, token, max_workers=15) # max_workers 수 조절 가능
    e_time_fetch_creatives = time.time()
    print(f"[Performance] Fetching creatives for {len(ad_ids_with_spend)} ads took {e_time_fetch_creatives - s_time_fetch_creatives:.2f} seconds.")

    s_time_df_creation = time.time()
    # DataFrame 생성
    result_list = list(ad_data.values()) # ad_data 딕셔너리에서 값들을 리스트로 변환
    if not result_list:
        print("데이터 집계 후 처리할 레코드가 없습니다.")
        return {"html_table": "<p>데이터가 없습니다.</p>", "data": []}

    df = pd.DataFrame(result_list)
    
    # 크리에이티브 정보 병합
    df['creative_details'] = df['ad_id'].map(lambda ad_id: creative_info_map.get(ad_id, {}))
    df['콘텐츠 유형'] = df['creative_details'].apply(lambda x: x.get('content_type', '알 수 없음'))
    df['display_url'] = df['creative_details'].apply(lambda x: x.get('display_url', ''))
    df['target_url'] = df['creative_details'].apply(lambda x: x.get('target_url', ''))
    df = df.drop(columns=['creative_details']) # 임시 컬럼 삭제

    # 숫자형 컬럼 타입 변환 및 결측치 처리
    numeric_cols = ['spend', 'impressions', 'link_clicks', 'purchase_count']
    for col in numeric_cols:
        if col in df.columns:
            if col == 'spend':
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(0).astype(int)
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        else:
            df[col] = 0 # 컬럼이 없는 경우 0으로 채움

    # 계산 필드 추가 (벡터화 연산으로 변경 시도)
    df['CTR_val'] = 0.0
    df.loc[df['impressions'] > 0, 'CTR_val'] = (df['link_clicks'] / df['impressions'] * 100)
    df['CTR'] = df['CTR_val'].round(2).astype(str) + '%'
    
    df['CPC_val'] = 0
    df.loc[df['link_clicks'] > 0, 'CPC_val'] = (df['spend'] / df['link_clicks'])
    df['CPC'] = df['CPC_val'].round(0).astype(int)

    df['CVR_val'] = 0.0
    df.loc[df['link_clicks'] > 0, 'CVR_val'] = (df['purchase_count'] / df['link_clicks'] * 100)
    df['CVR'] = df['CVR_val'].round(2).astype(str) + '%'
    
    df['구매당 비용_val'] = 0
    df.loc[df['purchase_count'] > 0, '구매당 비용_val'] = (df['spend'] / df['purchase_count'])
    df['구매당 비용'] = df['구매당 비용_val'].round(0).astype(int)
    
    df = df.drop(columns=['CTR_val', 'CPC_val', 'CVR_val', '구매당 비용_val']) # 임시 계산 컬럼 삭제

    df = df.rename(columns={
        'ad_name': '소재명', 'campaign_name': '캠페인명', 'adset_name': '광고세트명',
        'spend': 'FB 광고비용', 'impressions': '노출', 'link_clicks': 'Click',
        'purchase_count': '구매 수'
    })

    column_order = [
        '캠페인명', '광고세트명', '소재명', 'FB 광고비용', '노출', 'Click', 'CTR', 'CPC', 'CVR',
        '구매 수', '구매당 비용', 'ad_id', '광고 성과', '콘텐츠 유형', 'display_url', 'target_url'
    ]
    for col in column_order:
        if col not in df.columns:
            df[col] = "" # 없는 컬럼 초기화

    df = df[column_order] # 컬럼 순서 재정렬
    e_time_df_creation = time.time()
    print(f"[Performance] DataFrame creation and initial processing took {e_time_df_creation - s_time_df_creation:.2f} seconds.")

    s_time_df_aggregation_sort = time.time()
    # 합계 행 계산
    total_spend = df['FB 광고비용'].sum()
    total_impressions = df['노출'].sum()
    total_clicks = df['Click'].sum()
    total_purchases = df['구매 수'].sum()
    total_ctr_val = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
    total_ctr = f"{round(total_ctr_val, 2)}%"
    total_cpc = round(total_spend / total_clicks) if total_clicks > 0 else 0
    total_cvr = f"{round((total_purchases / total_clicks * 100), 2)}%" if total_clicks > 0 else "0%"
    total_cpp = round(total_spend / total_purchases) if total_purchases > 0 else 0
    totals_row_data = {
        '캠페인명': '', '광고세트명': '', '소재명': '합계', 'FB 광고비용': total_spend,
        '노출': total_impressions, 'Click': total_clicks, 'CTR': total_ctr,
        'CPC': total_cpc, 'CVR': total_cvr, '구매 수': total_purchases,
        '구매당 비용': total_cpp, 'ad_id': '', '광고 성과': '', '콘텐츠 유형': '',
        'display_url': '', 'target_url': ''
    }
    totals_row = pd.Series(totals_row_data, index=column_order)

    df['광고 성과'] = '' # 초기화
    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)


    # 정렬 키 계산 (구매당 비용이 숫자형이어야 함)
    # '구매당 비용' 컬럼을 정렬 전에 float으로 변환 (문자열 '%' 제거 등 처리 필요)
    # -> 이미 위에서 숫자형 '구매당 비용' 컬럼으로 계산했으므로, 이를 사용
    # -> 다만, 합계 행 추가 후 정렬하므로, 합계 행의 구매당 비용도 숫자여야 함.
    # -> 또는 정렬 키 생성 시 합계행은 별도 처리
    
    # 구매당 비용을 숫자형으로 변환 (이미 위에서 int로 변환했음)
    # df_with_total['구매당 비용_num_for_sort'] = pd.to_numeric(df_with_total['구매당 비용'], errors='coerce').fillna(float('inf'))
    # df_with_total.loc[df_with_total['소재명'] == '합계', '구매당 비용_num_for_sort'] = -1 # 합계 행을 맨 위로
    
    # custom_sort_key 함수를 사용하여 정렬
    def custom_sort_key(row): #
        if row['소재명'] == '합계': return -1 
        cost = row.get('구매당 비용', 0) # 이미 숫자형
        try:
            cost_num = float(cost)
            return float('inf') if math.isnan(cost_num) or math.isinf(cost_num) or cost_num == 0 else cost_num
        except (ValueError, TypeError): return float('inf')

    df_with_total['sort_key'] = df_with_total.apply(custom_sort_key, axis=1)
    
    # display_url, target_url 정보를 ad_id 기준으로 매핑 (정렬 후에도 유지하기 위함)
    url_map = df.set_index('ad_id')[['display_url', 'target_url']].to_dict('index') if 'ad_id' in df.columns and not df.empty else {}
    
    df_sorted = df_with_total.sort_values(by='sort_key', ascending=True).drop(columns=['sort_key'])
    # display_url, target_url은 이미 df_sorted에 포함되어 있음 (concat 시점에)


    # 광고 성과 분류
    df_non_total = df_sorted[df_sorted['소재명'] != '합계'].copy()
    # '구매당 비용'이 숫자형인지 확인 (이미 숫자형임)
    df_valid_cost = df_non_total[pd.to_numeric(df_non_total['구매당 비용'], errors='coerce').fillna(0) > 0].copy()

    top_indices = []
    if not df_valid_cost.empty:
        # 구매당 비용이 숫자형인지 확인
        df_valid_cost['구매당 비용_num'] = pd.to_numeric(df_valid_cost['구매당 비용'])
        # 구매당 비용이 100,000 미만인 후보군 중 상위 3개 선정
        df_rank_candidates = df_valid_cost[df_valid_cost['구매당 비용_num'] < 100000].sort_values(by='구매당 비용_num', ascending=True)
        top_indices = df_rank_candidates.head(3).index.tolist()


    def categorize_performance(row): #
        if row['소재명'] == '합계': return ''
        try:
            cost = float(row['구매당 비용']) # 이미 숫자형이지만, 안전하게 float 변환
            if math.isnan(cost) or math.isinf(cost) or cost == 0: return '' # 구매당 비용이 0 또는 유효하지 않으면 성과 없음
            if cost >= 100000: return '개선 필요!'
            
            # row.name은 concat 후의 인덱스이므로, top_indices의 인덱스와 일치하는지 확인
            if row.name in top_indices:
                rank = top_indices.index(row.name)
                if rank == 0: return '위닝 콘텐츠'
                if rank == 1: return '고성과 콘텐츠'
                if rank == 2: return '성과 콘텐츠'
            return '' # 위 조건에 해당하지 않으면 빈 문자열
        except (ValueError, TypeError, KeyError):
            return ''

    df_sorted['광고 성과'] = df_sorted.apply(categorize_performance, axis=1)
    
    # display_url, target_url 복원 (url_map 사용, 만약 정렬 시 유실되었다면)
    # 현재 로직에서는 df_sorted 생성 시 이미 포함되어 있음. 확인차 남겨둠.
    # df_sorted['display_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('display_url', ''))
    # df_sorted['target_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('target_url', ''))


    e_time_df_aggregation_sort = time.time()
    print(f"[Performance] DataFrame aggregation, sorting, and performance categorization took {e_time_df_aggregation_sort - s_time_df_aggregation_sort:.2f} seconds.")


    s_time_html_render = time.time()
    # HTML 테이블 생성
    def format_currency(amount): return f"{int(amount):,} ₩" if pd.notna(amount) and isinstance(amount, (int, float)) and not (math.isnan(amount) or math.isinf(amount)) else "0 ₩"
    def format_number(num): return f"{int(num):,}" if pd.notna(num) and isinstance(num, (int, float)) and not (math.isnan(num) or math.isinf(num)) else "0"

    # HTML 테이블 생성 시 필요한 컬럼만 선택
    # 'ad_id', 'display_url', 'target_url'은 HTML 생성에 직접 사용되지 않지만, 
    # '광고 콘텐츠' 셀 생성 시 df_sorted 에서 가져와 사용
    iter_df = df_sorted # 모든 필요한 컬럼이 df_sorted에 있어야 함

    html_table_rows = []
    header_row = """
      <tr>
        <th>캠페인명</th> <th>광고세트명</th> <th>소재명</th> <th>FB 광고비용</th>
        <th>노출</th> <th>Click</th> <th>CTR</th> <th>CPC</th> <th>CVR</th>
        <th>구매 수</th> <th>구매당 비용</th> <th>광고 성과</th> <th>콘텐츠 유형</th> <th>광고 콘텐츠</th>
      </tr>
    """
    html_table_rows.append(header_row)

    for index, row in iter_df.iterrows():
        row_class = 'total-row' if row['소재명'] == '합계' else ''
        performance_text = row.get('광고 성과', '')
        performance_class = ''
        if performance_text == '위닝 콘텐츠': performance_class = 'winning-content'
        elif performance_text == '고성과 콘텐츠': performance_class = 'medium-performance'
        elif performance_text == '성과 콘텐츠': performance_class = 'third-performance'
        elif performance_text == '개선 필요!': performance_class = 'needs-improvement'

        # 광고 콘텐츠 태그 생성 (display_url, target_url 사용)
        # df_sorted에서 직접 가져오도록 수정
        display_url = row.get('display_url', '')
        target_url = row.get('target_url', '')
        
        content_tag = ""
        if display_url:
            img_tag = f'<img src="{display_url}" class="ad-content-thumbnail" alt="광고 콘텐츠">'
            content_tag = f'<a href="{target_url}" target="_blank">{img_tag}</a>' if isinstance(target_url, str) and target_url.startswith('http') else img_tag
        elif row['소재명'] != '합계': content_tag = "-"


        html_table_rows.append(f"""
        <tr class="{row_class}">
          <td>{row.get('캠페인명','')}</td> <td>{row.get('광고세트명','')}</td> <td>{row.get('소재명','')}</td>
          <td>{format_currency(row.get('FB 광고비용',0))}</td> <td>{format_number(row.get('노출',0))}</td>
          <td>{format_number(row.get('Click',0))}</td> <td>{row.get('CTR','0%')}</td>
          <td>{format_currency(row.get('CPC',0))}</td> <td>{row.get('CVR','0%')}</td>
          <td>{format_number(row.get('구매 수',0))}</td> <td>{format_currency(row.get('구매당 비용',0))}</td>
          <td class="{performance_class}">{performance_text}</td>
          <td>{row.get('콘텐츠 유형','')}</td> <td class="ad-content-cell">{content_tag}</td>
        </tr>
        """)
    
    # CSS 스타일은 외부 파일 또는 HTML 템플릿에 정의하는 것이 좋지만, 현재 구조 유지
    html_table_full = f"""
    <style>
    table {{border-collapse: collapse; width: 100%;}}
    th, td {{padding: 8px; border-bottom: 1px solid #ddd;}}
    th {{background-color: #f2f2f2; text-align: center; white-space: nowrap; vertical-align: middle;}}
    td {{text-align: right; white-space: nowrap; vertical-align: middle;}}
    td:nth-child(1), td:nth-child(2), td:nth-child(3) {{ text-align: left; }}
    td:nth-child(12), td:nth-child(13) {{ text-align: center; }} /* 광고 성과, 콘텐츠 유형 */
    tr:hover {{background-color: #f5f5f5;}}
    .total-row {{background-color: #e6f2ff; font-weight: bold;}}
    .winning-content {{color: #009900; font-weight: bold;}}
    .medium-performance {{color: #E69900; font-weight: bold;}}
    .third-performance {{color: #FF9900; font-weight: bold;}}
    .needs-improvement {{color: #FF0000; font-weight: bold;}}
    a {{text-decoration: none; color: inherit;}}
    img.ad-content-thumbnail {{max-width:100px; max-height:100px; vertical-align: middle; border-radius: 6px; box-shadow: 0 2px 8px rgba(0,0,0,0.07);}}
    td.ad-content-cell {{ text-align: center; }}
    </style>
    <table>
      {''.join(html_table_rows)}
    </table>
    """
    e_time_html_render = time.time()
    print(f"[Performance] HTML table rendering took {e_time_html_render - s_time_html_render:.2f} seconds.")


    # JSON 반환용 데이터 준비 (ad_id, display_url, target_url 제외)
    df_for_json = df_sorted.drop(columns=['ad_id', 'display_url', 'target_url'], errors='ignore')

    def clean_numeric(data): #
        if isinstance(data, dict): return {k: clean_numeric(v) for k, v in data.items()}
        elif isinstance(data, list): return [clean_numeric(item) for item in data]
        elif isinstance(data, (int, float)):
            if math.isinf(data) or math.isnan(data): return 0 # 또는 None이나 적절한 값
            return data
        elif not isinstance(data, (str, bool)) and data is not None: # 추가: data가 None이 아닌 경우만 처리
            try: 
                if hasattr(data, 'item'): return data.item() # NumPy type 처리
            except: pass # 실패 시 문자열로 변환
            return str(data) # 그 외 타입은 문자열로
        return data

    records = df_for_json.to_dict(orient='records')
    cleaned_records = clean_numeric(records)
    
    e_time_func = time.time()
    print(f"[Performance] fetch_and_format_facebook_ads_data function total time: {e_time_func - s_time_func:.2f} seconds.")

    return {"html_table": html_table_full, "data": cleaned_records}

# Flask 앱 실행 (로컬 테스트 시 주석 해제)
# if __name__ == '__main__':
#     # 로컬 테스트를 위한 환경 변수 설정 예시
#     os.environ["REPORT_PASSWORD"] = "test_password"
#     os.environ["ACCOUNT_CONFIG_1_NAME"] = "TestAccount"
#     os.environ["ACCOUNT_CONFIG_1_ID"] = "act_your_account_id" # 실제 테스트용 계정 ID
#     os.environ["ACCOUNT_CONFIG_1_TOKEN"] = "your_access_token" # 실제 테스트용 액세스 토큰
#     ACCOUNT_CONFIGS = load_account_configs() # 환경변수 로드 후 재할당
#     app.run(debug=True, port=5001)
