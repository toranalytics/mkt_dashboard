import math 
import os
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import pandas as pd
import requests
from flask import Flask, jsonify, request

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
        password = data.get('password')
        if not password or password != os.environ.get("REPORT_PASSWORD"):
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

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

def get_creative_details(ad_id, ver, token):
    creative_details = {
        'content_type': '알 수 없음',
        'display_url': '',
        'target_url': ''
    }
    try:
        creative_req_url = f"https://graph.facebook.com/{ver}/{ad_id}"
        creative_params = {
            'fields': 'creative',
            'access_token': token
        }
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
            actual_video_id = video_id or feed_video_id

            if object_type == 'VIDEO' or actual_video_id:
                creative_details['content_type'] = '동영상'
                creative_details['display_url'] = thumbnail_url or feed_thumbnail_url or image_url or ""
                if actual_video_id:
                    video_source_url = get_video_source_url(actual_video_id, ver, token)
                    if video_source_url:
                        creative_details['target_url'] = video_source_url
                    else:
                        creative_details['target_url'] = f"https://www.facebook.com/watch/?v={actual_video_id}"
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
                        if video_source_url:
                            creative_details['target_url'] = video_source_url
                        else:
                            creative_details['target_url'] = f"https://www.facebook.com/watch/?v={feed_video_id}"
                    else:
                        creative_details['target_url'] = creative_details['display_url']
                elif link_data and (link_data.get('image_hash') or link_data.get('image_url')):
                    creative_details['content_type'] = '사진'
                    creative_details['display_url'] = image_url or oss_image_url or thumbnail_url or ""
                    creative_details['target_url'] = oss_link or creative_details['display_url']
                elif instagram_permalink_url:
                    if thumbnail_url:
                        creative_details['content_type'] = '동영상'
                    else:
                        creative_details['content_type'] = '사진'
                    creative_details['display_url'] = thumbnail_url or image_url or ""
                    creative_details['target_url'] = instagram_permalink_url
                elif thumbnail_url:
                    creative_details['content_type'] = '동영상'
                    creative_details['display_url'] = thumbnail_url
                    story_id = details_data.get('effective_object_story_id')
                    if story_id and "_" in story_id:
                        creative_details['target_url'] = f"https://www.instagram.com/p/{story_id.split('_')[1]}/"
                    else:
                        creative_details['target_url'] = thumbnail_url
                else:
                    creative_details['content_type'] = '사진'
                    creative_details['display_url'] = image_url or thumbnail_url or ""
                    creative_details['target_url'] = oss_link or creative_details['display_url']

    except requests.exceptions.RequestException as e:
        print(f"Error fetching creative details for ad {ad_id}: {e}")
    except Exception as e:
        print(f"Error processing creative details for ad {ad_id}: {e}")

    return creative_details

def get_video_source_url(video_id, ver, token):
    try:
        video_req_url = f"https://graph.facebook.com/{ver}/{video_id}"
        video_params = {
            'fields': 'source',
            'access_token': token
        }
        video_response = requests.get(url=video_req_url, params=video_params)
        video_response.raise_for_status()
        video_data = video_response.json()
        return video_data.get('source')
    except Exception as e:
        print(f"Error fetching video source for video {video_id}: {e}")
        return None

def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
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
    """
    데이터를 가져와 집계 및 DataFrame으로 정리합니다.
    수정된 부분: 클릭 수는 actions의 link_click이 아니라, API의 "clicks" 필드를 그대로 사용합니다.
    """
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
        raise Exception(f"성과 데이터 불러오기 오류: {response.status_code} - {response.text}")
    data = response.json()
    records = data.get('data', [])
    ad_data = {}

    # 1) 데이터 집계: API의 "clicks" 필드를 사용 (기존 actions 링크 클릭 집계 로직 제거)
    for record in records:
        ad_id = record.get('ad_id')
        if not ad_id: 
            continue

        try:
            api_clicks = int(record.get("clicks", 0))
        except Exception:
            api_clicks = 0

        spend = float(record.get("spend", 0))
        impressions = int(record.get("impressions", 0))
        ctr = float(record.get("ctr", 0.0))
        cpc = float(record.get("cpc", 0.0))
        campaign_name = record.get("campaign_name", "N/A")
        adset_name = record.get("adset_name", "N/A")
        ad_name = record.get("ad_name", "N/A")

        ad_data[ad_id] = {
            "campaign_name": campaign_name,
            "adset_name": adset_name,
            "ad_name": ad_name,
            "spend": spend,
            "impressions": impressions,
            "clicks": api_clicks,  # API의 전체 클릭 수 사용
            "ctr": ctr,
            "cpc": cpc
        }

    # 2) 크리에이티브 정보 병렬 요청
    fetch_creatives_parallel(ad_data, ver, token)

    # 3) DataFrame 변환 후 사전 형태로 변환 (엑셀 등 다른 출력 방식으로 쉽게 변경 가능)
    df = pd.DataFrame.from_dict(ad_data, orient='index')
    df.index.name = "ad_id"
    df.reset_index(inplace=True)
    result = df.to_dict(orient='records')
    return {"data": result}

if __name__ == '__main__':
    app.run(debug=True)
