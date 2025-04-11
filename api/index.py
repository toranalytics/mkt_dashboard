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

        # 패스워드 보호
        password = data.get('password')
        if not password or password != os.environ.get("REPORT_PASSWORD"):
            return jsonify({"error": "비밀번호가 올바르지 않습니다."}), 403

        # 시작/종료 날짜 기본값 설정
        today = datetime.today()
        default_date = (today - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = data.get('start_date') or default_date
        end_date = data.get('end_date') or default_date

        ver = "v19.0" # API 버전
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
# 광고 크리에이티브 상세 정보 (이미지 URL, 비디오 ID, 콘텐츠 유형 등) 병렬 처리
# --------------------------------------------------------------------------------

def get_creative_details(ad_id, ver, token):
    """
    광고 ID를 사용하여 크리에이티브 상세 정보 (콘텐츠 유형, 표시 URL, 대상 URL)를 가져옵니다.
    """
    creative_details = {
        'content_type': '알 수 없음', # 기본값: 알 수 없음
        'display_url': '',          # HTML에 표시될 이미지/썸네일 URL
        'target_url': ''            # 클릭 시 이동할 URL (이미지 원본 또는 비디오 플레이어)
    }
    try:
        # 1. 광고 ID로 크리에이티브 ID 가져오기
        creative_req_url = f"https://graph.facebook.com/{ver}/{ad_id}"
        creative_params = {'fields': 'creative', 'access_token': token}
        creative_response = requests.get(url=creative_req_url, params=creative_params)
        creative_response.raise_for_status() # 오류 발생 시 예외 발생
        creative_data = creative_response.json()
        creative_id = creative_data.get('creative', {}).get('id')

        if creative_id:
            # 2. 크리에이티브 ID로 상세 정보 가져오기 (이미지, 비디오 정보 요청)
            details_req_url = f"https://graph.facebook.com/{ver}/{creative_id}"
            # 비디오 ID, 썸네일, 이미지 URL 및 object_story_spec 내 상세 정보 요청
            fields = 'image_url,thumbnail_url,video_id,object_story_spec{link_data{image_hash,image_url},photo_data{image_hash,image_url},video_data{video_id,image_url}}'
            details_params = {'fields': fields, 'access_token': token}
            details_response = requests.get(url=details_req_url, params=details_params)
            details_response.raise_for_status()
            details_data = details_response.json()

            video_id = details_data.get('video_id')
            story_spec = details_data.get('object_story_spec', {})
            oss_video_id = story_spec.get('video_data', {}).get('video_id')
            oss_image_url = (
                story_spec.get('photo_data', {}).get('image_url') or
                story_spec.get('link_data', {}).get('image_url') or
                story_spec.get('video_data', {}).get('image_url') # 비디오 썸네일 대체
            )

            # 비디오 판별 (creative 레벨 video_id 또는 object_story_spec 내 video_id 확인)
            if video_id or oss_video_id:
                actual_video_id = video_id or oss_video_id
                creative_details['content_type'] = '동영상'
                # 썸네일 우선 사용, 없으면 이미지 URL 사용
                creative_details['display_url'] = details_data.get('thumbnail_url') or details_data.get('image_url') or oss_image_url or ""
                # Facebook Watch 링크 생성
                creative_details['target_url'] = f"https://www.facebook.com/watch/?v={actual_video_id}" if actual_video_id else creative_details['display_url']

            # 이미지가 있는 경우 (비디오가 아닐 때)
            elif details_data.get('image_url') or oss_image_url:
                creative_details['content_type'] = '사진'
                creative_details['display_url'] = details_data.get('image_url') or oss_image_url
                creative_details['target_url'] = creative_details['display_url'] # 이미지 클릭 시 원본 이미지

            # 이미지도 비디오도 아니지만 썸네일이 있는 경우 (예: 기타 포맷)
            elif details_data.get('thumbnail_url'):
                 creative_details['content_type'] = '사진' # 기본적으로 사진으로 취급
                 creative_details['display_url'] = details_data.get('thumbnail_url')
                 creative_details['target_url'] = creative_details['display_url']

    except requests.exceptions.RequestException as e:
        print(f"Error fetching creative details for ad {ad_id}: {e}")
    except Exception as e:
        print(f"Error processing creative details for ad {ad_id}: {e}")

    return creative_details


def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
    """
    ThreadPoolExecutor를 사용하여 여러 광고의 크리에이티브 정보를 병렬로 가져옵니다.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_creative_details, ad_id, ver, token): ad_id for ad_id in ad_data.keys()}
        for future in as_completed(futures):
            ad_id = futures[future]
            try:
                creative_info = future.result()
            except Exception:
                 # 오류 발생 시 기본값 설정
                creative_info = {'content_type': '오류', 'display_url': '', 'target_url': ''}
            # ad_data 딕셔너리에 결과 저장
            ad_data[ad_id]['creative_details'] = creative_info


# --------------------------------------------------------------------------------
# 메인 함수: 구매 수, 구매당 비용, 광고 성과, 콘텐츠 유형/링크 추가
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
        'use_unified_attribution_setting': 'true' # 통합 기여 설정 사용
    }
    response = requests.get(url=insights_url, params=params)
    if response.status_code != 200:
        raise Exception(f"성과 데이터 불러오기 오류: {response.status_code} - {response.text}")
    data = response.json()
    records = data.get('data', [])
    ad_data = {}

    # 1) actions에서 구매 수 (purchase_count) 및 링크 클릭(link_clicks) 추출 및 데이터 합산
    for record in records:
        ad_id = record.get('ad_id')
        if not ad_id:
            continue

        link_clicks = 0
        purchase_count = 0
        actions = record.get('actions')
        if actions and isinstance(actions, list):
            for action in actions:
                action_type = action.get("action_type")
                try:
                    value = int(action.get("value", 0))
                except (ValueError, TypeError):
                    value = 0

                if action_type == "link_click":
                    link_clicks += value
                # 다양한 구매 유형 포함 (omni_purchase 등 고려 가능)
                elif action_type == "purchase" or action_type.startswith("omni_purchase"):
                     purchase_count += value

        record["link_clicks"] = link_clicks
        record["purchase_count"] = purchase_count

        # 중복 데이터 합산 (광고 ID별)
        if ad_id not in ad_data:
            ad_data[ad_id] = record
        else:
            for key in ['spend', 'impressions', 'link_clicks', 'purchase_count']:
                if key in record:
                    current_val = float(ad_data[ad_id].get(key, '0'))
                    new_val = float(record.get(key, '0'))
                    ad_data[ad_id][key] = str(current_val + new_val)

    # 2) 병렬로 크리에이티브 상세 정보(유형, URL 등) 가져오기
    fetch_creatives_parallel(ad_data, ver, token, max_workers=10)

    # 3) DataFrame 변환 및 데이터 정리
    result_list = list(ad_data.values())
    df = pd.DataFrame(result_list)

    # 크리에이티브 상세 정보 컬럼 추가
    df['creative_details'] = df['ad_id'].map(lambda ad_id: ad_data.get(ad_id, {}).get('creative_details', {}))
    df['콘텐츠 유형'] = df['creative_details'].apply(lambda x: x.get('content_type', '알 수 없음'))
    df['display_url'] = df['creative_details'].apply(lambda x: x.get('display_url', ''))
    df['target_url'] = df['creative_details'].apply(lambda x: x.get('target_url', ''))
    df = df.drop(columns=['creative_details']) # 임시 컬럼 제거

    # 숫자형 컬럼 정리
    numeric_cols = ['spend', 'impressions', 'link_clicks', 'purchase_count']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(0).astype(int)
        else:
            df[col] = 0 # 컬럼이 없으면 0으로 초기화

    # CTR, CPC, 구매당 비용 계산
    df['ctr_val'] = df.apply(
        lambda r: (r['link_clicks'] / r['impressions'] * 100) if r['impressions'] > 0 else 0, axis=1
    )
    df['CTR'] = df['ctr_val'].apply(lambda x: f"{round(x, 2)}%")

    df['cpc_val'] = df.apply(
        lambda r: (r['spend'] / r['link_clicks']) if r['link_clicks'] > 0 else 0, axis=1
    )
    df['CPC'] = df['cpc_val'].apply(lambda x: round(x) if pd.notna(x) else 0).astype(int)

    df['cost_per_purchase_val'] = df.apply(
        lambda r: (r['spend'] / r['purchase_count']) if r['purchase_count'] > 0 else 0, axis=1
    )
    df['구매당 비용'] = df['cost_per_purchase_val'].apply(lambda x: round(x) if pd.notna(x) else 0).astype(int)

    # 불필요한 계산용 컬럼 제거
    df = df.drop(columns=['ctr_val', 'cpc_val', 'cost_per_purchase_val', 'actions', 'clicks']) # actions, clicks 제거

    # 컬럼명 한글화 및 재명명
    df = df.rename(columns={
        'ad_name': '광고명',
        'campaign_name': '캠페인명',
        'adset_name': '광고세트명',
        'spend': 'FB 광고비용',
        'impressions': '노출',
        'link_clicks': 'Click', # 링크 클릭 기준으로 변경됨
        'purchase_count': '구매 수'
        # CTR, CPC, 구매당 비용은 위에서 이미 생성됨
        # '콘텐츠 유형', 'display_url', 'target_url' 은 위에서 이미 생성됨
    })

    # 4) 합계 행 처리
    total_spend = df['FB 광고비용'].sum()
    total_impressions = df['노출'].sum()
    total_clicks = df['Click'].sum()
    total_purchases = df['구매 수'].sum()
    # 가중 평균 CTR, CPC, CPP 계산
    total_ctr_val = (total_clicks / total_impressions * 100) if total_impressions > 0 else 0
    total_ctr = f"{round(total_ctr_val, 2)}%"
    total_cpc = round(total_spend / total_clicks) if total_clicks > 0 else 0
    total_cpp = round(total_spend / total_purchases) if total_purchases > 0 else 0

    # 합계 행 Series 생성 (새 컬럼 포함)
    totals_row = pd.Series([
        '합계', '', '', total_spend, total_impressions, total_clicks,
        total_ctr, total_cpc, total_purchases, total_cpp,
        '', '', '', '' # 광고 성과, 콘텐츠 유형, display_url, target_url 에 대한 빈 값
    ], index=[
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click',
        'CTR', 'CPC', '구매 수', '구매당 비용', '광고 성과', '콘텐츠 유형',
        'display_url', 'target_url' # 인덱스 순서 주의
    ])

    # DataFrame 컬럼 순서 정의 (광고 성과, 콘텐츠 유형 포함)
    column_order = [
        '광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click',
        'CTR', 'CPC', '구매 수', '구매당 비용', '광고 성과', '콘텐츠 유형', 'display_url', 'target_url'
    ]
    # df에 '광고 성과' 컬럼 미리 추가 (빈 값으로)
    df['광고 성과'] = ''
    df = df[column_order] # 컬럼 순서 적용

    # 합계 행 추가
    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)


    # 5) 테이블 정렬: 합계 행은 맨 위, 그 외는 구매당 비용 기준 (0은 맨 아래)
    def custom_sort_key(row):
        if row['광고명'] == '합계':
            return -1 # 합계 행은 최상단
        cost = row.get('구매당 비용', 0)
        return float('inf') if cost == 0 else cost # 구매당 비용 0은 무한대로 취급하여 맨 아래로

    df_with_total['sort_key'] = df_with_total.apply(custom_sort_key, axis=1)
    df_sorted = df_with_total.sort_values(by='sort_key', ascending=True).drop(columns='sort_key')

    # 6) 광고 성과 컬럼 재생성 (구매당 비용 기준, 정렬 후 실행)
    #    - 합계 행 제외하고 처리
    #    - 구매당 비용 > 0 인 광고들 중 비용 낮은 순 3개 라벨링
    #    - 구매당 비용 >= 100,000 이면 '개선 필요!'
    df_non_total = df_sorted[df_sorted['광고명'] != '합계'].copy() # SettingWithCopyWarning 방지 위해 .copy()
    # 유효한 (비용 > 0) 광고만 필터링
    df_valid_cost = df_non_total[df_non_total['구매당 비용'] > 0].copy()

    # 비용 100,000 미만인 광고만 순위 선정 대상
    df_rank_candidates = df_valid_cost[df_valid_cost['구매당 비용'] < 100000].sort_values(by='구매당 비용', ascending=True)
    top_indices = df_rank_candidates.head(3).index.tolist() # 상위 3개 인덱스

    def categorize_performance(row):
        if row['광고명'] == '합계':
            return ''
        cost = row['구매당 비용']
        if cost == 0:
            return '' # 구매 없음
        if cost >= 100000:
            return '개선 필요!'

        # 상위 3위 라벨링 (인덱스 기반)
        if row.name in top_indices:
            rank = top_indices.index(row.name) # 0, 1, 2
            if rank == 0: return '위닝 콘텐츠'
            if rank == 1: return '고성과 콘텐츠'
            if rank == 2: return '성과 콘텐츠'
        return '' # 그 외 (비용 0 초과, 10만 미만, 3위 밖)

    # apply 함수를 전체 df_sorted 에 적용 (합계 행은 '' 반환됨)
    df_sorted['광고 성과'] = df_sorted.apply(categorize_performance, axis=1)

    # 7) HTML 테이블 생성 (콘텐츠 유형 및 클릭 가능한 콘텐츠 추가)
    def format_currency(amount):
        return f"{int(amount):,} ₩" if pd.notna(amount) else "0 ₩"
    def format_number(num):
        return f"{int(num):,}" if pd.notna(num) else "0"

    html_table = """
    <style>
    /* ... (기존 스타일 유지) ... */
    table {border-collapse: collapse; width: 100%;}
    th, td {padding: 8px; border-bottom: 1px solid #ddd;}
    th {background-color: #f2f2f2; text-align: center; white-space: nowrap; vertical-align: middle;}
    td {text-align: right; white-space: nowrap; vertical-align: middle;}
    td:nth-child(1), td:nth-child(2), td:nth-child(3) { text-align: left; } /* 광고명, 캠페인명, 광고세트명 왼쪽 정렬 */
    td:nth-child(11), td:nth-child(12) { text-align: center; } /* 광고 성과, 콘텐츠 유형 가운데 정렬 */
    tr:hover {background-color: #f5f5f5;}
    .total-row {background-color: #e6f2ff; font-weight: bold;}
    .winning-content {color: #009900; font-weight: bold;}    /* 초록: 위닝 콘텐츠 */
    .medium-performance {color: #E69900; font-weight: bold;} /* 노랑(진하게): 고성과 콘텐츠 */
    .third-performance {color: #FF9900; font-weight: bold;}  /* 주황: 성과 콘텐츠 */
    .needs-improvement {color: #FF0000; font-weight: bold;}  /* 빨강: 개선 필요! */
    a {text-decoration: none; color: inherit;}
    img.ad-content-thumbnail {max-width:100px; max-height:100px; vertical-align: middle;}
    td.ad-content-cell { text-align: center; } /* 광고 콘텐츠 셀 가운데 정렬 */
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
        <th>콘텐츠 유형</th>  <th>광고 콘텐츠</th> </tr>
    """
    for index, row in df_sorted.iterrows(): # iterrows() 사용
        row_class = 'total-row' if row['광고명'] == '합계' else ''
        performance_text = row.get('광고 성과', '')

        # 광고 성과 CSS 클래스
        if performance_text == '위닝 콘텐츠': performance_class = 'winning-content'
        elif performance_text == '고성과 콘텐츠': performance_class = 'medium-performance'
        elif performance_text == '성과 콘텐츠': performance_class = 'third-performance'
        elif performance_text == '개선 필요!': performance_class = 'needs-improvement'
        else: performance_class = ''

        # 광고 콘텐츠 (이미지/썸네일) 및 링크 생성
        display_url = row.get("display_url", "")
        target_url = row.get("target_url", "")
        content_tag = ""
        if display_url:
            img_tag = f'<img src="{display_url}" class="ad-content-thumbnail" alt="광고 콘텐츠">'
            # target_url이 있으면 링크 생성, 없으면 이미지 태그만 사용
            if target_url:
                content_tag = f'<a href="{target_url}" target="_blank">{img_tag}</a>'
            else:
                content_tag = img_tag
        elif row['광고명'] != '합계': # 합계 행 아니면서 이미지 없으면 'N/A' 표시 등 가능
             content_tag = "-" # 내용 없음 표시


        html_table += f"""
        <tr class="{row_class}">
          <td>{row.get('광고명','')}</td>
          <td>{row.get('캠페인명','')}</td>
          <td>{row.get('광고세트명','')}</td>
          <td>{format_currency(row.get('FB 광고비용',0))}</td>
          <td>{format_number(row.get('노출',0))}</td>
          <td>{format_number(row.get('Click',0))}</td>
          <td>{row.get('CTR','0%')}</td>
          <td>{format_currency(row.get('CPC',0))}</td> <td>{format_number(row.get('구매 수',0))}</td>
          <td>{format_currency(row.get('구매당 비용',0))}</td> <td class="{performance_class}">{performance_text}</td>
          <td>{row.get('콘텐츠 유형','')}</td>  <td class="ad-content-cell">{content_tag}</td> </tr>
        """
    html_table += "</table>"

    # 8) 최종 결과 준비 (JSON용 데이터 클리닝)
    #    - HTML 테이블에서는 이미 문자열 처리됨. JSON 데이터용 정리.
    #    - 불필요한 URL 컬럼 제거하고 JSON 반환
    df_for_json = df_sorted.drop(columns=['display_url', 'target_url'])

    def clean_numeric(data):
        if isinstance(data, dict):
            return {k: clean_numeric(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [clean_numeric(item) for item in data]
        elif isinstance(data, (float, int)):
             # 무한대 또는 NaN 값 처리
            if math.isinf(data) or math.isnan(data):
                return 0 # 또는 None 이나 다른 적절한 값
        return data

    records = df_for_json.to_dict(orient='records')
    cleaned_records = clean_numeric(records)

    return {"html_table": html_table, "data": cleaned_records}

# 로컬 테스트용 (Vercel에서는 사용되지 않음)
# if __name__ == '__main__':
#     # 로컬 테스트 시 환경 변수 설정 필요
#     # 예: os.environ['REPORT_PASSWORD'] = 'your_password'
#     #     os.environ['FACEBOOK_ACCOUNT_ID'] = 'act_your_account_id'
#     #     os.environ['FACEBOOK_ACCESS_TOKEN'] = 'your_long_lived_token'
#     app.run(debug=True, port=5001)
