from flask import Flask, request, jsonify
import requests
import json
import os
# traceback 모듈은 except 블록 안에서 import 해도 괜찮지만, 
# 파일 상단에 import하는 것이 더 일반적일 수 있습니다. (선택 사항)
import traceback 

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
        # Preflight 요청 처리
        return jsonify({}), 200
        
    try:
        data = request.get_json()
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({"error": "시작 날짜와 종료 날짜를 모두 입력해주세요."}), 400
        
        # 환경 변수에서 Facebook 계정 ID와 액세스 토큰 가져오기
        ver = "v19.0" # Facebook API 버전
        account = os.environ.get("FACEBOOK_ACCOUNT_ID")
        token = os.environ.get("FACEBOOK_ACCESS_TOKEN")

        if not account or not token:
            print("Error: Facebook Account ID or Access Token not found in environment variables.")
            # 환경 변수 누락 시 서버 설정 오류 반환
            return jsonify({"error": "Server configuration error: Missing Facebook credentials."}), 500

        print(f"Attempting to fetch data for account: {account} from {start_date} to {end_date}") # 로깅 추가

        # ================================================================
        # !!! 오류 발생 가능성이 높은 지점 !!!
        # fetch_and_format_facebook_ads_data 함수가 정의되어 있어야 하고,
        # 이 함수 내부에서 오류가 발생하지 않아야 합니다.
        # 필요한 라이브러리(예: facebook_business)가 requirements.txt에 있어야 합니다.
        # ================================================================
        result = fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token) 
        
        print("Successfully fetched and formatted data.") # 성공 로깅 추가
        return jsonify(result)

    except requests.exceptions.RequestException as req_err: # requests 라이브러리 관련 네트워크 오류 등 명시적 처리
        print(f"Error during Facebook API request: {str(req_err)}")
        return jsonify({"error": f"API request failed: {str(req_err)}"}), 500
    
    except KeyError as key_err: # Facebook API 응답 파싱 중 키 오류 등
        print(f"Error processing API response (KeyError): {str(key_err)}")
        return jsonify({"error": f"Error processing API data: {str(key_err)}"}), 500
    
    except Exception as e: # 그 외 예상치 못한 모든 오류 처리
        # traceback 모듈을 사용하여 더 자세한 오류 정보 로깅
        error_details = traceback.format_exc()
        print(f"An unexpected error occurred: {str(e)}\nDetails:\n{error_details}")
        # 사용자에게는 간단한 내부 서버 오류 메시지 전달
        return jsonify({"error": "An internal server error occurred while generating the report."}), 500

# ================================================================
# !!! 중요 !!!
# 아래 함수는 실제 구현이 필요합니다. 이 파일 또는 다른 파일에 정의되어야 합니다.
# 예시 이름이며, 실제 프로젝트의 함수 이름과 일치해야 합니다.
# ================================================================

def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token):
    import requests
    import pandas as pd

    # 1단계: 광고 성과 데이터 가져오기 (캠페인명과 광고세트명 포함)
    metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc'
    insights_url = f"https://graph.facebook.com/{ver}/{account}/insights"
    params = {
        'fields': metrics,
        'access_token': token,
        'level': 'ad',  # 광고 수준으로 설정
        'time_range[since]': start_date,
        'time_range[until]': end_date,
        'use_unified_attribution_setting': 'true'
    }
    
    response = requests.get(url=insights_url, params=params)
    ad_data = {}
    if response.status_code != 200:
        raise Exception(f"성과 데이터 불러오기 오류: {response.text}")
    
    data = response.json()
    records = data.get('data', [])
    # 광고 ID별로 데이터를 집계
    for record in records:
        ad_id = record.get('ad_id')
        if not ad_id:
            continue
        if ad_id not in ad_data:
            ad_data[ad_id] = record
        else:
            for key in ['spend', 'impressions', 'clicks']:
                if key in record:
                    ad_data[ad_id][key] = str(
                        float(ad_data[ad_id].get(key, '0')) + float(record.get(key, '0'))
                    )
    
    # 2단계: 각 광고의 크리에이티브를 조회하여 이미지 URL 가져오기
    for ad_id in ad_data:
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
                image_url = None
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
                    ad_data[ad_id]['image_url'] = image_url

    # 3단계: DataFrame 생성 및 데이터 가공
    result_list = list(ad_data.values())
    df = pd.DataFrame(result_list)
    
    # 숫자형 컬럼으로 변환
    numeric_columns = ['spend', 'impressions', 'clicks']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col])
    
    # CTR 계산 (clicks / impressions * 100)
    if 'clicks' in df.columns and 'impressions' in df.columns:
        df['ctr'] = (df['clicks'] / df['impressions'] * 100).round(2).astype(str) + '%'
    
    # CPC 계산 (spend / clicks)
    if 'spend' in df.columns and 'clicks' in df.columns:
        df['cpc'] = (df['spend'] / df['clicks']).round(2)
    
    # 컬럼명 한글화
    df = df.rename(columns={
        'ad_name': '광고명',
        'campaign_name': '캠페인명',
        'adset_name': '광고세트명',
        'spend': 'FB 광고비용',
        'impressions': '노출',
        'clicks': 'Click',
        'ctr': 'CTR',
        'cpc': 'CPC'
    })
    
    # 합계 행 계산
    numeric_columns = ['FB 광고비용', '노출', 'Click', 'CPC']
    totals = df[numeric_columns].sum()
    avg_ctr = (totals['Click'] / totals['노출'] * 100).round(2) if totals['노출'] > 0 else 0
    totals_row = pd.Series(
        ['합계', '', '', totals['FB 광고비용'], totals['노출'], totals['Click'], f"{avg_ctr}%", totals['CPC'], ''],
        index=['광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click', 'CTR', 'CPC', 'image_url']
    )
    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)
    
    # 광고 성과 컬럼 추가: 클릭 비율에 따라 '위닝콘텐츠', '고성과' 등을 분류
    def categorize_performance(row):
        if row['광고명'] == '합계' or pd.isna(row['Click']):
            return ''
        total_clicks = totals['Click']
        click_percentage = row['Click'] / total_clicks if total_clicks > 0 else 0
        if click_percentage >= 0.5:
            return '위닝콘텐츠'
        elif click_percentage >= 0.3:
            return '고성과'
        else:
            return '-'
    
    df_with_total['광고 성과'] = df_with_total.apply(categorize_performance, axis=1)
    
    # 정렬: 합계 행은 항상 상단, 그 외는 클릭 수 내림차순
    df_with_total['sort_key'] = df_with_total['광고명'].apply(lambda x: 0 if x == '합계' else 1)
    df_sorted = df_with_total.sort_values(by=['sort_key', 'Click'], ascending=[True, False]).drop('sort_key', axis=1)
    
    # 4단계: HTML 테이블 문자열 생성
    html_table = """
    <style>
    table {border-collapse: collapse; width: 100%;}
    th, td {padding: 8px; text-align: left; border-bottom: 1px solid #ddd;}
    th {background-color: #f2f2f2;}
    tr:hover {background-color: #f5f5f5;}
    .total-row {background-color: #e6f2ff; font-weight: bold;}
    .high-performance {color: #ff9900; font-weight: bold;}
    .winning-content {color: #009900; font-weight: bold;}
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
        <th>광고 성과</th>
        <th>광고 이미지</th>
    </tr>
    """
    for _, row in df_sorted.iterrows():
        row_class = 'total-row' if row['광고명'] == '합계' else ''
        performance_text = row.get('광고 성과', '')
        performance_class = ''
        if performance_text == '고성과':
            performance_class = 'high-performance'
        elif performance_text == '위닝콘텐츠':
            performance_class = 'winning-content'
        img_url = row.get("image_url", "")
        img_tag = f'<img src="{img_url}" style="max-width:100px; max-height:100px;">' if pd.notna(img_url) and img_url != "" else ""
        html_table += f"""
        <tr class="{row_class}">
            <td>{row.get('광고명', '')}</td>
            <td>{row.get('캠페인명', '')}</td>
            <td>{row.get('광고세트명', '')}</td>
            <td>{row.get('FB 광고비용', 0):.2f}</td>
            <td>{row.get('노출', 0):,}</td>
            <td>{row.get('Click', 0):,}</td>
            <td>{row.get('CTR', '0%')}</td>
            <td>{row.get('CPC', 0):.2f}</td>
            <td class="{performance_class}">{performance_text}</td>
            <td>{img_tag}</td>
        </tr>
        """
    html_table += "</table>"
    
    # API 응답용으로 HTML 테이블 문자열과 DataFrame 레코드 목록을 함께 반환
    return {"html_table": html_table, "data": df_sorted.to_dict(orient='records')}


# Flask 앱 실행 (로컬 테스트용, Vercel에서는 필요 없음)
# if __name__ == '__main__':
#    app.run(debug=True)
