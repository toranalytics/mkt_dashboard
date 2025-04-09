from flask import Flask, request, jsonify
import requests
import json
import pandas as pd
import os

app = Flask(__name__)

@app.route('/api', methods=['GET'])
def home():
    return jsonify({"message": "Facebook 광고 성과 보고서 API가 실행 중입니다."})

@app.route('/api/generate-report', methods=['POST'])
def generate_report():
    try:
        data = request.get_json()
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({"error": "시작 날짜와 종료 날짜를 모두 입력해주세요."}), 400
        
        # 환경 변수에서 Facebook 계정 ID와 액세스 토큰 가져오기
        ver = "v19.0"
        account = os.environ.get("FACEBOOK_ACCOUNT_ID", "act_1601012230605073")
        token = os.environ.get("FACEBOOK_ACCESS_TOKEN", "EAANZBwKbZBHwsBO1i1lgkwYZAWeaASGa46fvoDZBxSwXuUZCjgGuz1yT0Vcry7ANUdpAyJoGLeGdEhRvcSsFBkVFKWZB6bfzksEJ5z9vgjQ3L6Vb7Ax0e36U9FMk7YGYBfU5TqDmG0hjpm7WyqVDjN3u55TGRxcvO0IgOgPaApa05GhzmXsondFBpnurK1brIbCqwhqi5E")
        
        # Facebook 광고 데이터 가져오기
        result = fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token)
        return jsonify(result)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token):
    metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc'
    url = f"https://graph.facebook.com/{ver}/{account}/insights"

    params = {
        'fields': metrics,
        'access_token': token,
        'level': 'ad',
        'time_range[since]': start_date,
        'time_range[until]': end_date,
        'use_unified_attribution_setting': 'true'
    }

    response = requests.get(url=url, params=params)
    ad_data = {}

    if response.status_code != 200:
        return {"error": f"성과 데이터 불러오기 오류: {response.text}"}
    
    data = response.json()
    records = data.get('data', [])
    
    # 광고 ID를 키로 하는 딕셔너리 생성
    for record in records:
        ad_id = record.get('ad_id')
        if ad_id not in ad_data:
            ad_data[ad_id] = record
        else:
            # 이미 존재하는 광고면 지표 합산 (일별 데이터를 합산)
            for key in ['spend', 'impressions', 'clicks']:
                if key in record:
                    ad_data[ad_id][key] = str(float(ad_data[ad_id].get(key, '0')) + float(record.get(key, '0')))
    
    # 광고 이미지 URL 가져오기
    for ad_id in ad_data:
        # 광고 크리에이티브 ID 가져오기
        creative_url = f"https://graph.facebook.com/{ver}/{ad_id}"
        creative_params = {
            'fields': 'creative',
            'access_token': token
        }
        
        creative_response = requests.get(url=creative_url, params=creative_params)
        image_url = None
        
        if creative_response.status_code == 200:
            creative_data = creative_response.json()
            creative_id = creative_data.get('creative', {}).get('id')
            
            if creative_id:
                # 크리에이티브 ID로 이미지 URL 가져오기
                image_url_endpoint = f"https://graph.facebook.com/{ver}/{creative_id}"
                image_params = {
                    'fields': 'image_url,thumbnail_url,object_story_spec',
                    'access_token': token
                }
                
                image_response = requests.get(url=image_url_endpoint, params=image_params)
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
    
    # DataFrame 생성 및 결과 처리
    result_list = list(ad_data.values())
    df = pd.DataFrame(result_list)
    
    # 숫자형 컬럼 변환
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
    
    # 컬럼 이름 변경
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
    
    # 합계 계산
    numeric_columns = ['FB 광고비용', '노출', 'Click', 'CPC']
    totals = df[numeric_columns].sum()
    avg_ctr = (totals['Click'] / totals['노출'] * 100).round(2) if totals['노출'] > 0 else 0
    
    # 결과를 JSON으로 변환하기 위한 처리
    df_dict = df.to_dict('records')
    
    # 합계 행 추가
    totals_dict = {
        '광고명': '합계',
        '캠페인명': '',
        '광고세트명': '',
        'FB 광고비용': float(totals['FB 광고비용']),
        '노출': int(totals['노출']),
        'Click': int(totals['Click']),
        'CTR': f"{avg_ctr}%",
        'CPC': float(totals['CPC']),
        'image_url': '',
        '광고 성과': ''
    }
    
    # 최종 결과 생성 (합계 행을 첫 번째로)
    final_results = [totals_dict] + df_dict
    
    # 광고 성과 추가
    for item in final_results:
        if item['광고명'] != '합계':
            click_percentage = item['Click'] / totals['Click'] if totals['Click'] > 0 else 0
            if click_percentage >= 0.5:
                item['광고 성과'] = '위닝콘텐츠'
            elif click_percentage >= 0.3:
                item['광고 성과'] = '고성과'
            else:
                item['광고 성과'] = '-'
    
    return final_results

if __name__ == '__main__':
    app.run(debug=True)
