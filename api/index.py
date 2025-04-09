from flask import Flask, request, jsonify
import requests
import json
import os

app = Flask(__name__)

@app.route('/', methods=['GET'])
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
    
    # 결과 처리 (pandas 사용하지 않고 직접 계산)
    results = []
    totals = {
        'FB 광고비용': 0.0,
        '노출': 0,
        'Click': 0,
        'CPC': 0.0
    }
    
    # 각 광고 데이터 처리
    for ad_id, record in ad_data.items():
        spend = float(record.get('spend', '0'))
        impressions = int(float(record.get('impressions', '0')))
        clicks = int(float(record.get('clicks', '0')))
        
        # CTR 계산
        ctr = f"{((clicks / impressions) * 100):.2f}%" if impressions > 0 else '0%'
        
        # CPC 계산
        cpc = (spend / clicks) if clicks > 0 else 0
        
        # 결과 추가
        results.append({
            '광고명': record.get('ad_name'),
            '캠페인명': record.get('campaign_name'),
            '광고세트명': record.get('adset_name'),
            'FB 광고비용': spend,
            '노출': impressions,
            'Click': clicks,
            'CTR': ctr,
            'CPC': cpc,
            'image_url': record.get('image_url'),
            '광고 성과': ''  # 나중에 계산
        })
        
        # 합계 계산
        totals['FB 광고비용'] += spend
        totals['노출'] += impressions
        totals['Click'] += clicks
    
    # 평균 CTR 계산
    avg_ctr = f"{((totals['Click'] / totals['노출']) * 100):.2f}%" if totals['노출'] > 0 else '0%'
    
    # 평균 CPC 계산
    totals['CPC'] = totals['FB 광고비용'] / totals['Click'] if totals['Click'] > 0 else 0
    
    # 합계 행 추가
    total_row = {
        '광고명': '합계',
        '캠페인명': '',
        '광고세트명': '',
        'FB 광고비용': totals['FB 광고비용'],
        '노출': totals['노출'],
        'Click': totals['Click'],
        'CTR': avg_ctr,
        'CPC': totals['CPC'],
        'image_url': '',
        '광고 성과': ''
    }
    
    # 광고 성과 계산 및 정렬
    for result in results:
        if totals['Click'] > 0:
            click_percentage = result['Click'] / totals['Click']
            if click_percentage >= 0.5:
                result['광고 성과'] = '위닝콘텐츠'
            elif click_percentage >= 0.3:
                result['광고 성과'] = '고성과'
            else:
                result['광고 성과'] = '-'
    
    # 클릭 수 기준으로 정렬
    results.sort(key=lambda x: x['Click'], reverse=True)
    
    # 합계 행을 맨 앞에 추가
    final_results = [total_row] + results
    
    return final_results

if __name__ == '__main__':
    app.run(debug=True)
