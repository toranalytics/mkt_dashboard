import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()  # .env 파일에서 환경 변수 로드 (로컬 개발 시 필요)

ver = "v19.0"
account = os.environ.get("FACEBOOK_ACCOUNT_ID")  # Netlify 환경 변수 사용
token = os.environ.get("FACEBOOK_ACCESS_TOKEN")  # Netlify 환경 변수 사용

def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token):
    """
    Facebook 광고 성과 데이터를 가져와 JSON 형태로 포맷합니다.
    """
    metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc'
    url = f"https://graph.facebook.com/{ver}/{account}/insights"

    params = {
        'fields': metrics,
        'access_token': token,
        'level': 'ad',  # 광고 수준으로 설정
        'time_range[since]': start_date,
        'time_range[until]': end_date,
        'use_unified_attribution_setting': 'true'
    }

    response = requests.get(url=url, params=params)
    ad_data = {}

    if response.status_code != 200:
        return {"error": f"성과 데이터 불러오기 오류: {response.text}"}
    else:
        data = response.json()
        records = data.get('data', [])

        for record in records:
            ad_id = record.get('ad_id')
            if ad_id not in ad_data:
                ad_data[ad_id] = record
            else:
                for key in ['spend', 'impressions', 'clicks']:
                    if key in record:
                        ad_data[ad_id][key] = str(float(ad_data[ad_id].get(key, '0')) + float(record.get(key, '0')))

        results = []
        for record in ad_data.values():
            ad_id = record.get('ad_id')
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

            record['image_url'] = image_url

            spend = float(record.get('spend', '0'))
            impressions = int(record.get('impressions', '0'))
            clicks = int(record.get('clicks', '0'))
            ctr = f"{((clicks / impressions) * 100):.2f}%" if impressions > 0 else '0%'
            cpc = (spend / clicks) if clicks > 0 else 0

            results.append({
                '광고명': record.get('ad_name'),
                '캠페인명': record.get('campaign_name'),
                '광고세트명': record.get('adset_name'),
                'FB 광고비용': spend,
                '노출': impressions,
                'Click': clicks,
                'CTR': ctr,
                'CPC': cpc,
                'image_url': image_url
            })

        df = pd.DataFrame(results)
        numeric_columns = ['FB 광고비용', '노출', 'Click', 'CPC']
        totals = df[numeric_columns].sum()
        avg_ctr = f"{((totals['Click'] / totals['노출']) * 100):.2f}%" if totals['노출'] > 0 else '0%'

        totals_row = pd.Series(['합계', '', '', totals['FB 광고비용'], totals['노출'], totals['Click'],
                                        avg_ctr, totals['CPC'], ''],
                                       index=['광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click', 'CTR', 'CPC', 'image_url'])

        df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)

        def categorize_performance(row):
            if pd.isna(row['Click']) or row['광고명'] == '합계':
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
        df_sorted = df_with_total.sort_values(by=['Click'], ascending=False)

        return df_sorted.to_dict('records')

def handler(event, context):
    data = json.loads(event['body'])
    start_date = data.get('start_date')
    end_date = data.get('end_date')

    if not account or not token:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Facebook 계정 ID 및 액세스 토큰을 Netlify 환경 변수로 설정해야 합니다.'}, ensure_ascii=False),
        }

    if start_date and end_date:
        report_data = fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token)
        return {
            'statusCode': 200,
            'body': json.dumps(report_data, default=str, ensure_ascii=False),
            'headers': {
                'Content-Type': 'application/json',
            },
        }
    else:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': '시작 날짜와 종료 날짜를 모두 입력해주세요.'}, ensure_ascii=False),
        }
