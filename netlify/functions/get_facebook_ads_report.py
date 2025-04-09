import os
import sys
import json
import requests
import pandas as pd

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
    if response.status_code != 200:
        return {"error": f"Error fetching data: {response.text}"}

    data = response.json()
    records = data.get('data', [])
    
    # Process the data (simplified here for brevity)
    results = [{"ad_id": record.get("ad_id"), "spend": record.get("spend")} for record in records]
    
    return results

if __name__ == "__main__":
    try:
        # 명령줄 인자로 받은 시작 날짜와 종료 날짜
        start_date = sys.argv[1]
        end_date = sys.argv[2]

        # 환경 변수에서 Facebook 계정 ID와 액세스 토큰 가져오기
        ver = "v19.0"
        account = os.environ.get("FACEBOOK_ACCOUNT_ID")
        token = os.environ.get("FACEBOOK_ACCESS_TOKEN")

        # 환경 변수 확인
        if not account or not token:
            print(json.dumps({"error": "Missing Facebook account ID or access token"}))
            sys.exit(1)

        # Facebook 광고 데이터를 가져오고 JSON 형식으로 출력
        result = fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token)
        print(json.dumps(result))
    
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)
