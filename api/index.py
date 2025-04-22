# api/index.py 파일 내부에 포함될 함수입니다.

import requests
import json # JSON 로깅을 위해 추가
import traceback

# --- get_video_source_url 함수는 그대로 유지 ---
def get_video_source_url(video_id, ver, token):
    try:
        video_req_url = f"https://graph.facebook.com/{ver}/{video_id}"; video_params = {'fields': 'source', 'access_token': token}
        video_response = requests.get(url=video_req_url, params=video_params, timeout=10); video_response.raise_for_status()
        return video_response.json().get('source')
    except Exception as e: print(f"Notice: Could not fetch video source for video {video_id}. Error: {e}"); return None

# --- get_creative_details 함수 최종 수정 (JSON 분석 기반) ---
def get_creative_details(ad_id, ver, token):
    creative_details = {'content_type': '알 수 없음', 'display_url': '', 'target_url': ''}
    try:
        # 1. Creative ID 얻기
        creative_req_url = f"https://graph.facebook.com/{ver}/{ad_id}"
        creative_params = {'fields': 'creative{id}', 'access_token': token}
        creative_response = requests.get(url=creative_req_url, params=creative_params, timeout=10)
        creative_response.raise_for_status()
        creative_id = creative_response.json().get('creative', {}).get('id')

        if creative_id:
            # 2. Creative 상세 정보 얻기 (필요 필드 요청)
            details_req_url = f"https://graph.facebook.com/{ver}/{creative_id}"
            # video_id 포함 요청 (asset_feed_spec 내에서도)
            fields = 'object_type,image_url,thumbnail_url,video_id,object_story_spec{link_data{link,picture,image_url,video_id}},instagram_permalink_url,asset_feed_spec{videos{video_id,thumbnail_url},images{url},link_urls{website_url}}'
            details_params = {'fields': fields, 'access_token': token}
            details_response = requests.get(url=details_req_url, params=details_params, timeout=15)
            details_response.raise_for_status()
            details_data = details_response.json()

            # --- 디버깅 로그 (JSON 확인했으므로 이제 주석 처리 가능) ---
            # print(f"--- Creative RAW for ad_id: {ad_id} ---"); print(json.dumps(details_data, indent=2, ensure_ascii=False)); print("--- END RAW ---")
            # --- 디버깅 로그 ---

            # 3. 데이터 추출 및 변수 정의
            object_type = details_data.get('object_type')
            video_id = details_data.get('video_id') # 최상위 비디오 ID
            image_url = details_data.get('image_url') # 최상위 이미지 URL
            thumbnail_url = details_data.get('thumbnail_url') # 최상위 썸네일 URL
            instagram_permalink_url = details_data.get('instagram_permalink_url')
            story_spec = details_data.get('object_story_spec', {})
            asset_feed_spec = details_data.get('asset_feed_spec', {})

            # AFS 데이터
            videos_from_feed = asset_feed_spec.get('videos', []) if asset_feed_spec else []
            feed_video_id = videos_from_feed[0].get('video_id') if videos_from_feed else None
            feed_thumbnail_url = videos_from_feed[0].get('thumbnail_url') if videos_from_feed else None
            images_from_feed = asset_feed_spec.get('images', []) if asset_feed_spec else []
            feed_image_url = images_from_feed[0].get('url') if images_from_feed else None
            link_urls_from_feed = asset_feed_spec.get('link_urls', []) if asset_feed_spec else []
            feed_website_url = link_urls_from_feed[0].get('website_url') if link_urls_from_feed else None # AFS 랜딩 URL

            # OSS 데이터
            link_data = story_spec.get('link_data', {}) if story_spec else {}
            oss_image_url = link_data.get('picture') or link_data.get('image_url') # picture 우선
            oss_link = link_data.get('link') # OSS 랜딩 URL
            oss_video_id = link_data.get('video_id')

            # 4. 최종 값 결정
            actual_video_id = video_id or feed_video_id or oss_video_id
            display_image_url = thumbnail_url or feed_thumbnail_url or image_url or feed_image_url or oss_image_url or ""
            # ★ Target URL: 랜딩 페이지 링크 최우선 ★
            best_target_url = feed_website_url or oss_link or instagram_permalink_url

            # ★ Content Type 결정 로직 개선 ★
            content_type = '알 수 없음' # 기본값
            if object_type == 'VIDEO' or actual_video_id:
                content_type = '동영상'
            elif object_type == 'PHOTO':
                 content_type = '사진'
            elif object_type == 'CAROUSEL' or (asset_feed_spec and (images_from_feed or videos_from_feed)):
                 # asset_feed_spec 에 이미지나 비디오 있으면 캐러셀/컬렉션 가능성 높음
                 content_type = '캐러셀'
                 # 캐러셀의 경우에도 feed_website_url 이 target_url 로 사용될 가능성 높음
            elif instagram_permalink_url:
                 content_type = '인스타그램'
            # 'SHARE' 또는 object_type 불분명 시 내부 데이터로 재판단
            elif object_type == 'SHARE' or object_type is None:
                 if actual_video_id: content_type = '동영상' # 비디오 정보 있으면 동영상
                 elif image_url or thumbnail_url or feed_image_url or oss_image_url: content_type = '사진' # 이미지 있으면 사진
                 else: content_type = '공유 게시물' # 둘 다 없으면
            elif display_image_url: # 위 모든 경우 아니고 이미지라도 있으면 사진 취급
                content_type = '사진'

            # 5. creative_details 딕셔너리 채우기
            creative_details['content_type'] = content_type
            creative_details['display_url'] = display_image_url

            # target_url 최종 결정
            final_target_url = best_target_url # 랜딩 페이지 링크 우선
            if content_type == '동영상' and actual_video_id and not final_target_url:
                # 동영상인데 랜딩 링크 없으면 비디오 링크 시도
                video_source_url = get_video_source_url(actual_video_id, ver, token)
                final_target_url = video_source_url if video_source_url else f"https://www.facebook.com/watch/?v={actual_video_id}"

            # 최종 target_url 할당 (유효한 URL인지 확인)
            if isinstance(final_target_url, str) and final_target_url.startswith('http'):
                creative_details['target_url'] = final_target_url
            elif isinstance(display_image_url, str) and display_image_url.startswith('http'):
                 # 쓸만한 링크 없고 display_url 이라도 링크 형태면 사용 (이미지 클릭 시 이미지 보기)
                 creative_details['target_url'] = display_image_url
            else:
                 creative_details['target_url'] = '' # 유효한 링크 없으면 빈 값

    # except 블록 (오류 로깅)
    except requests.exceptions.RequestException as e:
        response_text = e.response.text[:500] if hasattr(e, 'response') and e.response is not None else 'N/A'
        print(f"Error fetching creative details for ad_id {ad_id}: {e}. Response: {response_text}...")
    except Exception as e: print(f"Error processing creative details for ad_id {ad_id}: {e}")
    return creative_details

# --- fetch_creatives_parallel 함수는 이전과 동일하게 유지 ---
def fetch_creatives_parallel(ad_data, ver, token, max_workers=10):
    print(f"Fetching creative details for {len(ad_data)} ads...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        if not ad_data: print("No ad data for creatives."); return
        futures = {executor.submit(get_creative_details, ad_id, ver, token): ad_id for ad_id in ad_data.keys()}
        for i, future in enumerate(as_completed(futures)):
            ad_id = futures[future]; creative_info = {'content_type': '오류', 'display_url': '', 'target_url': ''}
            try: creative_info = future.result()
            except Exception as e: print(f"Error in creative future for ad {ad_id}: {e}")
            if ad_id in ad_data: ad_data[ad_id]['creative_details'] = creative_info
    print("Finished fetching creative details.")


# --- fetch_and_format_facebook_ads_data 함수 (최신 유지) ---
def fetch_and_format_facebook_ads_data(start_date, end_date, ver, account, token, cafe24_totals):
    # ... (이 함수 전체는 이전 답변의 최신 버전을 그대로 사용) ...
    all_records = []; metrics = 'ad_id,ad_name,campaign_name,adset_name,spend,impressions,clicks,ctr,cpc,actions{action_type,value}'; insights_url = f"https://graph.facebook.com/{ver}/{account}/insights"
    params = { 'fields': metrics, 'access_token': token, 'level': 'ad', 'time_range[since]': start_date, 'time_range[until]': end_date, 'use_unified_attribution_setting': 'true', 'action_attribution_windows': ['1d_click', '7d_click', '1d_view'], 'limit': 200 }; page_count = 1
    while insights_url: # 페이지네이션
        print(f"Fetching Meta Ads data page {page_count}..."); current_url = insights_url; current_params = params if page_count == 1 else {'access_token': token}
        try: response = requests.get(url=current_url, params=current_params, timeout=60); response.raise_for_status()
        except requests.exceptions.Timeout: print(f"Meta Ads API request timed out (Page: {page_count})."); break
        except requests.exceptions.RequestException as req_err: print(f"Meta Ads API network error (Page: {page_count}): {req_err}"); break
        data = response.json(); records_on_page = data.get('data', []);
        if not records_on_page: break
        all_records.extend(records_on_page); insights_url = data.get('paging', {}).get('next'); page_count += 1; params = None
    print(f"Finished fetching Meta Ads pages. Total {len(all_records)} records found.")
    if not all_records: return {"html_table": "<p>선택한 기간에 Meta 광고 데이터가 없습니다.</p>", "data": [], "cafe24_totals": cafe24_totals}

    # 데이터 집계 (최신)
    ad_data = {}
    for record in all_records:
        ad_id = record.get('ad_id');
        if not ad_id: continue
        if ad_id not in ad_data: ad_data[ad_id] = {'ad_id': ad_id, 'ad_name': record.get('ad_name'), 'campaign_name': record.get('campaign_name'), 'adset_name': record.get('adset_name'), 'spend': 0.0, 'impressions': 0, 'link_clicks': 0, 'purchase_count': 0}
        try: ad_data[ad_id]['spend'] += float(record.get('spend', 0))
        except: pass
        try: ad_data[ad_id]['impressions'] += int(record.get('impressions', 0))
        except: pass
        try: ad_data[ad_id]['link_clicks'] += int(record.get('clicks', 0))
        except: pass
        actions_data = record.get('actions'); actions_list = [];
        if isinstance(actions_data, dict): actions_list = actions_data.get('data', [])
        elif isinstance(actions_data, list): actions_list = actions_data
        if not isinstance(actions_list, list): actions_list = []
        purchase_count_on_record = 0
        for action in actions_list:
            if not isinstance(action, dict): continue
            action_type = action.get("action_type", "");
            if action_type in ["purchase", "offsite_conversion.fb_pixel_purchase", "omni_purchase", "website_purchase"]:
                try: value_str = action.get("value", "0"); purchase_count_on_record += int(float(value_str))
                except: pass
        ad_data[ad_id]['purchase_count'] += purchase_count_on_record;
        ad_data[ad_id]['ad_name'] = record.get('ad_name') or ad_data[ad_id]['ad_name']; ad_data[ad_id]['campaign_name'] = record.get('campaign_name') or ad_data[ad_id]['campaign_name']; ad_data[ad_id]['adset_name'] = record.get('adset_name') or ad_data[ad_id]['adset_name']

    # 크리에이티브 정보 병렬 조회 (수정된 함수 호출)
    fetch_creatives_parallel(ad_data, ver, token)
    result_list = list(ad_data.values());
    if not result_list: return {"html_table": "<p>Meta 데이터 집계 결과 없음.</p>", "data": []}
    df = pd.DataFrame(result_list)

    # --- DataFrame 후처리 및 HTML/JSON 생성 (이전과 동일) ---
    df['creative_details'] = df['ad_id'].map(lambda aid: ad_data.get(aid, {}).get('creative_details', {'content_type': '알 수 없음', 'display_url': '', 'target_url': ''}))
    df['콘텐츠 유형'] = df['creative_details'].apply(lambda x: x.get('content_type', '알 수 없음'))
    df['display_url'] = df['creative_details'].apply(lambda x: x.get('display_url', ''))
    df['target_url'] = df['creative_details'].apply(lambda x: x.get('target_url', '')) # 개선된 target_url 반영됨
    df = df.drop(columns=['creative_details'])
    numeric_cols = ['spend', 'impressions', 'link_clicks', 'purchase_count']
    for col in numeric_cols: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['CTR'] = df.apply(lambda r: f"{(r['link_clicks'] / r['impressions'] * 100):.2f}%" if r['impressions'] > 0 else '0.00%', axis=1)
    df['CPC'] = df.apply(lambda r: int(round(r['spend'] / r['link_clicks'])) if r['link_clicks'] > 0 else 0, axis=1)
    df['구매당 비용'] = df.apply(lambda r: int(round(r['spend'] / r['purchase_count'])) if r['purchase_count'] > 0 else 0, axis=1)
    df = df.rename(columns={'ad_name': '광고명', 'campaign_name': '캠페인명', 'adset_name': '광고세트명', 'spend': 'FB 광고비용', 'impressions': '노출', 'link_clicks': 'Click', 'purchase_count': '구매 수'})
    int_cols = ['노출', 'Click', '구매 수', 'CPC', '구매당 비용'];
    for col in int_cols: df[col] = df[col].round(0).astype(int)
    df['FB 광고비용'] = df['FB 광고비용'].round(0).astype(int)
    total_spend = df['FB 광고비용'].sum(); total_impressions = df['노출'].sum(); total_clicks = df['Click'].sum(); total_purchases = df['구매 수'].sum()
    total_ctr = f"{(total_clicks / total_impressions * 100):.2f}%" if total_impressions > 0 else '0.00%'
    total_cpc = int(round(total_spend / total_clicks)) if total_clicks > 0 else 0
    total_cpp = int(round(total_spend / total_purchases)) if total_purchases > 0 else 0
    totals_data = {'광고명': '합계', '캠페인명': '', '광고세트명': '', 'FB 광고비용': total_spend, '노출': total_impressions, 'Click': total_clicks, 'CTR': total_ctr, 'CPC': total_cpc, '구매 수': total_purchases, '구매당 비용': total_cpp,
                   'Cafe24 방문자 수': cafe24_totals.get('total_visitors', 0), 'Cafe24 매출': cafe24_totals.get('total_sales', 0),
                   'ad_id': '', '콘텐츠 유형': '', 'display_url': '', 'target_url': '', '광고 성과': ''}
    totals_row = pd.Series(totals_data)
    df['ad_id'] = df['ad_id']
    df['광고 성과'] = ''
    url_map = df.set_index('ad_id')[['display_url', 'target_url']].to_dict('index') if 'ad_id' in df.columns and not df['ad_id'].isnull().all() else {}
    df_with_total = pd.concat([pd.DataFrame([totals_row]), df], ignore_index=True)
    def custom_sort_key(row):
        if row['광고명'] == '합계': return -1
        cost = pd.to_numeric(row.get('구매당 비용', float('inf')), errors='coerce'); return float('inf') if pd.isna(cost) or cost == 0 else cost
    df_with_total['sort_key'] = df_with_total.apply(custom_sort_key, axis=1)
    df_sorted = df_with_total.sort_values(by='sort_key', ascending=True).drop(columns=['sort_key'], errors='ignore')
    df_non_total = df_sorted[df_sorted['광고명'] != '합계'].copy(); df_valid_cost = df_non_total[pd.to_numeric(df_non_total['구매당 비용'], errors='coerce').fillna(float('inf')) > 0].copy()
    top_ad_ids = []
    if not df_valid_cost.empty and 'ad_id' in df_valid_cost.columns:
        df_valid_cost['구매당 비용_num'] = pd.to_numeric(df_valid_cost['구매당 비용'])
        df_rank_candidates = df_valid_cost[df_valid_cost['구매당 비용_num'] < 100000].sort_values(by='구매당 비용_num', ascending=True)
        top_ad_ids = df_rank_candidates.head(3)['ad_id'].tolist()
    def categorize_performance(row):
        # 합계 행은 제외
        if row['광고명'] == '합계':
            return ''

        ad_id_current = row.get('ad_id')
        cost = pd.to_numeric(row.get('구매당 비용', float('inf')), errors='coerce')

        # 구매당 비용 계산 불가 시 제외
        if pd.isna(cost) or cost == 0:
            return ''

        # 기준 금액 이상이면 '개선 필요!'
        if cost >= 100000:
            return '개선 필요!'

        # 상위 ad_id 리스트(top_ad_ids)에 포함되는지 확인
        # ★★★ 이 if 문의 들여쓰기가 중요합니다! ★★★
        if ad_id_current in top_ad_ids:
            try:
                # 순위(인덱스) 찾기
                rank = top_ad_ids.index(ad_id_current)
                # 순위에 따라 성과 분류 반환 (들여쓰기 확인!)
                if rank == 0:
                    return '위닝 콘텐츠'
                elif rank == 1:
                    return '고성과 콘텐츠'
                elif rank == 2:
                    return '성과 콘텐츠'
                # else: rank 3 이상은 아래 최종 return '' 로 처리됨
            except ValueError:
                # 혹시 리스트에 없는 예외적인 경우
                return ''

        # 위 if문에 해당하지 않거나 rank 3 이상인 경우 빈칸 반환
        return ''
    if 'ad_id' in df_sorted.columns: df_sorted['광고 성과'] = df_sorted.apply(categorize_performance, axis=1)
    else: df_sorted['광고 성과'] = ''
    df_sorted['display_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('display_url', '')) if 'ad_id' in df_sorted.columns else ''
    df_sorted['target_url'] = df_sorted['ad_id'].map(lambda x: url_map.get(x, {}).get('target_url', '')) if 'ad_id' in df_sorted.columns else ''
    # HTML 테이블 생성 (최신 로직)
    def format_currency(amount): return f"{int(amount):,} ₩" if pd.notna(amount) and not isinstance(amount, str) else ("0 ₩" if pd.notna(amount) else "0 ₩")
    def format_number(num): return f"{int(num):,}" if pd.notna(num) and not isinstance(num, str) else ("0" if pd.notna(num) else "0")
    display_columns = ['광고명', '캠페인명', '광고세트명', 'FB 광고비용', '노출', 'Click', 'CTR', 'CPC', '구매 수', '구매당 비용', 'Cafe24 방문자 수', 'Cafe24 매출', '광고 성과', '콘텐츠 유형', '광고 콘텐츠']
    html_table = """<style>/* ... CSS ... */</style><table><thead><tr>""" # CSS 축약
    for col_name in display_columns: html_table += f"<th>{col_name}</th>"
    html_table += "</tr></thead><tbody>"
    for index, row in df_sorted.iterrows():
        is_total_row = row.get('광고명') == '합계'; row_class = 'total-row' if is_total_row else ''
        html_table += f'<tr class="{row_class}">'
        for col in display_columns:
            value = None; td_class = []; td_align = 'right'
            # 컬럼별 처리 로직 ...
            if col in ['광고명', '캠페인명', '광고세트명']: value = row.get(col, ''); td_align = 'left'; td_class.append('text-left')
            elif col in ['FB 광고비용', 'CPC', '구매당 비용', 'Cafe24 매출']: value = format_currency(row.get(col))
            elif col in ['노출', 'Click', '구매 수', 'Cafe24 방문자 수']: value = format_number(row.get(col))
            elif col == 'CTR': value = row.get(col, '0.00%')
            elif col == '광고 성과': # 수정된 categorize_performance 결과 사용
                performance_text = row.get(col, ''); performance_class = '';
                if performance_text == '위닝 콘텐츠': performance_class = 'winning-content'
                elif performance_text == '고성과 콘텐츠': performance_class = 'medium-performance'
                elif performance_text == '성과 콘텐츠': performance_class = 'third-performance'
                elif performance_text == '개선 필요!': performance_class = 'needs-improvement'
                value = performance_text;
                if performance_class: td_class.append(performance_class)
                td_align = 'center'; td_class.append('text-center')
            elif col == '콘텐츠 유형': value = row.get(col, '-') if not is_total_row else ''; td_align = 'center'; td_class.append('text-center')
            elif col == '광고 콘텐츠': # ★ 개선된 target_url 사용 ★
                display_url = row.get('display_url', ''); target_url = row.get('target_url', '')
                content_tag = "";
                if not is_total_row and display_url:
                    img_tag = f'<img src="{display_url}" class="ad-content-thumbnail" alt="콘텐츠 썸네일">'
                    if isinstance(target_url, str) and target_url.startswith('http'): # 유효 링크 확인
                        content_tag = f'<a href="{target_url}" target="_blank" title="콘텐츠 보기">{img_tag}</a>'
                    else: content_tag = img_tag
                elif not is_total_row: content_tag = "-"
                value = content_tag; td_class.append("ad-content-cell"); td_align = 'center'
            else: value = row.get(col, '')
            if not is_total_row and col in ['Cafe24 방문자 수', 'Cafe24 매출']: value = '-'
            td_style = f'text-align: {td_align};'; td_class_attr = f' class="{" ".join(td_class)}"' if td_class else ''
            html_table += f'<td{td_class_attr} style="{td_style}">{value}</td>'
        html_table += "</tr>\n"
    html_table += "</tbody></table>"
    # --- JSON 데이터 준비 (최신 버전 로직 유지) ---
    final_columns_for_json = [col for col in display_columns if col not in ['광고 콘텐츠']] + ['ad_id']
    df_for_json = df_sorted[[col for col in final_columns_for_json if col in df_sorted.columns]].copy()
    def clean_data_for_json(obj): # 클리닝 함수
        if isinstance(obj, dict): return {k: clean_data_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list): return [clean_data_for_json(elem) for elem in obj]
        elif isinstance(obj, (int, float)):
            if pd.isna(obj) or math.isinf(obj): return None
            if hasattr(obj, 'item'): return obj.item()
            return obj
        elif isinstance(obj, (pd.Timestamp, date)): return obj.isoformat()
        elif hasattr(obj, 'item'):
             try: return obj.item()
             except: return str(obj)
        elif isinstance(obj, (bool, str)) or obj is None: return obj
        else: return str(obj)
    records = df_for_json.to_dict(orient='records'); cleaned_records = clean_data_for_json(records)
    return {"html_table": html_table, "data": cleaned_records}

# --- 앱 실행 부분 (최신 유지) ---
# if __name__ == '__main__':
#     app.run(debug=True, host='0.0.0.0', port=int(os.environ.get("PORT", 5001)))
