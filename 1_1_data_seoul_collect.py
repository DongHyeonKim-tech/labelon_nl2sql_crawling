import os
import re
import time
import json
import pandas as pd
import requests
import datetime
import jaydebeapi

from urllib.parse import unquote
from tqdm import tqdm
from bs4 import BeautifulSoup

# from requests_html import HTMLSession
# 서울 열린데이터 광장 데이터 수집 1차
# 리스트 수집

"""
def render_JS(URL):c
    session = HTMLSession()
    r = session.get(URL)
    r.html.render()
    return r.html.text
"""

if __name__=="__main__":

    print('==== Tibero DB Connection ===============================================================')

    # Tibero DB Connection
    conn = jaydebeapi.connect(
        "com.tmax.tibero.jdbc.TbDriver",
        "jdbc:tibero:thin:@172.7.0.23:8629:tibero",
        ["labelon", "euclid!@)$labelon"],
        "tibero6-jdbc.jar",
    )
    cur = conn.cursor()

    print('===== Tibero DB Test ====================================================================')
    sql = "SELECT * FROM DATA_BASIC_INFO"
    cur.execute(sql)
    print(cur.fetchall())
    print('=========================================================================================')
    
    param_list = []

    # 서울 데이터 URL
    basic_url = "http://data.seoul.go.kr/dataList/"
    search_url = basic_url + "datasetList.do"

    # post 데이터
    post_data = {"pageIndex": "1", "sortColBy": "R", "datasetKind": "1"}

    response = requests.post(f'{search_url}', post_data)

    html = response.text
    soup = BeautifulSoup(html, 'html.parser')

    search_result = soup.find("div", {"class": "search-result"})
    search_result = int(search_result.find("strong").text.replace(",", ""))
    
    temp = search_result % 10
    total_index = int(search_result / 10) + (0 if search_result % 10 == 0 else 1)
    
    print("search_result = " + str(search_result) + ", total_index = " + str(total_index))

    # 일부 데이터로 테스트
    # total_index = 3
    
    # 저장 데이셋
    df_data = pd.DataFrame(columns=['title', 'data_rel', 'data_type', 'make_url'])

    # Sleep 시간 랜덤
    list_random = [5.02, 5.51, 4.93, 5.35, 5.17, 4.81, 5.77, 5.23, 4.56, 5.49]
    len_random = len(list_random)
    
    # 리스트에 있는 데이터셋 기본 항목 저장
    for i in tqdm(range(0, total_index)):

        i_sleep = i % len_random
        time.sleep(list_random[i_sleep])

        post_index_data = {"pageIndex": i+1, "sortColBy": "R", "datasetKind": "1"}

        print(post_index_data)
        
        response_index = requests.post(f'{search_url}', post_index_data)
        html_index = response_index.text

        soup_index = BeautifulSoup(html_index, 'html.parser')
        dl_list = soup_index.find_all("dl", {"class": "type-b"})

        for dl_one in dl_list:
            
            chk_openapi = [0, 0]            # OpenAPI 인 경우

            # Insert Data 정의
            ts = time.time()
            timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

            # Title 정보
            alinks = dl_one.find_all("a", {"class": "goView"})

            title_name = alinks[0].text.strip()
            data_rel = alinks[0].attrs['data-rel'].split("/")[0]
            
            provide_list_data_type = []
            provide_list_url = []

            collect_data_type = ""
            collect_url = ""

            # Data 타입 정보
            buttons = dl_one.find_all("button")

            for i in range(len(buttons)):
                
                button_text = buttons[i].text.strip()
                
                provide_list_data_type.append(button_text)
                provide_list_url.append(buttons[i].attrs['data-rel'])

                if button_text == 'OpenAPI':
                    chk_openapi[0] = 1      # OpenAPI 인 경우 1
                    chk_openapi[1] = i      # OpenAPI 리스트에서의 위치

            if chk_openapi[0] == 1:
                # Open API 인 경우
                # print(chk_openapi)
                collect_data_type = 'OpenAPI'
                collect_url = basic_url + provide_list_url[chk_openapi[1]]
            else:
                # Open API 가 아닌 경우에는 0번째 타입으로 정의
                collect_data_type = provide_list_data_type[0]
                collect_url = basic_url + provide_list_url[0]
                
                # print(chk_openapi)
            
            # DATA_BASIC_INFO 데이터 입력
            insert_sql = """INSERT INTO DATA_BASIC_INFO (ID, COLLECT_SITE_ID, DATA_NAME, DATA_ORIGIN_KEY, PROVIDE_DATA_TYPE, PROVIDE_URL_LINK, """\
                         """COLLECT_DATA_TYPE, COLLECT_URL_LINK, IS_COLLECT_YN, SYSTEM_REGISTER_DATE) VALUES (SEQ_BASIC.NEXTVAL, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

            insert_values = (1, title_name, data_rel, str(provide_list_data_type), str(provide_list_url), collect_data_type, collect_url, 'N', timestamp)
            cur.execute(insert_sql, insert_values)
            
            df_data = df_data.append({'title': title_name, 'data_rel': data_rel, 'data_type': provide_list_data_type, 'make_url': provide_list_url}, ignore_index=True)

    cur.close()

    print('=========================================================================================')
    print('===== END ===============================================================================')
    print('=========================================================================================')