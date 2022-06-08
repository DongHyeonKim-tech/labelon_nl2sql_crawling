import os
from random import sample
import re
import time
import json
import pandas as pd
from pymysql import NULL
import requests
import datetime
import jaydebeapi
import xml.etree.ElementTree as ET
import xml.etree.cElementTree as etree

from urllib.parse import unquote
from tqdm import tqdm
from bs4 import BeautifulSoup

# from requests_html import HTMLSession
# 서울 열린데이터 광장 데이터 수집 2차
# 세부 항목 수집 및 OpenAPI 접속 후 데이터 수집

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

    basic_url = "http://data.seoul.go.kr/dataList/"

    # 인증키 : 716a72767867636833306d6a536955
    
    auth_key = '716a72767867636833306d6a536955'

    # 서울 열린데이터 광장 데이터 & 수집 데이터 타입이 OpenAPI 인 경우에 데이터 수집
    sql = "SELECT ID, COLLECT_SITE_ID, DATA_NAME, DATA_ORIGIN_KEY, COLLECT_DATA_TYPE, COLLECT_URL_LINK, IS_COLLECT_YN "\
        + "  FROM DATA_BASIC_INFO "\
        + " WHERE COLLECT_SITE_ID = 1 "\
        + "   AND COLLECT_DATA_TYPE = 'OpenAPI' "\
        + "   AND ROWNUM <= 20 "

    cur.execute(sql)
    sql_result = cur.fetchall()

    df_0_data = pd.DataFrame(sql_result).reset_index()
    df_0_data.columns = ['index', 'id', 'collect_site_id', 'data_name', 'data_origin_key', 'collect_data_type', 'collect_url_link', 'is_collect_yn']
    df_0_data = df_0_data.drop(columns=['index'], axis=1)

    column_idx = 0

    # 상세 페이지 접속
    for idx, row in df_0_data.iterrows():
        
        # OpenAPI 만 활용
        data_basic_id = row['id']

        data_type = row['collect_data_type']
        detail_url = row['collect_url_link']

        print(detail_url)

        # 상세 정보 업데이트
        detail_response = requests.get(f'{detail_url}')
        detail_html = detail_response.text
        detail_soup = BeautifulSoup(detail_html, 'html.parser')

        # 상세 정보
        detail_info = detail_soup.find("div", {"class": "tbl-base-d align-l only-d2"})
        detail_td_list = detail_info.find_all("td")

        # OPEN API 샘플 정보
        openapi_url = basic_url + 'openApiView.do?infId=' + row['data_origin_key'] + '&srvType=A'

        print(openapi_url)

        openapi_response = requests.get(f'{openapi_url}')
        openapi_html = openapi_response.text
        openapi_soup = BeautifulSoup(openapi_html, 'html.parser')

        basic_openapi_info = openapi_soup.find_all("div", {"class": "tbl-base-s"})

        # Sample OpenAPI URL 
        sample_openapi_url = basic_openapi_info[0].find("a", href=True)['href']

        table_info_list = sample_openapi_url.split('/sample/xml/')[1]
        master_openapi_url = sample_openapi_url.replace("/sample/", "/" + auth_key + "/")

        master_openapi_url = master_openapi_url.rsplit('/', 3)[0]
        
        # 데이터 물리 저장 테이블 정보
        # 논리 테이블 영어
        table_logical_name = table_info_list.split('/')[0]
        table_logical_name = re.sub(r'(?<!^)(?=[A-Z])', '_', table_logical_name).upper()

        table_physical_name = "REAL_" + str(data_basic_id).rjust(6, "0")

        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        insert_table_sql = """INSERT INTO MANAGE_PHYSICAL_TABLE (ID, DATA_BASIC_ID, LOGICAL_TABLE_HANGULE, LOGICAL_TABLE_ENGLISH, """\
                           """PHYSICAL_TABLE_NAME, PYHSICAL_CREATED_YN, TABLE_CREATE_DATE, DATA_INSERTED_YN) """\
                           """VALUES (SEQ_MTABLE.NEXTVAL, ?, ?, ?, ?, ?, ?, ?)"""
        
        # SEQ_MTABLE.NEXTVAL, 데이터 기본 일련번호, 논리 테이블 한글, 논리 테이블 영어, 물리 테이블명
        # 데이터 생성 유무(N), 테이블 생성일, 데이터 입력 유무(N)

        # 데이터 물리 저장 테이블 정보 INSERT
        insert_table_values = (data_basic_id, row['data_name'], table_logical_name, table_physical_name, 'N', timestamp, 'N')
        cur.execute(insert_table_sql, insert_table_values)

        # TO-DO >> DATA SELECT 필요
        
        select_table_sql = """SELECT ID FROM MANAGE_PHYSICAL_TABLE WHERE DATA_BASIC_ID = {} """.format(data_basic_id)
        
        cur.execute(select_table_sql)
        manage_table_result = cur.fetchall()
        manage_table_id = manage_table_result[0][0]
        

        # 테이블 생성
        create_table_sql = "CREATE TABLE " + table_physical_name + " (ID NUMBER, "

        temp_sql = ""
        for i in range(1, 101):
            if i <= 50:
                col_name = "COL_" + str(i).rjust(3, "0")
                temp_sql += col_name + " VARCHAR(2000), "
            else: 
                col_name = "COL_" + str(i).rjust(3, "0")
                temp_sql += col_name + " NUMBER(38)"

                if i == 100:
                    temp_sql += ") "
                else:
                    temp_sql += ", "

        create_table_sql = create_table_sql + temp_sql        
        print(create_table_sql)
        
        # 테이블 생성 쿼리 실행
        cur.execute(create_table_sql)

        # TO-DO >> MANGAE_TABLE_INFO 업데이트 진행 필요!!!







        # TO-DO >> 데이터 기본 정보 업데이트 진행 필요!!!
        
        # 출력값 coulmn_info
        data_output_list = basic_openapi_info[2].find_all("td")
        
        real_data_column = "(ID, "

        for idx in range(0, len(data_output_list)):
            if idx % 3 == 0:
                if data_output_list[idx].text != "공통":
                    # print(str(idx+1) + " : " + data_output_list[idx].text + " : " + data_output_list[idx+1].text + " : " + data_output_list[idx+2].text)

                    logical_column_hangule = data_output_list[idx+2].text
                    logical_column_english = data_output_list[idx+1].text
                    physical_column_order = int(data_output_list[idx].text)
                    physical_column_name = "COL_" + str(physical_column_order).rjust(3, "0")
                   
                    ts_column = time.time()
                    timestamp_column = datetime.datetime.fromtimestamp(ts_column).strftime('%Y-%m-%d %H:%M:%S')

                    insert_column_sql = """INSERT INTO MANAGE_PHYSICAL_COLUMN (ID, DATA_PHYSICAL_ID, LOGICAL_COLUMN_HANGULE, LOGICAL_COLUMN_ENGLISH, """\
                                        """PHYSICAL_COLUMN_ORDER, PHYSICAL_COLUMN_NAME, IS_USE_YN, IS_CREATED_YN, COLUMN_CREATE_DATE) """\
                                        """VALUES (SEQ_MCOLUMN.NEXTVAL, ?, ?, ?, ?, ?, ?, ?, ?)"""
                    
                    
                    insert_column_values = (manage_table_id, logical_column_hangule, logical_column_english,
                                            physical_column_order, physical_column_name, 'Y', 'N', timestamp_column)
                    
                    real_data_column += physical_column_name + ", "

                    cur.execute(insert_column_sql, insert_column_values)

                    column_idx += 1

        time.sleep(2)

        # DATA INSERT 컬럼 모음
        real_data_column = real_data_column.strip()
        real_data_column = real_data_column[:-1]
        real_data_column = real_data_column + ")"
       
        # OpenAPI 
        print("-------------------------------------------------------------------")
        print(sample_openapi_url)
        print(master_openapi_url)

        sample_openapi_response = requests.get(f'{sample_openapi_url}')
        sample_openapi_content = sample_openapi_response.content

        test_string = sample_openapi_content.decode('utf-8')
        test_xml = ET.fromstring(test_string)

        # 1000개 까지 검색 가능
        list_total_count = 0

        for child in test_xml:            
            if child.tag == 'list_total_count':
                list_total_count = int(child.text)

                print(child.tag, child.attrib)
                print(child.text)
                print("------------------------------")

        # 최대 검색 건수 : 1,000건
        openapi_search_num = (list_total_count // 1000) + 1
        
        save_xml_path = './data/seoul/'
        
        df_save_data = pd.DataFrame()
        
        for i in range(0, openapi_search_num):
            
            time.sleep(2)

            start_index = 1 + 1000 * i
            end_index = 1000 * (i+1)
            
            if i == list_total_count // 1000:
                # 마지막인 경우
                end_index = list_total_count
            else:
                end_index = 1000 * (i+1)

            print(str(start_index) + " ~ " + str(end_index))

            search_openapi_url = master_openapi_url + '/' + str(start_index ) + '/' + str(end_index) + '/'

            print(search_openapi_url)
            search_openapi_response = requests.get(f'{search_openapi_url}')
            search_openapi_content = search_openapi_response.content

            save_content = search_openapi_content.decode('utf-8')

            error_check = 0

            # 예외 처리 (OpenAPI 리턴 결과가 이상한 경우 제외)
            try:
                save_xml = ET.fromstring(save_content)
                error_check = 1
            except:
                error_check = -1

            print("error_check = " + str(error_check))

            if error_check == 1:
                # 에러가 아닐 경우에만 데이터 입력 진행
                save_file_name = table_physical_name + '_' + str(i) + '.xml'

                # TO-DO : DATA INSERT SQL 쿼리
                # real_data_column

                # 1000개 까지 검색 가능

                temp_id = start_index

                for child in save_xml:
                
                    if child.tag == 'row':
                        
                        insert_column_num = len(child)

                        insert_data_values = [temp_id]

                        for i in range(0, insert_column_num):

                            temp_text = child[i].text

                            if temp_text == None:
                                temp_text = NULL
                            else:
                                temp_text = temp_text.replace("\n", " ")
                                temp_text = temp_text.replace("\r", " ")
                                
                                if len(temp_text.encode('utf-8')) > 1990:
                                    temp_text = ""

                            insert_data_values.append(temp_text)

                        insert_data_values = tuple(insert_data_values)


                        temp_id += 1
                        
                        df_save_data = df_save_data.append(pd.Series(insert_data_values), ignore_index=True)
                        
                        insert_data_sql = "INSERT INTO " + table_physical_name + " " + real_data_column + " VALUES " + str(insert_data_values)
                        # print(insert_data_sql)
                        cur.execute(insert_data_sql)

                # 파일 저장
                file_xml = open(save_xml_path + save_file_name, 'w', encoding="UTF-8")
                file_xml.write(save_content)
                file_xml.close()
                time.sleep(3)


        print("-----------------------------------------------------------------------------------------")    

        print("-----------------------------------------------------------------------------------------")















    

    print('=========================================================================================')    
    print('=========================================================================================')
