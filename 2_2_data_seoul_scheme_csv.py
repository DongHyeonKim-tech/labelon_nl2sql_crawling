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
import csv

from urllib.parse import unquote
from tqdm import tqdm
from bs4 import BeautifulSoup

# from requests_html import HTMLSession
# 서울 열린데이터 광장 데이터 수집 2차
# 세부 항목 수집 및 OpenAPI 접속 후 데이터 수집

# 구조만 만들고 데이터를 TMP테이블에 데이터를 수동으로 넣기위한 사전작업
# MANAGE_PHYSICAL_TABLE, MANAGE_PHYSICAL_COLUMN 테이블에만 데이터 생성
# CSV 파일의 첫번째 행으로 컬럼 정보를 생성한다.

if __name__ == "__main__":

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
    auth_key_train = '764d6a4c56676368383658564f4e4e'

    # 서울 열린데이터 광장 데이터 & 수집 데이터 타입이 OpenAPI 인 경우에 데이터 수집
    sql = "SELECT ID, COLLECT_SITE_ID, DATA_NAME, DATA_ORIGIN_KEY, COLLECT_DATA_TYPE, COLLECT_URL_LINK, IS_COLLECT_YN " \
          + "  FROM DATA_BASIC_INFO " \
          + " WHERE COLLECT_SITE_ID = 1 " \
          + "   AND IS_COLLECT_YN = 'N' " \
          + "   AND ID IN (5758) "

    cur.execute(sql)
    sql_result = cur.fetchall()

    df_0_data = pd.DataFrame(sql_result).reset_index()
    df_0_data.columns = ['index', 'id', 'collect_site_id', 'data_name', 'data_origin_key', 'collect_data_type',
                         'collect_url_link', 'is_collect_yn']
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

        basic_openapi_info = detail_soup.find_all("h1", {"class": "main-content-tit"})

        print(basic_openapi_info)

        # 데이터 물리 저장 테이블 정보
        # 논리 테이블 영어
        table_logical_name = basic_openapi_info[0].text
        table_logical_name = re.sub(r'(?<!^)(?=[A-Z])', '_', table_logical_name).upper()

        table_physical_name = "NLDATA_" + str(data_basic_id).rjust(6, "0")
        table_orig_name = "TMP_" + str(data_basic_id).rjust(6, "0")

        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

        insert_table_sql = """INSERT INTO MANAGE_PHYSICAL_TABLE (ID, DATA_BASIC_ID, LOGICAL_TABLE_KOREAN, LOGICAL_TABLE_ENGLISH, """ \
                           """PHYSICAL_TABLE_NAME, PHYSICAL_CREATED_YN, TABLE_CREATE_DATE, DATA_INSERTED_YN, ORIG_TABLE_NAME, START_IDX) """ \
                           """VALUES (SEQ_MTABLE.NEXTVAL, ?, ?, ?, ?, ?, ?, ?, ?, 0)"""

        # SEQ_MTABLE.NEXTVAL, 데이터 기본 일련번호, 논리 테이블 한글, 논리 테이블 영어, 물리 테이블명
        # 데이터 생성 유무(N), 테이블 생성일, 데이터 입력 유무(N)

        # 데이터 물리 저장 테이블 정보 INSERT
        insert_table_values = (
        data_basic_id, row['data_name'], "", table_physical_name, 'N', timestamp, 'N', table_orig_name)
        cur.execute(insert_table_sql, insert_table_values)

        # TO-DO >> DATA SELECT 필요

        select_table_sql = """SELECT MAX(ID) FROM MANAGE_PHYSICAL_TABLE WHERE DATA_BASIC_ID = {} """.format(
            data_basic_id)

        cur.execute(select_table_sql)
        manage_table_result = cur.fetchall()
        manage_table_id = manage_table_result[0][0]

        # 테이블 생성
        create_table_sql = "CREATE TABLE " + table_orig_name + " (ID NUMBER, "

        temp_sql = ""
        for i in range(1, 101):
            col_name = "COL_" + str(i).rjust(3, "0")
            temp_sql += col_name + " VARCHAR(65532) "

            if i == 100:
                temp_sql += ") "
            else:
                temp_sql += ", "

        create_table_sql = create_table_sql + temp_sql
        print(create_table_sql)

        # 테이블 생성 쿼리 실행
        cur.execute(create_table_sql)

        save_xml_path = './data/seoul'

        list_total_count = 0
        row = 0

        f = open(save_xml_path + "/" + table_orig_name + '.csv', 'r')
        rdr = csv.reader(f)

        for line in rdr:
            if row != 0:
                break
            if row == 0:
                for idx in range(0, len(line)):
                    logical_column_hangule = line[idx]
                    logical_column_english = ""
                    physical_column_order = idx + 1
                    physical_column_name = "COL_" + str(physical_column_order).rjust(3, "0")

                    ts_column = time.time()
                    timestamp_column = datetime.datetime.fromtimestamp(ts_column).strftime('%Y-%m-%d %H:%M:%S')

                    insert_column_sql = """INSERT INTO MANAGE_PHYSICAL_COLUMN (ID, DATA_PHYSICAL_ID, LOGICAL_COLUMN_KOREAN, LOGICAL_COLUMN_ENGLISH, """ \
                                        """PHYSICAL_COLUMN_ORDER, PHYSICAL_COLUMN_NAME, IS_USE_YN, IS_CREATED_YN, COLUMN_CREATE_DATE) """ \
                                        """VALUES (SEQ_MCOLUMN.NEXTVAL, ?, ?, ?, ?, ?, ?, ?, ?)"""

                    insert_column_values = (
                        manage_table_id, logical_column_hangule, logical_column_english, physical_column_order,
                        physical_column_name, 'Y', 'N', timestamp_column)

                    cur.execute(insert_column_sql, insert_column_values)

                    column_idx += 1

            row += 1

        time.sleep(2)

        print("-----------------------------------------------------------------------------------------")
        print("-----------------------------------------------------------------------------------------")

        update_basic_sql = """UPDATE DATA_BASIC_INFO SET IS_COLLECT_YN = 'Y' """ \
                           """WHERE ID = ? """

        update_basic_values = (data_basic_id,)

        cur.execute(update_basic_sql, update_basic_values)

    print('=========================================================================================')
    print('=========================================================================================')
