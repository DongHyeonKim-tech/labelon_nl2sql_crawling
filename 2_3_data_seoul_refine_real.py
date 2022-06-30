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

    sql = "SELECT A.ID, A.DATA_BASIC_ID, A.LOGICAL_TABLE_KOREAN, A.PHYSICAL_TABLE_NAME, A.ORIG_TABLE_NAME "\
        + "  FROM MANAGE_PHYSICAL_TABLE A" \
        + " INNER JOIN DATA_BASIC_INFO B ON A.DATA_BASIC_ID = B.ID AND B.IS_COLLECT_YN = 'Y' "\
        + " WHERE A.DATA_INSERTED_YN = 'Y' "\
        + "   AND A.PHYSICAL_CREATED_YN = 'N' "\
        + " ORDER BY A.DATA_BASIC_ID "

    cur.execute(sql)
    sql_result = cur.fetchall()

    df_0_data = pd.DataFrame(sql_result).reset_index()
    df_0_data.columns = ['index', 'id', 'data_basic_id', 'logical_table_korean', 'physical_table_name', 'orig_table_name']
    df_0_data = df_0_data.drop(columns=['index'], axis=1)

    column_idx = 0

    for idx, row in df_0_data.iterrows():

        table_id = row['id']
        orig_table_name = row['orig_table_name']
        real_table_name = row['physical_table_name']

        print(orig_table_name + " " + str(table_id))

        sql = "SELECT ID, DATA_PHYSICAL_ID, LOGICAL_COLUMN_KOREAN, PHYSICAL_COLUMN_NAME, PHYSICAL_COLUMN_TYPE "\
              + "  FROM MANAGE_PHYSICAL_COLUMN "\
              + " WHERE DATA_PHYSICAL_ID = '" + str(table_id) + "' " \
              + " ORDER BY PHYSICAL_COLUMN_ORDER "

        print(sql)
        cur.execute(sql)
        sql_result = cur.fetchall()

        df_1_data = pd.DataFrame(sql_result).reset_index()
        df_1_data.columns = ['index', 'id', 'data_physical_id', 'logical_column_korean', 'physical_column_name', 'physical_column_type']
        df_1_data = df_1_data.drop(columns=['index'], axis=1)

        create_table_sql = "CREATE TABLE " + real_table_name + " (ID NUMBER, "
        temp_sql = ""
        temp2_sql = " ID ,"

        for idx2, row2 in df_1_data.iterrows():

            col_id = row2['id']
            col_name = row2['physical_column_name']
            col_type = ""
            print(col_name)

            sql = "  SELECT COUNT(1) AS CNT " \
                  + "  FROM " + orig_table_name  \
                  + " WHERE " + col_name + " IS NOT NULL "

            cur.execute(sql)
            chk_result = cur.fetchall()
            chk_cnt_o = chk_result[0][0]

            if chk_cnt_o != 0:
                sql = "  SELECT COUNT(1) AS CNT " \
                  + "  FROM " + orig_table_name  \
                  + " WHERE REGEXP_INSTR(" + col_name + ", '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.{1}') = 1"  \
                  + "   AND " + col_name + " IS NOT NULL "

                cur.execute(sql)
                chk_result = cur.fetchall()
                chk_cnt = chk_result[0][0]

                if chk_cnt_o != 0 and chk_cnt == chk_cnt_o:
                    col_type = "DATETIME"

            if chk_cnt_o != 0 and col_type == "":
                sql = "  SELECT COUNT(1) AS CNT " \
                  + "  FROM " + orig_table_name  \
                  + " WHERE REGEXP_INSTR(" + col_name + ", '^\d{4}\d{2}\d{2}\d{2}\d{2}\d{2}\d{2}\d{2}') = 1" \
                  + "   AND LENGTH(" + col_name + ") = 12 " \
                  + "   AND " + col_name + " IS NOT NULL "

                cur.execute(sql)
                chk_result = cur.fetchall()
                chk_cnt = chk_result[0][0]

                if chk_cnt_o != 0 and chk_cnt == chk_cnt_o:
                    col_type = "DATETIMESTR"

            if chk_cnt_o != 0 and col_type == "":
                sql = "  SELECT COUNT(1) AS CNT " \
                  + "  FROM " + orig_table_name  \
                  + " WHERE REGEXP_INSTR(" + col_name + ", '^\d{4}\d{2}\d{2}\d{2}\d{2}\d{2}\d{2}\d{2}\d{2}') = 1"  \
                  + "   AND LENGTH(" + col_name + ") = 14 " \
                  + "   AND " + col_name + " IS NOT NULL "

                cur.execute(sql)
                chk_result = cur.fetchall()
                chk_cnt = chk_result[0][0]

                if chk_cnt_o != 0 and chk_cnt == chk_cnt_o:
                    col_type = "DATETIMESTR2"

            if chk_cnt_o != 0 and col_type == "":
                sql = "SELECT COUNT(1) AS CNT " \
                      + "  FROM " + orig_table_name \
                      + " WHERE REGEXP_INSTR(" + col_name + ", '^((19|20)\d{2})(0[1-9]|1[012])' ) = 1 "\
                      + "   AND LENGTH(" + col_name + ") = 6 "\
                      + "   AND " + col_name + " IS NOT NULL "

                cur.execute(sql)
                chk_result = cur.fetchall()
                chk_cnt = chk_result[0][0]

                if chk_cnt_o != 0 and chk_cnt_o == chk_cnt:
                    col_type = "DATEYYMM"

            if chk_cnt_o != 0 and col_type == "":
                sql = "SELECT COUNT(1) AS CNT " \
                      + "  FROM " + orig_table_name \
                      + " WHERE REGEXP_INSTR(" + col_name + ", '^((19|20)\d{2})/(0[1-9]|1[012])' ) = 1 "\
                      + "   AND LENGTH(" + col_name + ") = 7 "\
                      + "   AND " + col_name + " IS NOT NULL "

                cur.execute(sql)
                chk_result = cur.fetchall()
                chk_cnt = chk_result[0][0]

                if chk_cnt_o != 0 and chk_cnt_o == chk_cnt:
                    col_type = "DATEYYMMS"

            if chk_cnt_o != 0 and col_type == "":
                sql = "SELECT COUNT(1) AS CNT " \
                      + "  FROM " + orig_table_name \
                      + " WHERE REGEXP_INSTR(" + col_name + ", '^((19|20)\d{2})' ) = 1 "\
                      + "   AND LENGTH(" + col_name + ") = 4 "\
                      + "   AND " + col_name + " IS NOT NULL "

                cur.execute(sql)
                chk_result = cur.fetchall()
                chk_cnt = chk_result[0][0]

                if chk_cnt_o != 0 and chk_cnt_o == chk_cnt:
                    col_type = "DATEYY"

            if chk_cnt_o != 0 and col_type == "":
                sql = "SELECT COUNT(1) AS CNT " \
                      + "  FROM " + orig_table_name \
                      + " WHERE REGEXP_INSTR(" + col_name + ", '^((19|20)\d{2})(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])' ) = 1 "\
                      + "   AND LENGTH(" + col_name + ") = 8 "\
                      + "   AND " + col_name + " IS NOT NULL "

                cur.execute(sql)
                chk_result = cur.fetchall()
                chk_cnt = chk_result[0][0]

                if chk_cnt_o != 0 and chk_cnt_o == chk_cnt:
                    col_type = "DATEONLY"

            if chk_cnt_o != 0 and col_type == "":
                sql = "SELECT COUNT(1) AS CNT " \
                      + "  FROM " + orig_table_name \
                      + " WHERE REGEXP_INSTR(" + col_name + ", '^((19|20)\d{2})-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])' ) = 1 "\
                      + "   AND LENGTH(" + col_name + ") = 10 "\
                      + "   AND " + col_name + " IS NOT NULL "

                cur.execute(sql)
                chk_result = cur.fetchall()
                chk_cnt = chk_result[0][0]

                if chk_cnt_o != 0 and chk_cnt_o == chk_cnt:
                    col_type = "DATE"

            if chk_cnt_o != 0 and col_type == "":

                sql = "SELECT COUNT(1) AS CNT " \
                      + "  FROM " + orig_table_name \
                      + " WHERE REGEXP_INSTR(" + col_name + ", '^((19|20)\d{2}).(0[1-9]|1[012]).(0[1-9]|[12][0-9]|3[01])' ) = 1"\
                      + "   AND LENGTH(" + col_name + ") = 10 "\
                      + "   AND " + col_name + " IS NOT NULL "

                cur.execute(sql)
                chk_result = cur.fetchall()
                chk_cnt = chk_result[0][0]

                if chk_cnt_o != 0 and chk_cnt_o == chk_cnt:
                    col_type = "DATEDOT"

            if chk_cnt_o != 0 and col_type == "":

                sql = "SELECT COUNT(1) AS CNT " \
                      + "  FROM " + orig_table_name \
                      + " WHERE REGEXP_INSTR(" + col_name + ", '^((19|20)\d{2}).(0[1-9]|1[012]).(0[1-9]|[12][0-9]|3[01]).\d{2}' ) = 1"\
                      + "   AND LENGTH(" + col_name + ") = 13 "\
                      + "   AND " + col_name + " IS NOT NULL "

                cur.execute(sql)
                chk_result = cur.fetchall()
                chk_cnt = chk_result[0][0]

                if chk_cnt_o != 0 and chk_cnt_o == chk_cnt:
                    col_type = "DATEDOT2"

            if chk_cnt_o != 0 and col_type == "":

                sql = "SELECT COUNT(1) AS CNT " \
                      + "  FROM " + orig_table_name \
                      + " WHERE REGEXP_LIKE(" + col_name + ", '^[+-]?\d*(\.?\d*)$' )"\
                      + "   AND " + col_name + " IS NOT NULL "

                cur.execute(sql)
                chk_result = cur.fetchall()
                chk_cnt = chk_result[0][0]

                if chk_cnt_o != 0 and chk_cnt_o == chk_cnt:
                    col_type = "NUMBER"

            if chk_cnt_o != 0 and col_type == "":

                sql = "SELECT COUNT(1) AS CNT " \
                      + "  FROM " + orig_table_name \
                      + " WHERE REGEXP_LIKE(" + col_name + ", '[^0-9]' )"\
                      + "   AND " + col_name + " IS NOT NULL "

                cur.execute(sql)
                chk_result = cur.fetchall()
                chk_cnt = chk_result[0][0]

                if chk_cnt_o != 0 and chk_cnt_o == chk_cnt:
                    col_type = "VARCHAR"

            if col_type == "":
                col_type = "VARCHAR"

            update_column_sql = """UPDATE MANAGE_PHYSICAL_COLUMN SET PHYSICAL_COLUMN_TYPE = ? """ \
                                """WHERE ID = ? """

            update_column_values = (col_type, col_id)

            cur.execute(update_column_sql, update_column_values)

            # print(col_type)
            if idx2 != 0:
                temp_sql += ","
                temp2_sql += ","

            if col_type == "DATE":
                temp2_sql += "NVL2(" + col_name + ",REPLACE(" + col_name + ",'-','') || '000000', " + col_name + ")"
            elif col_type == "DATEDOT":
                temp2_sql += "NVL2(" + col_name + ",REPLACE(" + col_name + ",'.','') || '000000', " + col_name + ")"
            elif col_type == "DATEDOT2":
                temp2_sql += "NVL2(" + col_name + ",REPLACE(" + col_name + ",'.','') || '0000', " + col_name + ")"
            elif col_type == "DATEONLY":
                temp2_sql += "NVL2(" + col_name + ", " + col_name + " || '000000', " + col_name + ")"
            elif col_type == "DATEYYMM":
                temp2_sql += "NVL2(" + col_name + ", " + col_name + " || '00000000', " + col_name + ")"
            elif col_type == "DATEYYMMS":
                temp2_sql += "NVL2(" + col_name + ",REPLACE(" + col_name + ",'/','') || '00000000', " + col_name + ")"
            elif col_type == "DATEYY":
                temp2_sql += "NVL2(" + col_name + ", " + col_name + " || '0000000000', " + col_name + ")"
            elif col_type == "DATETIMESTR":
                temp2_sql += "NVL2(" + col_name + ", " + col_name + " || '00', " + col_name + ")"
            elif col_type == "DATETIME":
                temp2_sql += "REPLACE(REPLACE(REPLACE(SUBSTR(" + col_name + ", 0, INSTR(" + col_name + ",'.') - 1), '-', ''), ':',''), ' ','')"
            else:
                temp2_sql += col_name

            if col_type == "VARCHAR":
                temp_sql += col_name + " " + col_type + "(65532)"
            elif col_type == "NUMBER":
                temp_sql += col_name + " " + col_type
            elif col_type == "DATE":
                temp_sql += col_name + " VARCHAR(14) "
            elif col_type == "DATEDOT":
                temp_sql += col_name + " VARCHAR(14) "
            elif col_type == "DATEDOT2":
                temp_sql += col_name + " VARCHAR(14) "
            elif col_type == "DATETIME":
                temp_sql += col_name + " VARCHAR(14) "
            elif col_type == "DATETIMESTR":
                temp_sql += col_name + " VARCHAR(14) "
            elif col_type == "DATETIMESTR2":
                temp_sql += col_name + " VARCHAR(14) "
            elif col_type == "DATEONLY":
                temp_sql += col_name + " VARCHAR(14) "
            elif col_type == "DATEYYMM":
                temp_sql += col_name + " VARCHAR(14) "
            elif col_type == "DATEYYMMS":
                temp_sql += col_name + " VARCHAR(14) "
            elif col_type == "DATEYY":
                temp_sql += col_name + " VARCHAR(14) "

        temp_sql += ") "
        create_table_sql = create_table_sql + temp_sql
        print(create_table_sql)

        # 테이블 생성 쿼리 실행
        cur.execute(create_table_sql)

        copy_sql = " INSERT INTO " + real_table_name + " SELECT " + temp2_sql + " FROM " + orig_table_name

        print(copy_sql)
        cur.execute(copy_sql)

        update_column_sql = """UPDATE MANAGE_PHYSICAL_TABLE SET PHYSICAL_CREATED_YN = 'Y' """ \
                            """WHERE ID = ? """

        update_column_values = (table_id, )

        cur.execute(update_column_sql, update_column_values)

        update_column_sql = " GRANT SELECT ON LABELON." + real_table_name + " to NL2SQL"

        cur.execute(update_column_sql)

        update_column_sql = " CREATE PUBLIC SYNONYM " + real_table_name + " for LABELON." + real_table_name

        cur.execute(update_column_sql)

        remove_sql = " DROP TABLE " + orig_table_name
        cur.execute(remove_sql)

        time.sleep(2)