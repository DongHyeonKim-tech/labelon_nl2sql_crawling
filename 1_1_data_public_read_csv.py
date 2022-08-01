import os
import csv
import jaydebeapi
import time
import shutil

if __name__ =="__main__":

    conn = jaydebeapi.connect(
        "com.tmax.tibero.jdbc.TbDriver",
        "jdbc:tibero:thin:@172.7.0.23:8629:tibero",
        ["labelon", "euclid!@)$labelon"],
        "tibero6-jdbc.jar",
    )
    cur = conn.cursor()

    path_dir = 'C:/euclid/nl2sql/ws'

    file_list = os.listdir(path_dir)
    
    destination_path = 'C:/euclid/nl2sql/done'

    ## CSV 파일 읽기
    for file in file_list:
        try:
            f = open(path_dir+ '/' + file, 'r', encoding='cp949')
            rdr = csv.reader(f)
            
            splited_name = file.split('.')[0].split('_')
            extn = file.split('.')[-1]
            name = ''
            for i in range(0, len(splited_name)-1):
                name += f'_{splited_name[i]}' if name != '' else f'{splited_name[i]}'
            data_list = []
            for line in rdr:
                defined_line = [l.replace('\x00', '') for l in line]
                data_list.append(defined_line)

            f.close()
        except UnicodeDecodeError as e:
            continue
        
        ## MANAGE_PHYSICAL_TABLE 테이블 데이터 insert    
        table_max_id_sql = "select max(id) from MANAGE_PHYSICAL_TABLE"
        cur.execute(table_max_id_sql)
        table_max_id = cur.fetchone()[0] + 1
        
        print("######################################")
        print(f'file name: {name}')
        data_basic_sql = f"select id from DATA_BASIC_INFO where DATA_NAME='{name}' and collect_sitd_id='2' and is_collect_yn='N'"
        cur.execute(data_basic_sql)
        data_basic_fetch = cur.fetchone()
        len_rows = len(data_list) - 1
        # 파일 명을 가진 데이터가 DATA_BASIC_INFO에 있고, 파일 확장자가 CSV이면 데이터 insert 시작
        if data_basic_fetch and extn == 'csv':
            print("-----------------------------------")
            print('start insert')
            data_basic_id = data_basic_fetch[0]
            physical_table_name = 'NLDATA_' + str(data_basic_id).rjust(6, '0')
            orig_table_name = 'TMP100_' + str(data_basic_id)
            logical_table_english = 'DATA_TMP_' + str(data_basic_id).rjust(6, '0')
            TABLE_SQL = f"insert into MANAGE_PHYSICAL_TABLE(ID,DATA_BASIC_ID, LOGICAL_TABLE_KOREAN, PHYSICAL_CREATED_YN, DATA_INSERTED_YN, DATA_INSERT_ROW, TARGET_ROWS, physical_table_name, orig_table_name, logical_table_english) VALUES('{table_max_id}', '{data_basic_id}', '{name}', 'N', 'N', '0', '{len_rows}', '{physical_table_name}', '{orig_table_name}', '{logical_table_english}')"
            cur.execute(TABLE_SQL)
            
            ## MANAGE_PHYSICAL_COLUMN 테이블 데이터 insert
            print("-----------------------------------")
            print('MANAGE_PHYSICAL_TABLE inserted')
            column = data_list[0]
            column_max_id_sql = "select max(id) from MANAGE_PHYSICAL_COLUMN"
            cur.execute(column_max_id_sql)
            column_max_id = cur.fetchone()[0] + 1
            physical_column_order = 1
            type_sample_data = data_list[1]
            for col, d in zip(column, type_sample_data):
                physical_column_type = 'NUMBER'
                try:
                    int(d)
                except ValueError:
                    physical_column_type = 'VARCHAR'
                physical_column_name = 'COL_' + str(physical_column_order).rjust(3, '0')
                logical_column_english = 'DATA_COL_' + str(physical_column_order).rjust(3, '0')
                insert_col_sql = f"insert into MANAGE_PHYSICAL_COLUMN(ID, data_physical_id, logical_column_korean, physical_column_name, physical_column_type, is_created_yn, physical_column_order, is_use_yn, logical_column_english) VALUES('{column_max_id}', '{table_max_id}', '{col}', '{physical_column_name}', '{physical_column_type}', 'N', '{physical_column_order}', 'Y', '{logical_column_english}')"
                cur.execute(insert_col_sql)
                column_max_id += 1
                physical_column_order += 1
            print("-----------------------------------")
            print('MANAGE_PHYSICAL_COLUMN inserted')
        
        ## MANAGE_PHYSICAL_TABLE physical_created_yn N -> Y update
            update_table_physical_created_yn_sql = f"update MANAGE_PHYSICAL_TABLE set PHYSICAL_CREATED_YN='Y' where id='{table_max_id}'"
            cur.execute(update_table_physical_created_yn_sql)        
            print("-----------------------------------")
            print('MANAGE_PHYSICAL_TABLE physical_created_yn update')
            
            data = data_list[1:]
            col_list = ["COL_" + str(i).rjust(3, '0') + " VARCHAR(65532), " if i<len(column) else "COL_" + str(i).rjust(3, '0') + " VARCHAR(65532)" for i in range(1, len(column)+1)]
            col_data = ''.join(col_list)
            
            ## TMP 테이블 create
            # print("######################################")
            # print(f'table name: {orig_table_name}')
            # create_tmp_sql = f"create table {orig_table_name} (ID NUMBER, {col_data})"
            # cur.execute(create_tmp_sql)
            # print("-----------------------------------")
            # print('TMP table created')
            
            ## NLDATA 테이블 create
            print(f'table name: {physical_table_name}')
            create_nl_sql = f"create table {physical_table_name} (ID NUMBER, {col_data})"
            cur.execute(create_nl_sql)
            print("-----------------------------------")
            print('NLDATA table created')

            
            ## TMP 테이블에 데이터 insert
            id = 1
            if len_rows:
                for dt in data:
                    single_quote_remove_list = [d.replace("'","") for d in dt]
                    defined_data = str(single_quote_remove_list).replace('[','').replace(']','').replace('"','')
                    # insert_tmp_sql = f"insert into {orig_table_name} VALUES({id}, {defined_data})"
                    # cur.execute(insert_tmp_sql)
                    insert_nl_sql = f"insert into {physical_table_name} VALUES({id}, {defined_data})"
                    cur.execute(insert_nl_sql)
                    id += 1
                print("-----------------------------------")
                # print('TMP_DATA inserted')
                print('NLDATA inserted')

                
                ## DATA_BASIC_INFO 테이블 is_collect_yn N -> Y update
                update_basic_sql = f"update DATA_BASIC_INFO set is_collect_yn='Y' where id='{data_basic_id}'"
                cur.execute(update_basic_sql)
                ## MANAGE_PHYSICAL_TABLE 테이블 data_inserted_yn N -> Y, data_insert_row None -> len_rows update 
                update_table_sql = f"update MANAGE_PHYSICAL_TABLE set data_inserted_yn='Y', physical_created_yn='Y', data_insert_row={len_rows} where id='{table_max_id}'"
                cur.execute(update_table_sql)
                print("-----------------------------------")
                print('table updated')
        shutil.move(f'{path_dir}/{file}', destination_path)
        time.sleep(3)
    
    cur.close()