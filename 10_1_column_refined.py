import pandas as pd
import jaydebeapi


def update_refined_col(one, temp):

    select_sql = """SELECT ID, LOGICAL_COLUMN_KOREAN FROM MANAGE_PHYSICAL_COLUMN WHERE ID = {} """.format(one['id'])
        
    cur.execute(select_sql)
    select_result = cur.fetchall()

    if len(select_result) == 1:
        update_sql = """UPDATE MANAGE_PHYSICAL_COLUMN
                           SET LOGICAL_COLUMN_KOREAN = ? 
                         WHERE ID = ?
                    """
        update_values = (one['hanugle_refined_col_nm'], one['id'])
        
        cur.execute(update_sql, update_values)
        
        update_yn = True
        return update_yn
    else:
        return False


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
    
    # 데이터 READ
    file_name = './data/refined_data/220722_column_refined_data.xlsx'

    df_0 = pd.read_excel(file_name, engine='openpyxl')
    df_0.columns = ['del_1', 'id', 'hangule_table_nm', 'hangule_col_nm', 'hanugle_refined_col_nm', 'note', 'same_yn', 'english_col_nm', 'physical_col_nm', 'del_2', 'del_3', 'del_4']

    df_1 = df_0[['id', 'hangule_table_nm', 'hangule_col_nm', 'hanugle_refined_col_nm', 'same_yn', 'english_col_nm', 'physical_col_nm']]

    df_2 = df_1[df_1['same_yn'] == 'x']

    df_2['update_yn'] = df_2.apply(lambda x: update_refined_col(x, cur), axis=1)
    cur.close()

