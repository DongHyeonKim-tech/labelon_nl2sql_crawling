import googletrans
import jaydebeapi


if __name__ == '__main__':
  
  # DB connection
  conn = jaydebeapi.connect(
    "com.tmax.tibero.jdbc.TbDriver",
    "jdbc:tibero:thin:@172.7.0.23:8629:tibero",
    ["labelon", "euclid!@)$labelon"],
    "tibero6-jdbc.jar",
  )
  cur = conn.cursor()
  
  
  # translator
  translator = googletrans.Translator()
  
  # MANAGE_PHYSICAL_TABLE select sql
  select_logical_table_korean = "select ID, LOGICAL_TABLE_KOREAN as kor from MANAGE_PHYSICAL_TABLE where DATA_BASIC_ID in (select id from DATA_BASIC_INFO where COLLECT_SITE_ID=2 and IS_COLLECT_YN='Y') and logical_table_english like 'DATA_TMP_%'"
  # MANAGE_PHYSICAL_COLUMN select sql
  select_logical_column_korean = "select ID, LOGICAL_COLUMN_KOREAN as kor FROM MANAGE_PHYSICAL_COLUMN WHERE DATA_PHYSICAL_ID IN (SELECT ID FROM MANAGE_PHYSICAL_TABLE WHERE DATA_BASIC_ID IN (SELECT ID FROM DATA_BASIC_INFO WHERE COLLECT_SITE_ID='2')) AND LOGICAL_COLUMN_ENGLISH LIKE 'DATA_COL_%'"
    
  # 데이터를 select, 번역 후 각 테이블에 LOGICAL_*_ENGLISH update
  def name_translator(select_sql, table):
    print("####################")
    cur.execute(select_sql)
    select_data = cur.fetchall()
    print('selected')
    for data in select_data:
      id_ = data['id']
      print(f'id: {id_}')
      kor = data['kor'].replace('_', ' ')
      print(f'kor: {kor}')
      result = translator.translate(kor, dest='en')
      logical_eng = result.text
      print(f'eng: {logical_eng}')
      update_sql = f"update MANAGE_PHYSICAL_{table} set logical_{table}_english='{logical_eng}' where id='{id_}'"
      cur.execute(update_sql)
      print('updated')
  
  name_translator(select_logical_table_korean, 'table')
  name_translator(select_logical_column_korean, 'column')