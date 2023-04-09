from sqlalchemy import create_engine, Table, MetaData, text, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
def migrateDB():

    server = 'xznozrobo3funm76yoyaoh75wm-fr3e3p3dk6eejffi7w4p27iybe.datamart.pbidedicated.windows.net'
    database = '2023_LPG_Datamart_Hanhdh'
    username = 'api@oilgas.ai'
    password = 'Vpi167YmWwnLEgac'
    driver = '{ODBC Driver 18 for SQL Server}'
    params = 'Driver=' + driver + ';Server=' + server + ',1433;Database=' + database + ';Uid={' + username + '};Pwd={' + password + '};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryPassword'

    db_name = 'test'
    db_user = 'postgres'
    db_password = 'mac0901'
    db_host = 'host.docker.internal'
    db_port = 5432

    # Tạo engine để kết nối đến cơ sở dữ liệu cổng sql datamart
    engine_datamart = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    # Tạo engine để kết nối đến cơ sở dữ liệu postgresql trên local
    engine_postgresql = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine_datamart)
    db = Session()
    #Lấy các table trong db datamart
    str_sql = text("SELECT table_name FROM information_schema.tables")
    result_query = db.execute(str_sql)
    results = result_query.fetchall()
    table_names = [row[0] for row in results]
    drop = ["relationshipColumns", "relationships", "database_firewall_rules"]
    list_table = [elem for elem in table_names if elem not in drop]
    #Lặp qua các table và ghi dữ liệu vao db postgres
    with engine_datamart.connect().execution_options(timeout=6000) as conn_datamart, engine_postgresql.connect().execution_options(timeout=6000) as conn_postgresql:
        for tableName in list_table:
            writeData(engine_postgresql, engine_datamart, conn_datamart, conn_postgresql , tableName)
        conn_postgresql.close()
        conn_datamart.close()


def writeData(engine_postgresql, engine_datamart, conn_datamart, conn_postgresql, tablename):
    print(tablename)
    logging.debug(tablename)
    metadata = MetaData()
    table_from_another_database = Table(tablename, metadata, autoload_with=engine_datamart)

    # Tạo bảng tương tự trong engine_postgresql

    table_in_postgresql = Table(tablename, metadata,
                                *[Column(c.name,  String if 'NVARCHAR' in str(c.type) else c.type, nullable=c.nullable) for c in
                                  table_from_another_database.columns], extend_existing=True)

    metadata.create_all(engine_postgresql)

    select_query = table_from_another_database.select()
    data = [dict(zip(table_from_another_database.columns.keys(),
                     [str(val) if isinstance(val, bytes) else val for val in row])) for row in
            conn_datamart.execute(select_query).fetchall()]

    # Đặt kích thước lô
    batch_size = 1

    # Lặp lại cho đến khi không còn dữ liệu để ghi
    while data:
        batch = data[:batch_size]
        data = data[batch_size:]

        # Mở transaction mới
        with conn_postgresql.begin() as trans:
            try:
                insert_query = postgresql_insert(table_in_postgresql).values(batch).on_conflict_do_nothing()
                conn_postgresql.execute(insert_query, batch)
                # Commit transaction
                trans.commit()
            except Exception as e:
                # Nếu có lỗi, rollback transaction
                trans.rollback()
                logging.error(f"Lỗi trong quá trình ghi dữ liệu: {e}")


if __name__ == '__main__':
    migrateDB()

