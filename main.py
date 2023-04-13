from sqlalchemy import create_engine, Table, MetaData, text, Column, select, func, Integer, Boolean, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
import logging
from retrying import retry
import pyodbc
# Hàm decorator để thực hiện retry logic cho việc kết nối đến engine
@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000, stop_max_attempt_number=3)
def create_sqlalchemy_engine(connection_string):
    return create_engine(connection_string)

# Cấu hình logging message
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Function chính
def migrateDB():
    server = 'xznozrobo3funm76yoyaoh75wm-mv3gtp6e24wexdkp4mlhd3oo2u.datamart.pbidedicated.windows.net'
    database = 'lantvh_clone'
    username = 'api@oilgas.ai'
    password = 'Vpi167YmWwnLEgac'
    driver = '{ODBC Driver 18 for SQL Server}'
    params = 'Driver=' + driver + ';Server=' + server + ',1433;Database=' + database + ';Uid={' + username + '};Pwd={' + password + '};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryPassword'

    db_name = 'postgres'
    db_user = 'postgres'
    db_password = 'mac0901'
    db_host = 'host.docker.internal'
    db_port = 5432

    # Tạo engine để kết nối đến cơ sở dữ liệu cổng sql datamart
    engine_datamart = create_sqlalchemy_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    # Tạo engine để kết nối đến cơ sở dữ liệu postgresql trên local
    engine_postgresql = create_sqlalchemy_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine_datamart)
    db = Session()
    with Session() as db:

        #Lấy các table trong db datamart
        str_sql = text("SELECT table_name FROM information_schema.tables")
        result_query = db.execute(str_sql)
        results = result_query.fetchall()
        table_names = [row[0] for row in results]
        drop = ["relationshipColumns", "relationships", "database_firewall_rules"]
        list_table = [elem for elem in table_names if elem not in drop]

        try:
            # Lặp qua các table và ghi dữ liệu vao db postgres
            with engine_datamart.connect().execution_options(
                    timeout=6000) as conn_datamart, engine_postgresql.connect().execution_options(
                    timeout=6000) as conn_postgresql:
                for tableName in list_table:
                    writeData(engine_postgresql, engine_datamart, conn_datamart, conn_postgresql, tableName, db)
                conn_postgresql.close()
                conn_datamart.close()
        except pyodbc.OperationalError as e:
            print("Lỗi kết nối tới cơ sở dữ liệu:", e)


# Function ghi dữ liệu vào DB
def writeData(engine_postgresql, engine_datamart, conn_datamart, conn_postgresql, tablename, db):
    print(tablename)
    logging.debug(tablename)
    metadata = MetaData()
    table_from_another_database = Table(tablename, metadata, autoload_with=engine_datamart)

    # Tạo bảng tương tự trong engine_postgresql
    new_columns = [
        Column(c.name, Boolean, nullable=c.nullable, server_default=text('false')) if 'BIT' in str(c.type)
        else Column(c.name, String if 'NVARCHAR' in str(c.type) else c.type, nullable=c.nullable) for c in
        table_from_another_database.columns
    ]

    # Định nghĩa bảng với các cột mới đã thay đổi kiểu dữ liệu
    table_in_postgresql = Table(tablename, metadata,
                                *new_columns, extend_existing=True)
    # table_in_postgresql = Table(tablename, metadata,
    #                             *[Column(c.name,  String if 'NVARCHAR' in str(c.type) else c.type, nullable=c.nullable) for c in
    #                               table_from_another_database.columns], extend_existing=True)

    metadata.create_all(engine_postgresql)

    # Tạo alias cho bảng
    total_records = db.query(table_from_another_database).count()
    # Thiết lập điều kiện ban đầu cho vòng lặp
    start = 0
    remain = total_records
    # Đặt số lượng mỗi lần ghi, đọc
    batch_read = 1000000
    # Lặp qua
    while remain > 0:
        # Thực hiện truy vấn dữ liệu
        columns = table_from_another_database.columns
        first_column_name = columns[0].name
        if remain < batch_read:
            batch_read = remain
        logging.debug("")
        logging.debug(f"batch_read = {batch_read}")
        logging.debug(f"total_records = {total_records}")
        logging.debug(f"start = {start}")
        logging.debug(f"remain = {remain}")
        logging.debug("==================================START_READ==================================")

        select_query = table_from_another_database.select().order_by(first_column_name).offset(start).limit(batch_read)
        data = [dict(zip(table_from_another_database.columns.keys(),
                         [str(val) if isinstance(val, bytes) else val for val in row])) for row in
                conn_datamart.execute(select_query).fetchall()]

        # # Truy vấn dữ liệu từ table_from_another_database với pandas
        # select_query = table_from_another_database.select().order_by(first_column_name).offset(start).limit(batch_size)
        # df = pd.read_sql(select_query, conn_datamart)
        #
        # # Chuyển đổi các giá trị dạng bytes thành chuỗi (nếu cần)
        # df = df.applymap(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
        #
        # # Chuyển đổi DataFrame thành list của dict
        # data = df.to_dict(orient='records')


        logging.debug("==================================END_READ==================================")
        logging.debug("==================================START_WRITE==================================")
        # Đặt kích thước lô
        batch_write = 10000
        # Lặp lại cho đến khi không còn dữ liệu để ghi
        while data:
            batch = data[:batch_write]
            data = data[batch_write:]

            insert_query = postgresql_insert(table_in_postgresql).on_conflict_do_nothing()
            conn_postgresql.execute(insert_query, batch)
            # Commit transaction
            conn_postgresql.commit()

        logging.debug("==================================END_WRITE==================================")
        # Giảm đi batch_size để chuẩn bị cho vòng lặp tiếp theo
        data = None
        remain -= batch_read
        start += batch_read





if __name__ == '__main__':
    migrateDB()

