from sqlalchemy import create_engine, Table, MetaData, text, Column, Integer, String


def migrateDB():

    server = 'xznozrobo3funm76yoyaoh75wm-lvvgvquleiuurnfvyvnetw7hoq.datamart.pbidedicated.windows.net'
    database = 'Oil price forecast'
    username = 'api@oilgas.ai'
    password = 'Vpi167YmWwnLEgac'
    driver = '{ODBC Driver 18 for SQL Server}'
    params = 'Driver=' + driver + ';Server=' + server + ',1433;Database=' + database + ';Uid={' + username + '};Pwd={' + password + '};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryPassword'

    # Set your local database here
    db_name = 'test'
    db_user = 'postgres'
    db_password = 'mac0901'
    db_host = 'host.docker.internal'
    db_port = 5432

    # Tạo engine để kết nối đến cơ sở dữ liệu cổng sql datamart
    engine_datamart = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    # Tạo engine để kết nối đến cơ sở dữ liệu postgresql trên local
    engine_postgresql = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    metadata = MetaData()
    table_from_another_database = Table('fact_oilprice_input', metadata, autoload_with=engine_datamart)

    # Tạo bảng tương tự trong engine_postgresql
    table_in_postgresql = Table('fact_oilprice_input', metadata,
                                *[Column(c.name,  String if 'NVARCHAR' in str(c.type) else c.type, nullable=c.nullable) for c in
                                  table_from_another_database.columns], extend_existing=True)

    metadata.create_all(engine_postgresql)


    with engine_datamart.connect() as conn_datamart, engine_postgresql.connect() as conn_postgresql:
        select_query = table_from_another_database.select()
        data = [dict(zip(table_from_another_database.columns.keys(),
                         [str(val) if isinstance(val, bytes) else val for val in row])) for row in
                conn_datamart.execute(select_query).fetchall()]

        insert_query = table_in_postgresql.insert()
        conn_postgresql.execute(insert_query, data)
        conn_postgresql.commit()


if __name__ == '__main__':
    migrateDB()

