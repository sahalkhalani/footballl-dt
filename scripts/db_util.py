from sqlalchemy import create_engine, MetaData, Table, Column, DateTime, String, select


host = "127.0.0.1"
user = "root"
password = "mysqlroot#123"
database = "football"

def insert_data_in_DB_Dummy():
    try:
        engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")
        table_name = 'prediction_results'
        metadata = MetaData()
        # all_league_names = list(prediction_result['league'].unique())

        table = Table(table_name, metadata, autoload_with=engine)

        future_date_condition = table.c.Date > datetime.now()
        print("ABCD")
        with engine.connect() as connection:
            
            print("EFG")
            select_statement = select(table).where(future_date_condition)
            print("EFG")
            result = connection.execute(select_statement)


            # Fetch and display the records
            for row in result:
                print(row)
            # condition = table.c.league.in_(all_league_names)
            # connection.execute(table.delete().where(condition))
            connection.commit()

        # prediction_result.to_sql(table_name, con=engine, if_exists='append', index=False)

    except mysql.connector.Error as err:
        print(f"Error while connecting DB: {err}")


# insert_data_in_DB_Dummy()