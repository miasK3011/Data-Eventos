import  psycopg2, os

def query_postgresql(query, fetch_size=10):
    host = os.getenv("DB_HOST")
    database = os.getenv("DB_DATABASE")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    try:
        connection = psycopg2.connect(
            host=host, database=database, user=user, password=password
        )

        cursor = connection.cursor()

        cursor.execute(query)

        results = cursor.fetchmany(fetch_size)

        cursor.close()
        connection.close()

        return results

    except (Exception, psycopg2.Error) as error:
        print("Erro ao conectar ao PostgreSQL", error)
        return None