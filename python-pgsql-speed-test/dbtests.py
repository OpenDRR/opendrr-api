import psycopg2

def go():
    conn = psycopg2.connect(
        "user=postgres password=password host=db-opendrr port=5432")
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE test;")
    conn = psycopg2.connect(
        "user=postgres password=password host=db-opendrr port=5432 dbname=test"
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE test (id serial PRIMARY KEY, " +
        "num integer, data varchar);")
    for i in range(10000):
        cursor.execute(
            "INSERT INTO test (data) VALUES ('hello!');")

go()
