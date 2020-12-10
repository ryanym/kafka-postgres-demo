import psycopg2
import yaml

def create_connection(host, port, database, username, password):

    conn = None
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=username,
            password=password,
            database=database
        )

    except psycopg2.DatabaseError as e:
        print(e)

    finally:
        return conn


def create_table_if_not_exists(conn, table_name, column_name):

    cur = conn.cursor()
    cur.execute(f"""create table if not exists {table_name} ({column_name} text);""")
    conn.commit()
    cur.close()

def drop_table(conn, table_name):
    cur = conn.cursor()
    cur.execute(f"""drop table if exists {table_name};""")
    conn.commit()
    cur.close()


def insert_message(conn, table_name, column_name, message):
    affected_rows = 0
    cur = conn.cursor()
    cur.execute(f"""insert into {table_name} ({column_name}) values (\'{message}\')""")
    conn.commit()

    affected_rows = cur.rowcount
    cur.close()

    return affected_rows


def parse_config(config_file):
    with open(config_file, 'r') as stream:
        config = yaml.safe_load(stream)
        return config