import psycopg2
import yaml

def create_connection(host, port, database, username, password):
    """
    Creates a postgres database connection object
    :param host(str): url of the postgres host
    :param port(int): port of the postgres instance
    :param database(str): name of the database
    :param username: database user
    :param password: database user password
    :return: postgres connection
    """
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
    """
    shorthand for create a table if it doesn't exist
    :param conn(connection): postgres database connection
    :param table_name(str): name of the table to create
    :param column_name(str): column of the table
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"""create table if not exists {table_name} ({column_name} text);""")
    conn.commit()
    cur.close()


def drop_table(conn, table_name):
    """
    shorthand for dropping a table if it exists
    :param conn(connection): postgres database connection
    :param table_name(str): name of the table to create
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"""drop table if exists {table_name};""")
    conn.commit()
    cur.close()


def insert_message(conn, table_name, column_name, message):
    """
    shorthand for insert message to a table
    :param conn(connection): postgres database connection
    :param table_name(str): name of the table to create
    :param column_name(str): column of the table
    :param message(str): message to insert
    :return: number of rows inserted, this should be 1
    """
    affected_rows = 0
    cur = conn.cursor()
    cur.execute(f"""insert into {table_name} ({column_name}) values (\'{message}\')""")
    conn.commit()

    affected_rows = cur.rowcount
    cur.close()

    return affected_rows

def parse_config(config_file):
    """
    parse a yaml config file to dictionary
    :param config_file: path of yaml config file
    :return: config(dict) of the configuration items
    """
    with open(config_file, 'r') as stream:
        config = yaml.safe_load(stream)
        return config