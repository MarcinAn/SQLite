import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)

    return conn


def execute_sql(conn, sql):
    """Execute sql
    :param conn: Connection object
    :param sql: a SQL script
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)


def add_project(conn, project):
    """
    Create a new project into the projects table
    :param conn:
    :param project:
    :return: project id
    """
    sql = """INSERT INTO projects(nazwa, start_date, end_date)
                VALUES(?,?,?)"""
    cur = conn.cursor()
    cur.execute(sql, project)
    conn.commit()
    return cur.lastrowid


def select_task_by_status(conn, status):
    """
    Query tasks by priority
    :param conn: the Connection object
    :param status:
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM tasks WHERE status=?", (status,))

    rows = cur.fetchall()
    return rows


def select_all(conn, table):
    """
    Query all rows in the table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()

    return rows


def select_where(conn, table, **query):
    """
    Query tasks from table with data from **query dict
    :param conn: the Connection object
    :param table: table name
    :param query: dict of attributes and values
    :return:
    """
    cur = conn.cursor()
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)
    cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
    rows = cur.fetchall()
    return rows


def update(conn, table, id, **kwargs):
    """
    update status, begin_date, and end date of a task
    :param conn:
    :param table: table name
    :param id: row id
    :return:
    """
    parameters = [f"{k} = ?" for k in kwargs]
    parameters = ", ".join(parameters)
    values = tuple(v for v in kwargs.values())
    values += (id,)

    sql = f""" UPDATE {table}
             SET {parameters}
             WHERE id = ?"""
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print("OK")
    except sqlite3.OperationalError as e:
        print(e)


def delete_where(conn, table, **kwargs):
    """
    Delete from table where attributes from
    :param conn:  Connection to the SQLite database
    :param table: table name
    :param kwargs: dict of attributes and values
    :return:
    """
    qs = []
    values = tuple()
    for k, v in kwargs.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)

    sql = f"DELETE FROM {table} WHERE {q}"
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    print("Deleted")


def delete_all(conn, table):
    """
    Delete all rows from table
    :param conn: Connection to the SQLite database
    :param table: table name
    :return:
    """
    sql = f"DELETE FROM {table}"
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    print("Deleted")


if __name__ == "__main__":

    create_projects_sql = """
    -- projects table
    CREATE TABLE IF NOT EXISTS projects (
    id integer PRIMARY KEY,
    nazwa text NOT NULL,
    start_date text,
    end_date text
    );
    """

    db_file = "database.db"
    conn = create_connection(db_file)

    if conn is not None:
        execute_sql(conn, create_projects_sql)
        project = [
            ("SQLite w praktyce", "2023-08-09", "2023-08-09"),
            ("Baza danych", "2023-08-09", "2023-08-09"),
        ]
        for item in project:
            print(f"dodaję do projektu: {item}")
            add_project(conn, item)

        print("Aktualizuję pole end_date ")
        update(conn, "projects", 2, end_date="2023-08-10")

        print("Wyświetlam pozycję")

        for item in select_all(conn, "projects"):
            print(item)

        print("Usuwam wszystkie wartości")

        delete_all(conn, "projects")
        conn.close()
