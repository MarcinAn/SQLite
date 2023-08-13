import sqlite3
from sqlite3 import Error


def create_connection(db_file):
    """create a database connection to the SQLite database
       specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
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


def add_module(conn, module):
    """
    :param conn:
    :param module:
    :return: module id
    """
    sql = """INSERT INTO modules(nazwa, ilosc_zadan, opis, start_date)
         VALUES(?,?,?,?)"""
    cur = conn.cursor()
    cur.execute(sql, module)
    conn.commit()
    return cur.lastrowid


def add_submodule(conn, submodule):
    """
    :param conn:
    :param submodule:
    :return: submodule id
    """
    sql = """INSERT INTO submodules(module_id, nazwa, zawiera_zadanie, status)
         VALUES(?,?,?,?)"""
    cur = conn.cursor()
    cur.execute(sql, submodule)
    conn.commit()
    return cur.lastrowid


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
    Query submodules from table with data from **query dict
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
    update status, begin_date, and end date of a submodule
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
    create_modules_sql = """
   -- modules table
   CREATE TABLE IF NOT EXISTS modules (
      id integer PRIMARY KEY,
      nazwa text NOT NULL,
      ilosc_zadan integer NOT NULL,
      opis TEXT,
      start_date text
   );
   """

    create_submodules_sql = """
   -- submodule table
   CREATE TABLE IF NOT EXISTS submodules (
      id integer PRIMARY KEY,
      module_id integer NOT NULL,
      nazwa VARCHAR(250) NOT NULL,
      zawiera_zadanie boolean NOT NULL,
      status VARCHAR(15) NOT NULL,
      FOREIGN KEY (module_id) REFERENCES modules (id)
   );
   """

    db_file = "database.db"

    conn = create_connection(db_file)
    if conn is not None:
        execute_sql(conn, create_modules_sql)
        execute_sql(conn, create_submodules_sql)
        modules = [
            (
                "Podstawy Pythona cz. 1",
                3,
                "podstawowe typy danych, tworzenie zmiennych, pętle, wyrażenia warunkowe.",
                "2023-04-05",
            ),
            (
                "Podstawy Pythona cz. 2",
                2,
                "rodzaje kolekcji, dodawanie, odejmowanie, sortowanie, porządkowanie elementów",
                "2023-04-05",
            ),
            (
                "Środowisko pracy programisty",
                3,
                "programowanie na swoim komputerze, narzędzia, które ułatwiają pracę, dzielenie kodeu z innymi.",
                "2023-04-19",
            ),
        ]
    for item in modules:
        add_module(conn, item)
        submodules = [
            (1, "Podstawy pisania kodu", "No", "ended"),
            (1, "Zmienne", "Yes", "ended"),
            (1, "Liczby i działania", "Yes", "ended"),
            (1, "Pętle", "Yes", "ended"),
            (1, "Wyrażenia warunkowe i boolean", "No", "ended"),
            (1, "Pętle – rozszerzenie", "No", "ended"),
            (1, "Podsumowanie", "No", "ended"),
            (2, "Poznajemy kolekcje", "No", "ended"),
            (2, "Funkcje kolekcji", "No", "ended"),
            (2, "Modyfikacje kolekcji", "Yes", "ended"),
            (2, "Nawigacja w pętlach", "No", "ended"),
            (2, "Operacje na danych", "Yes", "ended"),
            (2, "Podsumowanie", "No", "ended"),
            (2, "Podsumowanie", "No", "ended"),
            (3, "Niezbędne narzędzia", "Yes", "ended"),
            (3, "Śledzenie zmian w kodzie", "Yes", "ended"),
            (3, "Git – repozytorium zdalne", "No", "ended"),
            (3, "Praca ze zdalnymi repozytoriami", "Yes", "ended"),
            (3, "Podsumowanie", "No", "ended"),
        ]
    for item in submodules:
        add_submodule(conn, item)

    # Wyświetlanie danych
    print("Wyświetlanie drugiego modułu: \n")
    print(f"{select_where(conn, 'modules', id= 2)} \n")
    print("Wyświetlanie zawartości modułu: \n")
    print(f"{select_where(conn, 'submodules', module_id= 2)} \n")

    # Update daty w module 2
    print("Aktualizacja daty w module drugim: \n")
    update(conn, "modules", 2, start_date="2023-05-12")
    print()
    print("Wyświetlanie drugiego modułu z poprawioną datą: \n")
    print(f"{select_where(conn, 'modules', id= 2)}\n")

    # Usuwanie ostatniego elementu z moduły drugiego
    print("Usuwanie zdublowanej pozycji w module drugim: \n")
    delete_where(conn, "submodules", id=14)
    print()
    print("Wyświetlanie drugiego modułu z usuniętą pozycją: \n")
    print(f"{select_where(conn, 'submodules', module_id= 2)}\n")
    conn.close()
