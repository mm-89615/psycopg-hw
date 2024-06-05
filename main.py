import psycopg2


def create_db(connect):
    with connect.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS client(
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(100) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                email VARCHAR(320) UNIQUE NOT NULL
            );
            """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS client_phone(
                id SERIAL PRIMARY KEY,
                client_id INTEGER NOT NULL REFERENCES client(id) ON DELETE CASCADE,            
                phone VARCHAR(20) UNIQUE NOT NULL
            );
            """)
        connect.commit()


def drop_table(connect):
    with connect.cursor() as cur:
        cur.execute("""
        DROP TABLE client_phone;
        DROP TABLE client;
        """)


def add_client(connect, first_name, last_name, email, phone=None):
    with connect.cursor() as cur:
        cur.execute("""
            INSERT INTO client(first_name, last_name, email)
            VALUES (%s, %s, %s) 
            RETURNING id;
            """, (first_name, last_name, email))
        client_id = cur.fetchone()[0]
        if phone:
            add_phone(connect, client_id, phone)


def add_phone(connect, client_id, phone):
    with connect.cursor() as cur:
        cur.execute("""
            INSERT INTO client_phone(client_id, phone)
            VALUES (%s, %s);
            """, (client_id, phone))
        connect.commit()


def update_client(connect, client_id, first_name=None, last_name=None,
                  email=None):
    query = "UPDATE client SET"
    values = []
    if first_name:
        query += " first_name=%s,"
        values.append(first_name)
    if last_name:
        query += " last_name=%s,"
        values.append(last_name)
    if email:
        query += " email=%s,"
        values.append(email)
    query = query.rstrip(",") + "WHERE id=%s;"
    values.append(client_id)
    with connect.cursor() as cur:
        cur.execute(query, values)
        connect.commit()


def update_phone(connect, client_id, old_phone, new_phone):
    with connect.cursor() as cur:
        cur.execute("""
            UPDATE client_phone SET phone=%s WHERE client_id=%s AND phone=%s;
            """, (new_phone, client_id, old_phone))
        connect.commit()


def delete_phone(connect, client_id, phone):
    with connect.cursor() as cur:
        cur.execute("""
            DELETE FROM client_phone WHERE client_id=%s AND phone=%s;
            """, (client_id, phone))
        connect.commit()


def delete_client(connect, client_id):
    with connect.cursor() as cur:
        cur.execute("""
            DELETE FROM client WHERE id=%s;
            """, (client_id,))
        connect.commit()


def find_client(connect, first_name=None, last_name=None, email=None,
                phone=None):
    query = """
            SELECT c.id, c.first_name, c.last_name, c.email, cp.phone FROM client c
            LEFT JOIN client_phone cp ON c.id = cp.client_id
            """
    values = []
    if first_name:
        query += " WHERE c.first_name=%s;"
        values.append(first_name)
    elif last_name:
        query += " WHERE c.last_name=%s;"
        values.append(last_name)
    elif email:
        query += " WHERE c.email=%s;"
        values.append(email)
    elif phone:
        query += " WHERE cp.phone=%s;"
        values.append(phone)
    with connect.cursor() as cur:
        cur.execute(query, values)
        result = cur.fetchall()
        return result


def main():
    with psycopg2.connect(database="psycopg_hw", user="postgres",
                          password="root") as conn:
        drop_table(conn)

        create_db(conn)

        add_client(conn, 'Дмитрий', 'Дмитриев', 'dmitriev@mail.ru',
                   '+7987978997')
        add_client(conn, 'Олег', 'Олегов', 'olegov@mail.ru', '+79879782397')
        add_client(conn, 'Иван', 'Иванов', 'ivanov@mail.ru', '+79879732397')

        add_phone(conn, 1, '+79879732323')
        add_phone(conn, 2, '+74287973232')
        add_phone(conn, 3, '+74287377323')

        update_client(conn, 2, email='oleg2@mail.ru')

        add_phone(conn, 1, '+7956')
        update_phone(conn, 1, '+7956', '+79999')

        delete_phone(conn, 1, '+7987978997')

        delete_client(conn, 3)

        print(find_client(conn, first_name='Олег'))
        print(find_client(conn, last_name='Дмитриев'))
        print(find_client(conn, email='oleg2@mail.ru'))
        print(find_client(conn, phone='+79999'))


if __name__ == '__main__':
    main()
