import psycopg2


class PGConnection:
    def __init__(self, db_name, user_name, password):
        self.database = db_name
        self.user = user_name
        self.password = password
        self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.password)

    # def connect(self):
    #     conn = psycopg2.connect(database=self.database, user=self.user, password=self.password)
    #     return conn

    def create_db(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                            DROP TABLE IF EXISTS user_contact;
                        """)
            cur.execute("""DROP TABLE IF EXISTS users;""")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id SERIAL PRIMARY KEY
                    , name varchar(50) NOT NULL
                    , lastname varchar(50) NOT NULL
                    , email varchar(50) NOT NULL
                );
            """)
            cur.execute("""
                            CREATE TABLE IF NOT EXISTS user_contact (
                                contact_id SERIAL PRIMARY KEY
                                , phone_number varchar(50) NOT NULL CONSTRAINT UNIQUE_phone_number UNIQUE
                                , user_id int NOT NULL CONSTRAINT FK_user_id references users(user_id) 
                            );
                        """)
        self.conn.commit()

    def create_user(self, name, lastname, email):
        with self.conn.cursor() as cur:
            cur.execute("""
                insert into users(name, lastname, email)
                values(%s, %s, %s) RETURNING user_id;
            """, (name, lastname, email,))
            res = cur.fetchone()[0]
        self.conn.commit()
        return res

    def add_phone_number(self, phone_number, user_id):
        with self.conn.cursor() as cur:
            cur.execute("""
                insert into user_contact(phone_number, user_id)
                values(%s, %s) RETURNING contact_id;
            """, (phone_number, user_id,))
            self.conn.commit()
            res = cur.fetchone()[0]
        return res

    def modify_user(self, name, lastname, email, user_id):
        with self.conn.cursor() as cur:
            cur.execute("""
                update users
                set 
                     name = %s
                    ,lastName = %s
                    ,email = %s
                where user_id = %s;
            """, (name, lastname, email, user_id,))
            self.conn.commit()

    def del_phone_number(self, phone_number):
        with self.conn.cursor() as cur:
            cur.execute("""
                delete from user_contact
                where 
                     phone_number = %s;
            """, (phone_number,))
            self.conn.commit()

    def del_user(self, user_id):
        with self.conn.cursor() as cur:
            cur.execute("""
                delete from users
                where user_id = %s;
            """, (user_id,))
            self.conn.commit()

    def find_user(self, name, lastname, email, phone_number):
        if phone_number is None:
            with self.conn.cursor() as cur:
                cur.execute("""
                    select u.user_id, u.name, u.lastname, u.email
                    from users u
                    where 
                        name = %s
                    and lastname = %s
                    and email = %s
                """, (name, lastname, email,))
                res = cur.fetchall()
            return res
        else:
            with self.conn.cursor() as cur:
                cur.execute("""
                    select u.user_id, u.name, u.lastname, u.email
                    from users u
                    inner join user_contact uc ON u.user_id = uc.user_id
                    where uc.phone_number = %s
                """, (phone_number,))
                res = cur.fetchall()
            return res

if __name__ == '__main__':
    user = PGConnection(db_name='postgres', user_name='postgres', password='asd13asd')
    user.create_db()
    user1_id = user.create_user('Ivan', 'Ivanov', 'ivanov@gmail.com')
    user2_id = user.create_user('Sidorov', 'Petr', 'sidorov@gmail.com')
    user3_id = user.create_user('Dzan', 'Yang', 'kimchhi@yahoo.com')
    phone_number1_id = user.add_phone_number('+79258862178', user1_id)
    phone_number2_id = user.add_phone_number('+79256661122', user2_id)

    #ищем либо по фамилии
    res = user.find_user(name='Ivan', lastname='Ivanov', email='ivanov@gmail.com', phone_number=None)
    res1 = user.find_user(name=None, lastname=None, email=None, phone_number='+79258862178')
    print(res)
    print(res1)
    user.modify_user('Иванка', 'Иванова', 'ivanova@gmail.com', user1_id)
    user.del_phone_number('+79258862178')
    user.del_user(user1_id)
    #Finish
    user.conn.close()








