import psycopg2


class PGConnection:
    def __init__(self, db_name, user_name, password):
        self.database = db_name
        self.user = user_name
        self.password = password
        self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.password)
        self.cur = self.conn.cursor()

    def create_db(self):
        self.cur.execute("""DROP TABLE IF EXISTS user_contact;""")
        self.cur.execute("""DROP TABLE IF EXISTS users;""")

        self.cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id SERIAL PRIMARY KEY
                    , name varchar(50) NOT NULL
                    , lastname varchar(50) NOT NULL
                    , email varchar(50) NOT NULL
                );""")
        self.cur.execute("""
                            CREATE TABLE IF NOT EXISTS user_contact (
                                contact_id SERIAL PRIMARY KEY
                                , phone_number varchar(50) NOT NULL CONSTRAINT UNIQUE_phone_number UNIQUE
                                , user_id int NOT NULL CONSTRAINT FK_user_id references users(user_id) 
                            );""")
        self.conn.commit()

    def create_user(self, name, lastname, email):
        self.cur.execute("""
                insert into users(name, lastname, email)
                values(%s, %s, %s) RETURNING user_id;
            """, (name, lastname, email,))
        res = self.cur.fetchone()[0]
        self.conn.commit()
        return res

    def add_phone_number(self, phone_number, user_id):
        self.cur.execute("""
                insert into user_contact(phone_number, user_id)
                values(%s, %s) RETURNING contact_id;
            """, (phone_number, user_id,))
        self.conn.commit()
        res = self.cur.fetchone()[0]
        return res

    def modify_user_name(self, name, user_id):
        self.cur.execute("""
                        update users
                        set 
                             name = %s
                        where user_id = %s;
                """, (name, user_id,))
        self.conn.commit()

    def modify_user_lastname(self, lastname, user_id):
        self.cur.execute("""
                        update users
                        set 
                             lastname = %s
                        where user_id = %s;
                """, (lastname, user_id,))
        self.conn.commit()

    def modify_user_email(self, email, user_id):
        self.cur.execute("""
                        update users
                        set 
                             email = %s
                        where user_id = %s;
                """, (email, user_id,))
        self.conn.commit()

    def modify_user(self, **kwargs):
        update_fields = {'name', 'lastname', 'email', 'user_id'} & set(kwargs)
        if len(update_fields) == 0:
            return None

        if kwargs.get('name') is not None and kwargs.get('lastname') is not None and kwargs.get('email') is not None \
                and kwargs.get('user_id') is not None:
            self.cur.execute("""
                            update users
                            set 
                                 name = %s
                                ,lastname = %s
                                ,email = %s
                            where user_id = %s;
                    """, (kwargs['name'], kwargs['lastname'], kwargs['email'], kwargs['user_id'],))
            self.conn.commit()
        elif kwargs.get('name') is not None and kwargs.get('user_id') is not None:
            self.cur.execute("""
                            update users
                            set 
                                 name = %s
                            where user_id = %s;
                    """, (kwargs['name'], kwargs['user_id'],))
            self.conn.commit()
        elif kwargs.get('lastname') is not None and kwargs.get('user_id') is not None:
            self.cur.execute("""
                            update users
                            set 
                                 lastname = %s
                            where user_id = %s;
                    """, (kwargs['lastname'], kwargs['user_id'],))
            self.conn.commit()
        elif kwargs.get('email') is not None and kwargs.get('user_id') is not None:
            self.cur.execute("""
                            update users
                            set 
                                 email = %s
                            where user_id = %s;
                    """, (kwargs['email'], kwargs['user_id'],))
            self.conn.commit()

    def del_phone_number(self, phone_number):
        self.cur.execute("""
                delete from user_contact
                where 
                     phone_number = %s;
        """, (phone_number,))
        self.conn.commit()

    def del_user(self, user_id):
        self.cur.execute("""
                delete from users
                where user_id = %s;
        """, (user_id,))
        self.conn.commit()

    def get_user_by_phone(self, phone_number):
        self.cur.execute("""
                            select u.user_id, u.name, u.lastname, u.email
                            from users u
                            inner join user_contact uc ON u.user_id = uc.user_id
                            where uc.phone_number = %s
                    """, (phone_number,))
        return self.cur.fetchall()

    def get_user_by_name(self, name):
        self.cur.execute("""
                            select u.user_id, u.name, u.lastname, u.email
                            from users u
                            where name = %s
                    """, (name,))
        return self.cur.fetchall()

    def get_user_by_lastname(self, lastname):
        self.cur.execute("""
                            select u.user_id, u.name, u.lastname, u.email
                            from users u
                            where lastname = %s
                    """, (lastname,))
        return self.cur.fetchall()

    def get_user_by_names(self, name, lastname):
        self.cur.execute("""
                            select u.user_id, u.name, u.lastname, u.email
                            from users u
                            where name = %s
                            and lastname = %s
                    """, (name, lastname,))
        return self.cur.fetchall()

    def get_user_by_email(self, email):
        self.cur.execute("""
                            select u.user_id, u.name, u.lastname, u.email
                            from users u
                            where email = %s
                    """, (email,))
        return self.cur.fetchall()

    def get_user(self, **kwargs):
        seek_keys = {'name', 'lastname', 'email', 'phone_number'} & set(kwargs)
        if len(seek_keys) == 0:
            return None
        if kwargs.get('phone_number') is not None:
            self.cur.execute("""
                                select u.user_id, u.name, u.lastname, u.email
                                from users u
                                inner join user_contact uc ON u.user_id = uc.user_id
                                where uc.phone_number = %s
                        """, (kwargs['phone_number'],))
            return self.cur.fetchall()
        elif kwargs.get('name') is not None and kwargs.get('lastname') is not None and kwargs.get('email') is not None:
            self.cur.execute("""
                                        select u.user_id, u.name, u.lastname, u.email
                                        from users u
                                        where name = %s
                                        and lastname = %s
                                        and email = %s
                                """, (kwargs['name'], kwargs['lastname'], kwargs['email'],))
            return self.cur.fetchall()
        elif kwargs.get('name') is not None and kwargs.get('lastname') is not None:
            self.cur.execute("""
                                        select u.user_id, u.name, u.lastname, u.email
                                        from users u
                                        where name = %s
                                        and lastname = %s
                                """, (kwargs['name'], kwargs['lastname'],))
            return self.cur.fetchall()

        elif kwargs.get('email') is not None:
            self.cur.execute("""
                                        select u.user_id, u.name, u.lastname, u.email
                                        from users u
                                        where email = %s
                                """, (kwargs['email'],))
            return self.cur.fetchall()

        elif kwargs.get('name') is not None:
            self.cur.execute("""
                                select u.user_id, u.name, u.lastname, u.email
                                from users u
                                where name = %s
                        """, (kwargs['name'],))
            return self.cur.fetchall()

        elif kwargs.get('lastname') is not None:
            self.cur.execute("""
                                select u.user_id, u.name, u.lastname, u.email
                                from users u
                                where lastname = %s
                        """, (kwargs['lastname'],))
            return self.cur.fetchall()

if __name__ == '__main__':
    user = PGConnection(db_name='postgres', user_name='postgres', password='asd13asd')
    user.create_db()

    #Создание
    user1_id = user.create_user('Ivan', 'Ivanov', 'ivanov@gmail.com')
    user2_id = user.create_user('Petr', 'Sidorov', 'sidorov@gmail.com')
    user3_id = user.create_user(name='Dzan', lastname='Yang', email='kimchhi@yahoo.com')
    user4_id = user.create_user('Ivan', 'Petrov', 'petroff@gmail.com')

    phone_number1_id = user.add_phone_number('+79258862178', user1_id)
    phone_number2_id = user.add_phone_number('+79256661122', user2_id)

    #Поиск
    print(user.get_user_by_name('Ivan'))
    print(user.get_user_by_phone('+79256661122'))
    print(user.get_user_by_email('kimchhi@yahoo.com'))
    print(user.get_user_by_names('Ivan', 'Petrov'))

    print('For experiment: *overload method')
    print(user.get_user(phone_number='+79258862178'))
    print(user.get_user(email='kimchhi@yahoo.com', name='Dzan', lastname='Yang'))
    print(user.get_user(name='Ivan', lastname='Petrov'))
    print(user.get_user(name='Ivan'))
    print(user.get_user(email='kimchhi@yahoo.com'))

    #Изменения данных пользователя
    user.modify_user_email('ivanov6@gmail.com', user1_id)
    user.modify_user_name('Sidor', user2_id)
    user.modify_user_lastname('Yung', user3_id)
    #Через *overload метод
    user.modify_user(name='Иванка', lastname='Иванова', email='ivanova@gmail.com', user_id=user1_id)
    user.modify_user(email='petroff_66@gmail.com', user_id=user4_id)

    #Изменение номера телефона
    user.del_phone_number('+79258862178')
    user.del_user(user1_id)

    #Finish
    user.cur.close()
    user.conn.close()








