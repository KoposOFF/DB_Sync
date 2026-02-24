import psycopg2


class DBSync:

    def __init__(self, db1_url, db2_url):
        # db1 — эталонная база
        # db2 — боевая база
        self.db1_url = db1_url
        self.db2_url = db2_url

        self.db1_conn = None
        self.db2_conn = None

        self.db1_tables = []
        self.db2_tables = []

        self.missing_tables = []

        self.db1_columns = {}
        self.db2_columns = {}

        self.missing_columns = {}

        self.sql_plan = []

    def connect(self):
        print("Подключаюсь к базам...")

        self.db1_conn = psycopg2.connect(self.db1_url)
        self.db2_conn = psycopg2.connect(self.db2_url)

        print("Подключение успешно")

    def close(self):
        print("Закрываю соединения")

        if self.db1_conn:
            self.db1_conn.close()

        if self.db2_conn:
            self.db2_conn.close()

    def get_tables(self, conn):
        """
        Возвращает список таблиц из базы
        """

        query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE';
        """

        cur = conn.cursor()
        cur.execute(query)

        tables = [row[0] for row in cur.fetchall()]

        cur.close()
        return tables

    def load_tables(self):
        """
        Загружает таблицы из обеих баз
        """

        self.db1_tables = self.get_tables(self.db1_conn)
        self.db2_tables = self.get_tables(self.db2_conn)

        print("Таблицы в эталонной БД:", self.db1_tables)
        print("Таблицы в боевой БД:", self.db2_tables)

    def compare_tables(self):
        """
        Сравниваем таблицы и находим отсутствующие в боевой БД
        """

        missing_tables = []

        for table in self.db1_tables:
            if table not in self.db2_tables:
                missing_tables.append(table)

        self.missing_tables = missing_tables

        print("Таблицы, которых нет в боевой БД:", missing_tables)

    def get_columns(self, conn, table_name):
        """
        Возвращает структуру колонок таблицы
        """

        query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name = %s;
        """

        cur = conn.cursor()
        cur.execute(query, (table_name,))

        columns = {}

        for name, dtype, nullable in cur.fetchall():
            columns[name] = {"type": dtype, "nullable": nullable}

        cur.close()
        return columns

    def load_columns(self):
        """
        Загружает колонки всех таблиц из обеих БД
        """

        self.db1_columns = {}
        self.db2_columns = {}

        for table in self.db1_tables:
            self.db1_columns[table] = self.get_columns(self.db1_conn, table)

        for table in self.db2_tables:
            self.db2_columns[table] = self.get_columns(self.db2_conn, table)

        print("Колонки загружены")


    def compare_columns(self):
        """
        Сравниваем колонки таблиц и ищем те,
        которых нет в боевой базе
        """

        self.missing_columns = {}

        for table in self.db1_columns.items():

            # если таблицы нет в боевой базе — пропускаем
            if table not in self.db2_columns:
                continue

            ref_cols = self.db1_columns[table]
            target_cols = self.db2_columns[table]

            for col_name in ref_cols:

                if col_name not in target_cols:

                    if table not in self.missing_columns:
                        self.missing_columns[table] = []

                    self.missing_columns[table].append(col_name)

        if self.missing_columns:
            print("Нужно добавить колонки:", self.missing_columns)
        else:
            print("Все колонки синхронизированы")

    def generate_create_table_sql(self, table):
        cols = self.db1_columns[table]

        parts = []

        for col_name, info in cols.items():
            col_sql = f"{col_name} {info['type']}"

            if info["nullable"] == "NO":
                col_sql += " NOT NULL"

            parts.append(col_sql)

        columns_sql = ", ".join(parts)

        return f"CREATE TABLE {table} ({columns_sql});"

    def generate_add_column_sql(self, table, column):
        info = self.db1_columns[table][column]

        sql = f"ALTER TABLE {table} ADD COLUMN {column} {info['type']}"

        if info["nullable"] == "NO":
            sql += " NOT NULL"

        sql += ";"
        return sql

    def build_migration_plan(self):

        self.sql_plan = []

        # создаём таблицы
        for table in self.missing_tables:
            sql = self.generate_create_table_sql(table)
            self.sql_plan.append(sql)

        # добавляем колонки
        for table, cols in self.missing_columns.items():
            for col in cols:
                sql = self.generate_add_column_sql(table, col)
                self.sql_plan.append(sql)

        print("\nПлан миграции:")
        for s in self.sql_plan:
            print(s)

    def apply(self):

        if not self.sql_plan:
            print("Изменений нет")
            return

        cur = self.db2_conn.cursor()

        try:
            for sql in self.sql_plan:
                print("Выполняю:", sql)
                cur.execute(sql)

            self.db2_conn.commit()
            print("Миграция завершена")

        except psycopg2.Error as e:
            self.db2_conn.rollback()
            print("Ошибка, откат транзакции:", e)

        finally:
            cur.close()
