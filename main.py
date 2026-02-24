from dbsync import DBSync

DB1 = "postgresql://user:pass@localhost:5432/db1"
DB2 = "postgresql://user:pass@localhost:5432/db2"

sync = DBSync(DB1, DB2)
sync.connect()

sync.load_tables()
sync.compare_tables()

sync.load_columns()
sync.compare_columns()

sync.build_migration_plan()

# сначала смотрим план, потом применяем
sync.apply()

sync.close()
