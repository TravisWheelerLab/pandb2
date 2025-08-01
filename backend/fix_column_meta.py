#!/usr/bin/env python3

from pandb_models import ApiPandbcolumnmetadata
import psycopg2
import psycopg2.extras

dbh = psycopg2.connect(dbname="pandb")
cur = dbh.cursor(cursor_factory=psycopg2.extras.DictCursor)
sql = """
    SELECT *
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name   = %s
       AND column_name  = %s
    """

for rec in ApiPandbcolumnmetadata.select():
    table = rec.table.table_name
    column = rec.column_name
    cur.execute(sql, (table, column))
    res = cur.fetchone()
    found = "FOUND" if res else "MISSING"
    print(f"{found:10}: {table}.{column}")

    if not res:
        ApiPandbcolumnmetadata.delete_by_id(rec.id)

print('Done')
