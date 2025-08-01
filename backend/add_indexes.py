#!/usr/bin/env python3

import psycopg2
import psycopg2.extras

dbh = psycopg2.connect(dbname="pandb")
cur = dbh.cursor(cursor_factory=psycopg2.extras.DictCursor)
sql = """
    SELECT table_name, column_name
      FROM information_schema.columns
     WHERE table_schema = 'public'
       AND column_name in ('hml_id', 'mindcrowd_id')
    """
cur.execute(sql)

for rec in cur.fetchall():
    print(
        "create index on {} ({});".format(
            rec["table_name"], rec["column_name"]
        )
    )
