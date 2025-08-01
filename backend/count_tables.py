#!/usr/bin/env python3

import psycopg2
import psycopg2.extras
from pandb_models import ApiPandbtablemetadata
from tabulate import tabulate

dbh = psycopg2.connect(dbname="pandb")
cur = dbh.cursor(cursor_factory=psycopg2.extras.DictCursor)
sql = """
    SELECT   relname AS table, n_live_tup AS count
    FROM     pg_stat_user_tables
    ORDER BY 1
    """
cur.execute(sql)

data = []
for rec in map(dict, cur.fetchall()):
    aliases = ApiPandbtablemetadata.select().where(
        ApiPandbtablemetadata.table_name == rec["table"]
    ).objects()

    alias = ""
    if len(aliases) == 1:
        print(aliases[0])
        alias = aliases[0].table_name_alias

    data.append((rec["table"], alias, rec["count"]))

print(tabulate(data, headers=["table", "alias", "count"]))
