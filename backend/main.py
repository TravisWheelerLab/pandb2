from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
import re

dbname = "pandb"
dbh = psycopg2.connect(dbname=dbname)

app = FastAPI(root_path="/api/v1")
origins = [
    "http://localhost:*",
    "*",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class TableMeta(BaseModel):
    table_name: str
    alias: str


class DataDictionary(BaseModel):
    table_name: str
    alias: str
    field_label: str
    definition: Optional[str]


class DataResult(BaseModel):
    count: int
    results: List[Dict[str, Any]]
    columns: List[str]


# --------------------------------------------------
def get_cur():
    """Get db cursor"""

    return dbh.cursor(cursor_factory=psycopg2.extras.DictCursor)


# --------------------------------------------------
@app.get("/get_tables")
def get_tables() -> List[TableMeta]:
    sql = """
        select   table_name, table_name_alias as alias
        from     api_pandbtablemetadata
        where    is_visible=true
        order by alias
    """

    to_snake = re.compile(r"(?<!^)(?=[A-Z])")

    def f(rec):
        return TableMeta(
            table_name=to_snake.sub("_", rec["table_name"]).lower(),
            alias=rec["alias"],
        )

    res = []
    try:
        cur = get_cur()
        cur.execute(sql)
        res = cur.fetchall()
    except Exception:
        dbh.rollback()
    finally:
        cur.close()

    return list(map(f, res))


# --------------------------------------------------
@app.get("/get_data")
def get(
    primary_table: str,
    additional_tables: Optional[str] = "",
    join_col: Optional[str] = "",
    limit: Optional[int] = 10,
    offset: Optional[int] = 0,
) -> DataResult:
    column_sql = """
        select  c.column_name,
                c.column_name_alias as alias,
                t.table_name
        from    api_pandbcolumnmetadata c,
                api_pandbtablemetadata t
        where   c.is_visible=true
        and     c.column_name!='pk'
        and     c.table_id=t.id
        and     t.table_name=%s
    """

    count_sql = f"""
        select count(*) as count
        from   {primary_table}
    """

    records = []
    columns = []
    count = 0
    try:
        cur = get_cur()

        # Count the records
        cur.execute(count_sql)
        if res := cur.fetchone():
            count = res[0]

        # Find the column names/aliases
        cur.execute(column_sql, (primary_table,))
        select_cols = []
        for name, alias, table_name in cur.fetchall():
            if not alias:
                alias = " ".join(map(str.capitalize, name.split("_")))
            select_cols.append(f'{table_name}.{name} as "{alias}"')
            columns.append(alias)

        # Select the records
        select_sql = f"""
            select {", ".join(select_cols)}
            from   {primary_table}
            limit  {limit}
            offset {offset}
        """
        print(select_sql)

        cur.execute(select_sql)
        records = list(map(dict, cur.fetchall()))
    except Exception as e:
        print(f"ERROR: {e}")
        dbh.rollback()
    finally:
        cur.close()

    return DataResult(results=records, count=count, columns=columns)


# --------------------------------------------------
@app.get("/get_data_dictionary")
def get_data_dictionary() -> List[DataDictionary]:
    sql = """
        select d.table_name,
               m.table_name_alias as alias,
               d.field_label,
               d.definition
        from   info_data_dictionary d, api_pandbtablemetadata m
        where  d.table_name=m.table_name
    """

    def f(rec):
        return DataDictionary(
            table_name=rec["table_name"],
            alias=rec["alias"],
            field_label=rec["field_label"],
            definition=rec["definition"],
        )

    res = []
    try:
        cur = get_cur()
        cur.execute(sql)
        res = cur.fetchall()

    except Exception:
        dbh.rollback()
    finally:
        cur.close()

    return list(map(f, res))
