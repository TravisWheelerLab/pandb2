from typing import List, Optional  # Union

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


class DataDictionaryResult(BaseModel):
    data_dictionary: List[DataDictionary]
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
@app.get("/get/{table_name}")
def get(table_name: str):
    sql = f"""
        select *
        from   {table_name}
        limit  10
    """

    res = []
    try:
        cur = get_cur()
        cur.execute(sql)
        res = cur.fetchall()
    except Exception:
        dbh.rollback()
    finally:
        cur.close()

    return list(map(dict, res))


# --------------------------------------------------
@app.get("/get_data_dictionary")
def get_data_dictionary() -> DataDictionaryResult:
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
    columns = []
    try:
        cur = get_cur()
        cur.execute(sql)
        res = cur.fetchall()

        if len(res) > 0:
            columns = res[0].keys()

    except Exception:
        dbh.rollback()
    finally:
        cur.close()

    return {"data_dictionary": list(map(f, res)), "columns": columns}
