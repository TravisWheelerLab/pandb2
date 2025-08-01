from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
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
    """Table metadata"""

    table_name: str
    alias: str


class DataDictionary(BaseModel):
    """A data_dictionary record"""

    table_name: str
    alias: str
    field_label: str
    definition: Optional[str]


class DataResult(BaseModel):
    """Return type for get_data"""

    count: int
    results: List[Dict[str, Any]]
    columns: List[str]


class ColumnMeta(BaseModel):
    """Column metadata"""

    name: str
    alias: str


# --------------------------------------------------
def get_cur():
    """Get db cursor"""

    return dbh.cursor(cursor_factory=psycopg2.extras.DictCursor)


# --------------------------------------------------
@app.get("/get_tables")
def get_tables() -> List[TableMeta]:
    """Get data table names/aliases"""

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
    """Get data for primary table and joins"""

    records = []
    columns = []
    count = 0
    try:
        cur = get_cur()

        # Find the column names/aliases
        select_columns = []
        select_tables = [primary_table]
        for col in get_columns(cur, primary_table):
            select_columns.append(
                f'{primary_table}.{col.name} as "{col.alias}"'
            )
            columns.append(col.alias)

        if additional_tables.strip():
            for additional_table in re.split(r"\s*,\s*", additional_tables):
                table_alias = get_table_alias(additional_table)
                for col in get_columns(cur, additional_table):
                    full_col_alias = f"{table_alias}::{col.alias}"
                    select_columns.append(
                        f'{additional_table}.{col.name} as "{full_col_alias}"'
                    )
                    columns.append(full_col_alias)
                select_tables.append(additional_table)

        from_clause = ", ".join(select_tables)
        where_clause = ""
        if join_col and len(select_tables) > 1:
            parts = []
            for table in select_tables[1:]:
                parts.append(
                    f"{primary_table}.{join_col}={table}.{join_col}"
                )
            where_clause = f"where {' and '.join(parts)}"

        # Count the records
        count_sql = f"""
            select count(*) as count
            from   {from_clause}
            {where_clause}
        """
        print(count_sql)
        cur.execute(count_sql)
        if res := cur.fetchone():
            count = res[0]

        # Select the records
        select_sql = f"""
            select {"\n, ".join(select_columns)}
            from   {from_clause}
            {where_clause}
            limit  {limit}
            offset {offset}
        """
        print(select_sql)

        cur.execute(select_sql)
        records = list(map(dict, cur.fetchall()))
    except Exception as err:
        print(f"ERROR: {err}")
        dbh.rollback()
        raise HTTPException(status_code=500, detail=str(err))
    finally:
        cur.close()

    return DataResult(results=records, count=count, columns=columns)


# --------------------------------------------------
def get_columns(cur, table_name: str) -> List[ColumnMeta]:
    """Find the column names/aliases for a table"""

    column_sql = """
        select  c.column_name,
                c.column_name_alias as alias,
                t.table_name
        from    api_pandbcolumnmetadata c,
                api_pandbtablemetadata t
        where   c.is_visible=true
        and     c.table_id=t.id
        and     t.table_name=%s
    """

    cur.execute(column_sql, (table_name,))
    columns = []
    for name, alias, table_name in cur.fetchall():
        if not alias:
            alias = snake_to_title(name)
        columns.append(ColumnMeta(name=name, alias=alias))

    if not columns:
        raise Exception(f"Table '{table_name}' has no columns")

    return columns


# --------------------------------------------------
@app.get("/get_data_dictionary")
def get_data_dictionary() -> List[DataDictionary]:
    """Get fields/aliases/table"""

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


# --------------------------------------------------
def get_table_alias(table_name: str) -> Optional[str]:
    """Get table alias"""

    res = []
    try:
        cur = get_cur()
        cur.execute(
            """
            select table_name_alias as alias
            from   api_pandbtablemetadata
            where  table_name=%s
            """,
            (table_name,),
        )

        if res := cur.fetchone():
            alias = res[0]
            return alias if alias else snake_to_title(table_name)
        else:
            raise Exception(f"Table '{table_name}' not found")
    except Exception:
        dbh.rollback()
    finally:
        cur.close()


# --------------------------------------------------
def snake_to_title(name: str) -> str:
    """Turn 'snake_case' into 'Snake Case'"""

    return " ".join(map(str.capitalize, name.split("_")))
