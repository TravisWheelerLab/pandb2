#!/usr/bin/env python3

import re
from pandb_models import ApiPandbtablemetadata

to_snake = re.compile(r"(?<!^)(?=[A-Z])")

for table in ApiPandbtablemetadata.select():
    table_name = str(table.table_name)
    snake = to_snake.sub("_", table_name).lower()
    if table_name != snake:
        print(f"{table}: {table_name} => {snake}")
        table.table_name = snake
        table.save()

print('Done')
