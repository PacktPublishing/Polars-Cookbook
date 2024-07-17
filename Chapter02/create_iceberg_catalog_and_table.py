import polars as pl
from pyiceberg.catalog import load_catalog
from config import postgres_pass, postgres_user

# set up catalog
uri = f'postgresql+pg8000://{postgres_user}:{postgres_pass}@localhost:5432/postgres'  
catalog = load_catalog('my_iceberg_catalog', uri=uri, warehouse='../data/my_iceberg_catalog')
namespace = 'demo'
table_name = 'demo.my_table'
catalog.create_namespace(namespace)

try:
    catalog.drop_table(table_name)
except:
    pass

# add a table to the iceberg catalog
arrow_df = pl.DataFrame({'a': [1,2,3], 'b': [4,5,6], 'c': [7,8,9]}).to_arrow()
catalog.create_table(table_name, arrow_df.schema)
tbl = catalog.load_table(table_name)
tbl.append(arrow_df)

# read an iceberg table by connecting to the iceberg catalog
lf = pl.scan_iceberg(tbl)
print(lf.head().collect())