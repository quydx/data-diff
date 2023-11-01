import data_diff

table1 = data_diff.connect_to_table(
    "trino://admin@10.159.19.101:8080/iceberg_hms/default",
    table_name=("postgres121vietpq__kafka_hudi_test_test_user"),
)

print(table1.sum_column("test_addcol"))

table2 = data_diff.connect_to_table(
    "postgresql://postgres:changeme@10.159.19.121:5431/test",
    table_name=("kafka_hudi_test.test_user"),
)

print(table2.sum_column("test_addcol"))

result1 = data_diff.diff_tables(
    table1=table1, table2=table2,
    key_columns=[("test_addcol")],
    extra_columns=["name", "user_id"],
    algorithm="hashdiff"
)
print(f"Number of differnt records following int: {len(list(result1))}")
result2 = data_diff.diff_tables(
    table1=table1, table2=table2,
    key_columns=[("user_id")],
    extra_columns=["name", "test_addcol"],
    algorithm="hashdiff"
)
print(f"Number of differnt records following varchar: {len(list(result2))}")


table3 = data_diff.connect_to_table(
    "oracle://dbzuser:dbz@10.159.19.101:1521/pdbdev",
    table_name=("dbzuser.user_table"),
    key_columns="user_id"
)

print(table3.count_with_condition("test_addcol > 10 and test_addcol < 200"))
print(table3.sum_column_with_condition("test_addcol", "test_addcol > 10 and test_addcol < 200"))
print(table3.get_schema())
