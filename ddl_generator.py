from pyspark.sql import DataFrame

def generate_ddl_statement(df: DataFrame, table_name: str) -> str:
    type_map = {
        "long": "NUMBER",
        "integer": "NUMBER",
        "string": "VARCHAR(256)",
        "double": "FLOAT",
        "float": "FLOAT",
        "timestamp": "TIMESTAMP",
        "date": "DATE",
        "boolean": "BOOLEAN"
    }
    
    columns = []
    for field in df.schema.fields:
        spark_type = field.dataType.typeName()
        snowflake_type = type_map.get(spark_type, "VARCHAR(256)")
        columns.append(f'"{field.name}" {snowflake_type}')
        
    cols_sql = ",\n    ".join(columns)
    return f'CREATE TABLE IF NOT EXISTS {table_name} (\n    {cols_sql}\n);'

def write_ddl_report(ddl_statements: list[str], output_path: str) -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n\n".join(ddl_statements))
    print("[INFO] Star-schema DDL written to:", output_path)
