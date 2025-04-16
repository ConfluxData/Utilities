import csv
import trino
import argparse

def generate_ctas_query(
    source_catalog: str,
    source_schema: str,
    source_table: str,
    partition_columns: str,
    target_catalog: str,
    target_schema: str,
    target_table: str,
    target_format: str,
    with_no_data: bool,
    trino_host: str,
    trino_port: str,
    trino_user: str
) -> str:
    """
    Generates a CREATE TABLE AS SELECT (CTAS) query for Trino with correct partitioning syntax
    for Iceberg, Delta, and Parquet table formats.
    """

    source_table_full = f"{source_catalog}.{source_schema}.{source_table}"
    target_table_full = f"{target_catalog}.{target_schema}.{target_table}"
    target_format = target_format.lower()

    # Handle partitioning syntax based on format
    partition_clause = ""
    select_columns_str="*"
    if partition_columns:
        partition_list = [f"'{col.strip()}'" for col in partition_columns.split(",")]
        if target_format == "iceberg":
            partition_clause = f"partitioning = ARRAY[{', '.join(partition_list)}]"
        elif target_format in ["delta", "parquet"]:
            partition_clause = f"partitioned_by = ARRAY[{', '.join(partition_list)}]"

    if target_format == "parquet":
        if partition_clause:
            ## get all columns
            partition_list = [f'{col.strip()}' for col in partition_columns.split(",")]
            all_columns = get_trino_table_columns(trino_host, trino_port, trino_user, source_table_full)
            print("All Columns: ", all_columns)
            print("Part Columns: ", partition_columns)
            final_columns = reorder_columns_with_partitions(all_columns, partition_list)
            print("Re-ordered Columns: ", final_columns)
            select_columns_str = ','.join(final_columns)
            partition_clause += f", format = 'PARQUET'"
        else:
            partition_clause += f"format = 'PARQUET'"

    # Construct WITH clause
    if partition_clause:
        with_clause = f"WITH ( "
        if partition_clause:
            with_clause += f"{partition_clause}"
        with_clause += ")"
    else:
        with_clause = ""

    withNoData = ""

    if with_no_data:
        withNoData = "WITH NO DATA"

    # Final CTAS query
    query = f"""
    CREATE TABLE {target_table_full} {with_clause}
    AS SELECT {select_columns_str} FROM {source_table_full} {withNoData}
    """

    return query.strip()

def generate_iis_query(
    source_catalog: str,
    source_schema: str,
    source_table: str,
    partition_columns: str,
    target_catalog: str,
    target_schema: str,
    target_table: str,
    target_format: str,
    where_predicates: str,
    trino_host: str,
    trino_port: str,
    trino_user: str
) -> str:
    """
    Generates a INSERT INTO <table> AS SELECT (IIS) query for Trino with correct partitioning syntax
    for Iceberg, Delta, and Parquet table formats.
    """

    source_table_full = f"{source_catalog}.{source_schema}.{source_table}"
    target_table_full = f"{target_catalog}.{target_schema}.{target_table}"
    target_format = target_format.lower()

    # Handle partitioning syntax based on format
    where_clause = ""
    select_columns_str="*"

    final_iis_queries = []

    if partition_columns:
        if target_format == "parquet":
            ## get all columns
            partition_list = [f'{col.strip()}' for col in partition_columns.split(",")]
            all_columns = get_trino_table_columns(trino_host, trino_port, trino_user, target_table_full)
            #print("All Columns: ", all_columns)
            #print("Part Columns: ", partition_columns)
            final_columns = reorder_columns_with_partitions(all_columns, partition_list)
            #print("Re-ordered Columns: ", final_columns)
            select_columns_str = ','.join(final_columns)

    query = f"""
        INSERT INTO {target_table_full}
        SELECT {select_columns_str} FROM {source_table_full} 
        """
    if where_predicates:
        ## get all where predicates
        predicates_list = [f'{predicate.strip()}' for predicate in where_predicates.split(",")]

        for predicate in predicates_list:
            ret_query = query + " where " + predicate
            final_iis_queries.append(ret_query)
    else:
        final_iis_queries.append(ret_query)

    return final_iis_queries


def reorder_columns_with_partitions(all_columns, partition_columns):
    """
    Reorders columns so that partition columns are at the end.

    Parameters:
    - all_columns (list of str): List of all column names in the table.
    - partition_columns (list of str): List of column names that should be treated as partition columns.

    Returns:
    - list of str: Reordered list with partition columns at the end.
    """
    #partition_set = set(partition_columns)
    print("Part Set: ", partition_columns)
    non_partition_columns = [col for col in all_columns if col not in partition_columns]
    print("Non Part Set: ", non_partition_columns)
    final_columns = non_partition_columns + [col for col in partition_columns if col in all_columns]
    return final_columns

def get_trino_table_columns(trino_host, trino_port, user, catalog_schema_table):
    """
    Connects to Trino and fetches column names for a given table.

    Parameters:
    - trino_host (str): Hostname or IP of the Trino coordinator.
    - trino_port (int): Port where Trino is listening (default: 8080).
    - user (str): Username for Trino.
    - catalog_schema_table (str): Fully qualified table name in the format 'catalog.schema.table'.

    Returns:
    - list of str: List of column names.
    """
    catalog, schema, table = catalog_schema_table.split(".")

    conn = trino.dbapi.connect(
        host=trino_host,
        port=trino_port,
        user=user,
        catalog=catalog,
        schema=schema,
    )

    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE {table}")
    rows = cursor.fetchall()
    columns = [row[0] for row in rows if row[0] and not row[0].startswith('#')]
    return columns

def process_csv(input_csv: str, output_csv: str, trino_host: str, trino_port: str, trino_user: str):
    """Reads a CSV file, generates CTAS queries, and updates the last column with the generated queries."""
    with open(input_csv, mode="r", newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ["Generated CTAS Query"]  # Add new column

        rows = []
        for row in reader:
            type = row["type"]
            if type == "CTAS" or type == "CTASND" :
                query = generate_ctas_query(
                    row["Source Catalog"],
                    row["Source Schema"],
                    row["Source Table"],
                    row["Partition Column names"],
                    row["Target catalog"],
                    row["Target Schema"],
                    row["Target Table Name"],
                    row["Target Table Format"],
                    type == "CTASND",
                    trino_host,
                    trino_port,
                    trino_user
                )
                row["Generated CTAS Query"] = query  # Append query to row
                rows.append(row)
            elif type == "IIS":
                queries = generate_iis_query(
                    row["Source Catalog"],
                    row["Source Schema"],
                    row["Source Table"],
                    row["Partition Column names"],
                    row["Target catalog"],
                    row["Target Schema"],
                    row["Target Table Name"],
                    row["Target Table Format"],
                    row["where_predicate"],
                    trino_host,
                    trino_port,
                    trino_user
                )

                for query in queries:
                    row["Generated CTAS Query"] = query  # Append query to row
                    rows.append(row)

    # Write back to new CSV
    with open(output_csv, mode="w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ CSV file updated successfully! Output saved to: {output_csv}")


# Example Usage
#input_csv_file = "trino_input.csv"  # Replace with your actual input file
#output_csv_file = "output_data.csv"  # Replace with your actual output file

#process_csv(input_csv_file, output_csv_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate SQL queries from a CSV file onto Trino.")
    parser.add_argument("--input_csv_file", default="trino_input.csv", help="Path to the CSV file containing Table information.")
    parser.add_argument("--output_csv_file", default="output_data.csv", help="Name of the CSV file for keeping generated queries.")
    parser.add_argument("--trino_host", default="localhost", help="Trino server hostname or IP address.")
    parser.add_argument("--trino_port", type=int, default=8080, help="Trino server port.")
    parser.add_argument("--trino_user", default="admin", help="Username for Trino.")
    
    args = parser.parse_args()
    process_csv(args.input_csv_file, args.output_csv_file, args.trino_host, args.trino_port, args.trino_user)

    #process_csv(args.csv_file, args.trino_host, args.trino_port, args.trino_user)

