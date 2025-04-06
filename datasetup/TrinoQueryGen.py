import csv

def generate_ctas_query(
    source_catalog: str,
    source_schema: str,
    source_table: str,
    partition_columns: str,
    target_catalog: str,
    target_schema: str,
    target_table: str,
    target_format: str
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
    if partition_columns:
        partition_list = [f"'{col.strip()}'" for col in partition_columns.split(",")]

        if target_format == "iceberg":
            partition_clause = f"partitioning = ARRAY[{', '.join(partition_list)}]"
        elif target_format in ["delta", "parquet"]:
            partition_clause = f"partitioned_by = ARRAY[{', '.join(partition_list)}]"

    if target_format == "parquet":
        if partition_clause:
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

    # Final CTAS query
    query = f"""
    CREATE TABLE {target_table_full} {with_clause}
    AS SELECT * FROM {source_table_full}
    """

    return query.strip()


def process_csv(input_csv: str, output_csv: str):
    """Reads a CSV file, generates CTAS queries, and updates the last column with the generated queries."""
    with open(input_csv, mode="r", newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ["Generated CTAS Query"]  # Add new column

        rows = []
        for row in reader:
            query = generate_ctas_query(
                row["Source Catalog"],
                row["Source Schema"],
                row["Source Table"],
                row["Partition Column names"],
                row["Target catalog"],
                row["Target Schema"],
                row["Target Table Name"],
                row["Target Table Format"],
            )
            row["Generated CTAS Query"] = query  # Append query to row
            rows.append(row)

    # Write back to new CSV
    with open(output_csv, mode="w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ CSV file updated successfully! Output saved to: {output_csv}")


# Example Usage
input_csv_file = "trino_input.csv"  # Replace with your actual input file
output_csv_file = "output_data.csv"  # Replace with your actual output file

process_csv(input_csv_file, output_csv_file)
