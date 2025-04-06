import csv
import trino
import argparse

def execute_query(trino_host, trino_port, trino_user, catalog, schema, query):
    try:
        conn = trino.dbapi.connect(
            host=trino_host,
            port=trino_port,
            user=trino_user,
            catalog=catalog,
            schema=schema,
        )
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        print(f"Query executed successfully on {catalog}.{schema}: {query}")
        for row in results:
            print(row)
    except Exception as e:
        print(f"Error executing query on {catalog}.{schema}: {e}")

def process_csv(csv_file, trino_host, trino_port, trino_user):
    with open(csv_file, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            execute_query(
                trino_host,
                trino_port,
                trino_user,
                row['Source Catalog'],
                row['Source Schema'],
                row['Generated CTAS Query']
            )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Execute SQL queries from a CSV file onto Trino.")
    parser.add_argument("--csv_file", default="output_data.csv", help="Path to the CSV file containing SQL queries.")
    parser.add_argument("--trino_host", default="localhost", help="Trino server hostname or IP address.")
    parser.add_argument("--trino_port", type=int, default=8080, help="Trino server port.")
    parser.add_argument("--trino_user", default="admin", help="Username for Trino.")
    
    args = parser.parse_args()
    process_csv(args.csv_file, args.trino_host, args.trino_port, args.trino_user)
