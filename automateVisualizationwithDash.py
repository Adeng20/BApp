import psycopg2
import psycopg2.sql as sql

db_connection_params = {
    "dbname": "beatbnk_db",
    "user": "user",
    "password": "X1SOrzeSrk",
    "host": "beatbnk-db-green-0j3yjq.cdgq4essi2q1.ap-southeast-2.rds.amazonaws.com",
    "port": "5432"
}

tables_to_query = [
    "SequelizeMeta", "attendees", "categories", "category_mappings",
    "event_tickets", "events", "follows", "genres", "group_permissions",
    "groups", "interests", "media_files", "media_types",
    "mpesa_stk_push_payments", "otps", "performer_genres",
    "performer_tip_payments", "performer_tips", "performers",
    "permissions", "refresh_tokens", "song_request_payments",
    "song_requests", "tickets", "user_fcm_tokens", "user_groups",
    "user_interests", "user_venue_bookings", "users", "venue_bookings",
    "venues"
]


def fetch_all_data_with_columns(db_params, table_names):
    """
    Connects to a DB, fetches all records and their column names from a list of tables,
    and closes the connection.

    Args:
        db_params (dict): Database connection parameters.
        table_names (list): A list of table names to query.

    Returns:
        dict: A dictionary mapping table names to a dictionary containing 'columns' (list of column names)
              and 'records' (list of records).
    """
    connection = None
    all_data_structured = {}
    try:
        # 1. Connect to the database
        print("Connecting to the PostgreSQL database...")
        connection = psycopg2.connect(**db_params)
        print("Connection successful.")

        # 2. Use a single cursor for all queries
        with connection.cursor() as cursor:
            print("\nFetching data from all tables with column information...")
            for table_name in table_names:
                try:
                    # Safely format the query to prevent SQL injection
                    query = sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name))
                    cursor.execute(query)

                    # Get column names from cursor description
                    columns = [desc[0] for desc in cursor.description]
                    records = cursor.fetchall()

                    all_data_structured[table_name] = {
                        "columns": columns,
                        "records": records
                    }
                    print(f"-> Fetched {len(records)} records from '{table_name}'. Columns: {columns}")
                except (Exception, psycopg2.Error) as error:
                    print(f"-> Error fetching from table '{table_name}': {error}")
                    all_data_structured[table_name] = {"columns": [], "records": []} # Assign empty on error

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"A critical database error occurred: {error}")

    finally:
        # 3. Ensure the connection is always closed
        if connection is not None:
            connection.close()
            print("Database connection closed.")

    if all_data_structured:
        print("\n--- Detailed Query Results for EDA ---")
        for table_name, data in all_data_structured.items():
            print(f"\n--- Data from Table: '{table_name}' ---")
            columns = data["columns"]
            records = data["records"]

            if columns and records:
                # Print column headers
                header_line = " | ".join(columns)
                print(header_line)
                print("-" * len(header_line))

                # Print each row, aligning values with headers
                for row in records:
                    row_values = []
                    for i, value in enumerate(row):
                        # Simple truncation for display if values are too long
                        display_value = str(value)
                        if len(display_value) > 30: # Adjust as needed
                            display_value = display_value[:27] + "..."
                        row_values.append(display_value)
                    print(" | ".join(row_values))
            elif columns and not records:
                print(f"Table '{table_name}' has columns: {columns}, but no records found.")
            else:
                print(f"No columns or records found for table '{table_name}' (possibly an error during fetch).")
            print("=" * (len(table_name) + 25)) # Separator for tables

    return all_data_structured


all_table_data = fetch_all_data_with_columns(db_connection_params, tables_to_query)