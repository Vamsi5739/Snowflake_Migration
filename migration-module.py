import snowflake.connector
from concurrent.futures import ThreadPoolExecutor
import time
from typing import Dict, List, Any

def migrate_table(table: str, source_params: Dict[str, Any], target_params: Dict[str, Any], batch_size: int = 2000) -> Dict[str, Any]:
    """
    Migrate a single table from source to target Snowflake instance.
    
    Args:
        table: Table name to migrate
        source_params: Source connection parameters
        target_params: Target connection parameters
        batch_size: Number of rows to process in each batch
        
    Returns:
        Dict with migration status and details
    """
    start_time = time.time()
    result = {
        "table": table,
        "success": False,
        "rows_migrated": 0,
        "error": None,
        "time_taken": 0
    }
    
    try:
        source_conn = snowflake.connector.connect(**source_params)
        source_cursor = source_conn.cursor()
 
        target_conn = snowflake.connector.connect(**target_params)
        target_cursor = target_conn.cursor()
 
        # Fetch CREATE TABLE statement
        source_cursor.execute(f"SELECT GET_DDL('TABLE', '{source_params['database']}.{source_params['schema']}.{table}')")
        create_table_statement = source_cursor.fetchone()[0]
 
        # Replace schema/database references for target
        create_table_statement = create_table_statement.replace(
            f"{source_params['database']}.{source_params['schema']}",
            f"{target_params['database']}.{target_params['schema']}"
        )
 
        # Create the table in the target schema
        target_cursor.execute(f"DROP TABLE IF EXISTS {target_params['database']}.{target_params['schema']}.{table}")
        target_cursor.execute(create_table_statement)
        target_conn.commit()
 
        # Fetch column names
        source_cursor.execute(f"SELECT COLUMN_NAME FROM {source_params['database']}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}' AND TABLE_SCHEMA = '{source_params['schema']}'")
        columns = [row[0] for row in source_cursor.fetchall()]
        column_list = ', '.join(columns)
 
        # Copy data in batches
        offset = 0
        total_rows = 0
        while True:
            source_cursor.execute(f"""
                SELECT {column_list} FROM {source_params['database']}.{source_params['schema']}.{table}
                LIMIT {batch_size} OFFSET {offset}
            """)
            rows = source_cursor.fetchall()
            
            if not rows:
                break
 
            insert_query = f"""
                INSERT INTO {target_params['database']}.{target_params['schema']}.{table} ({column_list})
                VALUES ({', '.join(['%s'] * len(columns))})
            """
            target_cursor.executemany(insert_query, rows)
            target_conn.commit()
            
            offset += batch_size
            total_rows += len(rows)
            result["rows_migrated"] = total_rows
 
        result["success"] = True

    except Exception as e:
        result["error"] = str(e)
    finally:
        if 'source_cursor' in locals():
            source_cursor.close()
        if 'source_conn' in locals():
            source_conn.close()
        if 'target_cursor' in locals():
            target_cursor.close()
        if 'target_conn' in locals():
            target_conn.close()
            
        result["time_taken"] = round(time.time() - start_time, 2)
        return result
 
def get_tables(conn_params: Dict[str, Any]) -> List[str]:
    """
    Get all tables from the specified schema.
    
    Args:
        conn_params: Connection parameters
        
    Returns:
        List of table names
    """
    tables = []
    try:
        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()
 
        # Fetch all table names from source schema
        cursor.execute(f"SELECT TABLE_NAME FROM {conn_params['database']}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{conn_params['schema']}'")
        tables = [row[0] for row in cursor.fetchall()]
 
    except Exception as e:
        raise Exception(f"Error fetching tables: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
            
    return tables

def migrate_all_tables(source_params: Dict[str, Any], target_params: Dict[str, Any], batch_size: int = 2000, max_workers: int = 4) -> List[Dict[str, Any]]:
    """
    Migrate all tables from source to target Snowflake instance.
    
    Args:
        source_params: Source connection parameters
        target_params: Target connection parameters
        batch_size: Number of rows to process in each batch
        max_workers: Number of concurrent migrations
        
    Returns:
        List of migration results
    """
    # Fetch all table names from source schema
    tables = get_tables(source_params)
    results = []
    
    # Use ThreadPoolExecutor to migrate tables concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_table = {
            executor.submit(migrate_table, table, source_params, target_params, batch_size): table 
            for table in tables
        }
        
        for future in future_to_table:
            results.append(future.result())
    
    return results

def test_connection(conn_params: Dict[str, Any]) -> bool:
    """
    Test if connection parameters are valid.
    
    Args:
        conn_params: Connection parameters
        
    Returns:
        True if connection successful, False otherwise
    """
    try:
        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        return True
    except Exception:
        return False
