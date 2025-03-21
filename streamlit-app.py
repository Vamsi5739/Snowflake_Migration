import streamlit as st
import pandas as pd
import time
from migration import migrate_all_tables, test_connection, get_tables, migrate_table

st.set_page_config(
    page_title="Snowflake Migration Tool",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .success-message {
        color: #0f5132;
        background-color: #d1e7dd;
        padding: 10px;
        border-radius: 5px;
        border: 1px solid #badbcc;
    }
    .error-message {
        color: #842029;
        background-color: #f8d7da;
        padding: 10px;
        border-radius: 5px;
        border: 1px solid #f5c2c7;
    }
    .info-message {
        color: #055160;
        background-color: #cff4fc;
        padding: 10px;
        border-radius: 5px;
        border: 1px solid #b6effb;
    }
</style>
""", unsafe_allow_html=True)

st.title("❄️ Snowflake Data Migration Tool")
st.write("Easily migrate tables between Snowflake databases and schemas")

# Initialize session state variables
if 'migration_results' not in st.session_state:
    st.session_state.migration_results = None
if 'source_tables' not in st.session_state:
    st.session_state.source_tables = []
if 'migration_in_progress' not in st.session_state:
    st.session_state.migration_in_progress = False

# Sidebar for connection parameters
with st.sidebar:
    st.header("Connection Settings")
    
    # Source connection parameters
    st.subheader("Source Connection")
    source_account = st.text_input("Account", key="source_account", help="Format: orgname-accountname")
    source_user = st.text_input("Username", key="source_user")
    source_password = st.text_input("Password", key="source_password", type="password")
    source_warehouse = st.text_input("Warehouse", key="source_warehouse", value="COMPUTE_WH")
    source_database = st.text_input("Database", key="source_database")
    source_schema = st.text_input("Schema", key="source_schema")
    
    # Target connection parameters
    st.subheader("Target Connection")
    target_account = st.text_input("Account", key="target_account", help="Format: orgname-accountname")
    target_user = st.text_input("Username", key="target_user")
    target_password = st.text_input("Password", key="target_password", type="password")
    target_warehouse = st.text_input("Warehouse", key="target_warehouse", value="COMPUTE_WH")
    target_database = st.text_input("Database", key="target_database")
    target_schema = st.text_input("Schema", key="target_schema")
    
    # Migration settings
    st.subheader("Migration Settings")
    batch_size = st.number_input("Batch Size", min_value=100, max_value=10000, value=2000, step=100,
                                help="Number of rows to process in each batch")
    max_workers = st.number_input("Concurrent Tables", min_value=1, max_value=8, value=4, step=1,
                                help="Number of tables to migrate simultaneously")
    
    # Test connection buttons
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Test Source"):
            source_params = {
                "user": source_user,
                "password": source_password,
                "account": source_account,
                "warehouse": source_warehouse,
                "database": source_database,
                "schema": source_schema
            }
            
            with st.spinner("Testing source connection..."):
                if test_connection(source_params):
                    st.success("Connection successful!")
                    try:
                        st.session_state.source_tables = get_tables(source_params)
                        st.info(f"Found {len(st.session_state.source_tables)} tables")
                    except Exception as e:
                        st.error(f"Error listing tables: {e}")
                else:
                    st.error("Connection failed")
    
    with col2:
        if st.button("Test Target"):
            target_params = {
                "user": target_user,
                "password": target_password,
                "account": target_account,
                "warehouse": target_warehouse,
                "database": target_database,
                "schema": target_schema
            }
            
            with st.spinner("Testing target connection..."):
                if test_connection(target_params):
                    st.success("Connection successful!")
                else:
                    st.error("Connection failed")

# Main content
tab1, tab2 = st.tabs(["Migration", "Results"])

with tab1:
    # Table selection
    if st.session_state.source_tables:
        st.header("Select Tables to Migrate")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            selected_tables = st.multiselect(
                "Choose tables", 
                options=st.session_state.source_tables,
                default=st.session_state.source_tables,
                help="Select the tables you want to migrate"
            )
        
        with col2:
            select_all = st.checkbox("Select All", value=True)
            if select_all:
                selected_tables = st.session_state.source_tables
        
        # Start migration button
        if st.button("Start Migration", type="primary", disabled=st.session_state.migration_in_progress):
            if not selected_tables:
                st.warning("Please select at least one table to migrate")
            else:
                source_params = {
                    "user": source_user,
                    "password": source_password,
                    "account": source_account,
                    "warehouse": source_warehouse,
                    "database": source_database,
                    "schema": source_schema
                }
                
                target_params = {
                    "user": target_user,
                    "password": target_password,
                    "account": target_account,
                    "warehouse": target_warehouse,
                    "database": target_database,
                    "schema": target_schema
                }
                
                st.session_state.migration_in_progress = True
                
                progress_bar = st.progress(0.0)
                status_text = st.empty()
                
                # Create placeholders for each table's status
                table_statuses = {table: st.empty() for table in selected_tables}
                
                # Start migration for each selected table individually
                results = []
                for i, table in enumerate(selected_tables):
                    status_text.text(f"Migrating table {i+1}/{len(selected_tables)}: {table}")
                    table_statuses[table].markdown(f"⏳ Migrating `{table}`...")
                    
                    result = migrate_table(table, source_params, target_params, batch_size)
                    results.append(result)
                    
                    if result["success"]:
                        table_statuses[table].markdown(f"✅ `{table}`: Migrated {result['rows_migrated']} rows in {result['time_taken']} seconds")
                    else:
                        table_statuses[table].markdown(f"❌ `{table}`: Failed - {result['error']}")
                    
                    # Update progress bar
                    progress_bar.progress((i + 1) / len(selected_tables))
                
                st.session_state.migration_results = results
                st.session_state.migration_in_progress = False
                status_text.text("Migration completed!")
                
                # Add summary
                success_count = sum(1 for r in results if r["success"])
                total_rows = sum(r["rows_migrated"] for r in results if r["success"])
                
                summary = f"""
                <div class="success-message">
                <b>Migration Summary:</b><br>
                ✅ Successfully migrated: {success_count}/{len(results)} tables<br>
                ❌ Failed: {len(results) - success_count} tables<br>
                📊 Total rows migrated: {total_rows}<br>
                ⏱️ Total time: {sum(r["time_taken"] for r in results):.2f} seconds
                </div>
                """
                st.markdown(summary, unsafe_allow_html=True)
    
    else:
        st.info("Connect to source database to view available tables")

with tab2:
    if st.session_state.migration_results:
        st.header("Migration Results")
        
        # Create a DataFrame from results
        df = pd.DataFrame(st.session_state.migration_results)
        df = df[["table", "success", "rows_migrated", "time_taken", "error"]]
        df.columns = ["Table", "Success", "Rows Migrated", "Time (sec)", "Error"]
        
        # Success/Failure metrics
        col1, col2, col3 = st.columns(3)
        success_count = sum(1 for r in st.session_state.migration_results if r["success"])
        
        with col1:
            st.metric("Total Tables", len(st.session_state.migration_results))
        with col2:
            st.metric("Successful", success_count)
        with col3:
            st.metric("Failed", len(st.session_state.migration_results) - success_count)
        
        # Show results as a table
        st.dataframe(df, use_container_width=True)
        
        # Show any errors
        errors = [(r["table"], r["error"]) for r in st.session_state.migration_results if not r["success"]]
        if errors:
            st.subheader("Error Details")
            for table, error in errors:
                st.error(f"Error in table '{table}': {error}")
    else:
        st.info("Run a migration to see results here")

# Footer
st.markdown("---")
st.markdown("Created with Streamlit • [View on GitHub](https://github.com/yourusername/snowflake-migration-tool)")
