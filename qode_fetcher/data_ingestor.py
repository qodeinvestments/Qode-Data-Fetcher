import streamlit as st
import pandas as pd
import os
import time
from datetime import datetime
from auth import get_current_user
from database import get_all_tables, ingest_data_to_duckdb, prepare_dataframe_for_append, fuzzy_search_tables, get_table_metadata, get_table_sample_data, append_data_to_table, validate_table_compatibility, download_table_data
from data_utils import save_upload_log
from file_operations import convert_column_dtype, check_file_exists, get_file_size, load_data_file_with_header, load_data_preview, analyze_dataframe, get_column_statistics, detect_quarterly_columns, detect_accord_code_columns, detect_timestamp_columns, has_accord_code_columns, create_quarterly_visualization


def clear_session_data():
    """Clear all session state data related to file upload and analysis"""
    keys_to_clear = [
        'data_loaded', 'df', 'file_info', 'analysis_computed', 'cached_analysis',
        'selected_stat_column', 'selected_quarterly_col', 'file_name', 'file_size'
    ]
    
    for key in keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]


def ingest_data(conn):
    st.title("Upload Data")
    st.markdown("---")

    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
    if 'df' not in st.session_state:
        st.session_state.df = None
    if 'file_info' not in st.session_state:
        st.session_state.file_info = {}
    if 'analysis_computed' not in st.session_state:
        st.session_state.analysis_computed = False
    if 'cached_analysis' not in st.session_state:
        st.session_state.cached_analysis = None
    
    with st.expander("📋 Data Upload Guidelines", expanded=False):
        st.markdown("""
        **Data Quality Requirements:**
        - Ensure data has minimal timestamp gaps for time series data
        - Verify data integrity before upload (no corrupted files)
        - Check for existing similar datasets to avoid duplication
        - Recommended file size: < 500MB for optimal performance
        
        **Supported Formats:**
        - CSV, Excel (xlsx/xls), Parquet, Pickle, CSV GZIP
        
        **Table Naming Convention:**
        `EXCHANGE_INSTRUMENT_UNDERLYING_[ADDITIONAL_PARAMS]`
        
        **Examples:**
        - Index: `NSE_Index_NIFTY`, `BSE_Index_SENSEX`
        - Options: `NSE_Options_NIFTY_20240125_21000_call`
        - Stocks: `NSE_Stocks_RELIANCE`, `BSE_Stocks_TCS`
        - Economic: `FRED_Economic_GDP_Quarterly`
        """)
    
    st.subheader("Upload Method")
    
    upload_method = st.radio(
        "Choose upload method:",
        options=["Upload File", "Server File Path"],
        horizontal=True
    )
    
    uploaded_file = None
    file_path = None
    file_name = None
    file_size = 0
    
    if upload_method == "Upload File":
        
        col1, col2, col3 = st.columns([1, 1, 2])
        
        with col1:
            if st.button("Refresh", 
                        help="Clear all uploaded data and reset the form", 
                        type="secondary"):
                clear_session_data()
                st.success("✅ Session data cleared!")
                st.rerun()
                
        uploaded_file = st.file_uploader(
            "Choose a file",
            type=['csv', 'xlsx', 'xls', 'parquet', 'pkl', 'gz'],
            help="Select a financial data file to upload"
        )
        
        if uploaded_file:
            file_size = uploaded_file.size
            file_name = uploaded_file.name
            st.info(f"File size: {file_size / (1024*1024):.2f} MB")
            
            if file_size > 500 * 1024 * 1024:
                st.warning("⚠️ File size exceeds 500MB. Consider splitting the data for better performance.")
    
    else:
        file_path = st.text_input(
            "Enter server file path:",
            placeholder="/path/to/your/data/file.csv",
            help="Full path to the file on the server"
        )
        
        if file_path:
            with st.spinner("🔍 Checking file existence and size..."):
                if check_file_exists(file_path):
                    file_size = get_file_size(file_path)
                    file_name = os.path.basename(file_path)
                    st.success(f"✅ File found. Size: {file_size / (1024*1024):.2f} MB")
                    
                    if file_size > 500 * 1024 * 1024:
                        st.warning("⚠️ File size exceeds 500MB. Consider splitting the data for better performance.")
                else:
                    st.error("❌ File not found on server")
    
    if (uploaded_file or file_path) and file_name and not st.session_state.data_loaded:
        st.markdown("---")
        st.subheader("📋 Header Row Selection")
        
        with st.spinner("📄 Loading file preview (first 20 rows)..."):
            preview_df = load_data_preview(file_path, uploaded_file, max_rows=20)
        
        if preview_df is not None:
            st.write("**File Preview (First 20 rows):**")
            st.dataframe(preview_df, use_container_width=True)
            
            header_row = st.number_input(
                "Which row contains the column headers?",
                min_value=0,
                max_value=len(preview_df)-1,
                value=0,
                help="Row index (0-based) that contains the column names. Default is 0 (first row)."
            )
            
            st.write(f"**Selected header row (Row {header_row}):**")
            if header_row < len(preview_df):
                header_preview = preview_df.iloc[header_row].tolist()
                st.dataframe(header_preview)
            
            if st.button("✅ Confirm Header and Load Data"):
                with st.spinner("Loading complete dataset with processing..."):
                    df_result, _ = load_data_file_with_header(file_path, uploaded_file, header_row)
                    
                    if df_result is not None:
                        st.session_state.df = df_result
                        st.session_state.file_info = {
                            'file_name': file_name,
                            'file_size': file_size
                        }
                        st.session_state.data_loaded = True
                        st.session_state.analysis_computed = False
                        st.success("✅ Data loaded successfully!")
                        st.rerun()
    
    if st.session_state.data_loaded and st.session_state.df is not None:
        df = st.session_state.df
        file_name = st.session_state.file_info['file_name']
        file_size = st.session_state.file_info['file_size']
        
        st.markdown("---")
        st.subheader("Data Analysis")
        
        if not st.session_state.analysis_computed:
            with st.spinner("🔍 Analyzing dataset structure and computing statistics..."):
                analysis = analyze_dataframe(df)
                st.session_state.cached_analysis = analysis
                st.session_state.analysis_computed = True
        else:
            analysis = st.session_state.cached_analysis
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Rows", f"{analysis['shape'][0]:,}")
        with col2:
            st.metric("Columns", analysis['shape'][1])
        with col3:
            st.metric("Memory Usage", f"{analysis['memory_usage'] / (1024*1024):.2f} MB")
        with col4:
            st.metric("Duplicate Rows", analysis['duplicate_rows'])
        
        if analysis['duplicate_rows'] > 0:
            st.warning(f"⚠️ Found {analysis['duplicate_rows']} duplicate rows. Consider removing duplicates.")
        
        st.subheader("Column Information")
        
        col_info_df = pd.DataFrame({
            'Column': analysis['columns'],
            'Data Type': [str(analysis['dtypes'][col]) for col in analysis['columns']],
            'Null Count': [analysis['null_counts'][col] for col in analysis['columns']],
            'Null %': [f"{(analysis['null_counts'][col] / analysis['shape'][0]) * 100:.1f}%" for col in analysis['columns']]
        })
        
        st.dataframe(col_info_df, use_container_width=True)
        
        st.subheader("Column Statistics")
        
        if 'selected_stat_column' not in st.session_state:
            st.session_state.selected_stat_column = df.columns.tolist()[0] if len(df.columns) > 0 else None
        
        selected_stat_column = st.selectbox(
            "Select a column to view detailed statistics:",
            options=df.columns.tolist(),
            index=df.columns.tolist().index(st.session_state.selected_stat_column) if st.session_state.selected_stat_column in df.columns else 0,
            help="Choose a column to see comprehensive statistics",
            key="stat_column_selector"
        )
        
        if selected_stat_column != st.session_state.selected_stat_column:
            st.session_state.selected_stat_column = selected_stat_column
        
        if selected_stat_column:
            with st.spinner(f"Computing detailed statistics for '{selected_stat_column}'..."):
                stats = get_column_statistics(df, selected_stat_column)
            
            with st.expander(f"Detailed Statistics for '{selected_stat_column}'", expanded=True):
                if stats['type'] == 'numeric':
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Count", stats['non_null_count'])
                        st.metric("Mean", f"{stats['mean']:.4f}")
                        st.metric("Min", f"{stats['min']:.4f}")
                    with col2:
                        st.metric("Null Count", stats['null_count'])
                        st.metric("Std Dev", f"{stats['std']:.4f}")
                        st.metric("Max", f"{stats['max']:.4f}")
                    with col3:
                        st.metric("Median", f"{stats['median']:.4f}")
                        st.metric("25th Percentile", f"{stats['q25']:.4f}")
                        st.metric("Skewness", f"{stats['skewness']:.4f}")
                    with col4:
                        st.metric("Variance", f"{stats['variance']:.4f}")
                        st.metric("75th Percentile", f"{stats['q75']:.4f}")
                        st.metric("Kurtosis", f"{stats['kurtosis']:.4f}")
                    
                    st.metric("IQR (Interquartile Range)", f"{stats['iqr']:.4f}")
                
                else:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Count", stats['total_count'])
                        st.metric("Non-null Count", stats['non_null_count'])
                    with col2:
                        st.metric("Null Count", stats['null_count'])
                        st.metric("Unique Values", stats['unique_count'])
                    with col3:
                        st.metric("Most Frequent", str(stats['most_frequent']))
                        st.metric("Frequency", stats['most_frequent_count'])
                    
                    st.write("**Top 10 Values:**")
                    top_values_df = pd.DataFrame(list(stats['top_values'].items()), 
                                               columns=['Value', 'Count'])
                    st.dataframe(top_values_df, use_container_width=True)
        
        st.subheader("Data Preview")
        
        tab1, tab2 = st.tabs(["First 10 Rows", "Last 10 Rows"])
        
        with tab1:
            st.dataframe(df.head(10), use_container_width=True)
        
        with tab2:
            st.dataframe(df.tail(10), use_container_width=True)
        
        with st.spinner("🔍 Detecting data patterns (quarterly, timestamps, accord codes)..."):
            is_quarterly_file = 'quarterly' in file_name.lower()
            has_accord_code = has_accord_code_columns(df)
            accord_code_cols = detect_accord_code_columns(df)
        
        if is_quarterly_file and has_accord_code:
            st.markdown("---")
            with st.spinner("Analyzing quarterly data structure..."):
                quarterly_candidates = detect_quarterly_columns(df, file_name)
            
            if quarterly_candidates:
                if 'selected_quarterly_col' not in st.session_state:
                    st.session_state.selected_quarterly_col = quarterly_candidates[0]
                
                selected_quarterly_col = st.selectbox(
                    "Select quarterly column for visualization:",
                    options=quarterly_candidates,
                    index=quarterly_candidates.index(st.session_state.selected_quarterly_col) if st.session_state.selected_quarterly_col in quarterly_candidates else 0,
                    help="Choose the column containing quarterly periods for analysis",
                    key="quarterly_col_selector"
                )
                
                if selected_quarterly_col != st.session_state.selected_quarterly_col:
                    st.session_state.selected_quarterly_col = selected_quarterly_col
                
                if selected_quarterly_col:
                    with st.spinner("Creating quarterly data visualization..."):
                        create_quarterly_visualization(df, selected_quarterly_col, accord_code_cols)
        
        st.markdown("---")
        st.subheader("⚙️ Data Configuration")
        
        with st.spinner("Fetching existing database tables..."):
            existing_tables = get_all_tables(conn)
        
        col1, col2 = st.columns(2)
        
        with col1:
            if is_quarterly_file and has_accord_code:
                st.write("**Quarterly Column:**")
                quarterly_candidates = detect_quarterly_columns(df, file_name)
                quarterly_col = st.selectbox(
                    "Select quarterly/period column:",
                    options=["None"] + quarterly_candidates + [col for col in df.columns if col not in quarterly_candidates],
                    help="Choose the column containing quarterly periods"
                )
            else:
                st.write("**Timestamp Column:**")
                with st.spinner("Detecting timestamp columns..."):
                    timestamp_candidates = detect_timestamp_columns(df)
                timestamp_col = st.selectbox(
                    "Select timestamp column:",
                    options=["None"] + timestamp_candidates + [col for col in df.columns if col not in timestamp_candidates],
                    help="Choose the column containing timestamps"
                )
        
        with col2:
            st.write("**Columns to Include:**")
            selected_columns = st.multiselect(
                "Select columns to include:",
                options=df.columns.tolist(),
                default=df.columns.tolist(),
                help="Choose which columns to include in the database"
            )
        
        if selected_columns:
            st.write("**Column Data Type Conversion:**")
            
            dtype_changes = {}
            cols = st.columns(min(3, len(selected_columns)))
            
            for i, col in enumerate(selected_columns):
                with cols[i % 3]:
                    current_dtype = str(df[col].dtype)
                    
                    if (is_quarterly_file and has_accord_code and col == quarterly_col) or \
                       (not (is_quarterly_file and has_accord_code) and col == timestamp_col):
                        new_dtype = st.selectbox(
                            f"{col}:",
                            options=["datetime"],
                            index=0,
                            key=f"dtype_{col}"
                        )
                    elif df[col].dtype in ['float64']:
                        new_dtype = st.selectbox(
                            f"{col}:",
                            options=["float32", "float64"],
                            index=1 if current_dtype == "float64" else 0,
                            key=f"dtype_{col}"
                        )
                    elif df[col].dtype in ['int64']:
                        new_dtype = st.selectbox(
                            f"{col}:",
                            options=["int32", "int64"],
                            index=1 if current_dtype == "int64" else 0,
                            key=f"dtype_{col}"
                        )
                    else:
                        new_dtype = st.selectbox(
                            f"{col}:",
                            options=["string", "float32", "float64", "int32", "int64", "datetime"],
                            index=0,
                            key=f"dtype_{col}"
                        )
                    
                    if new_dtype != current_dtype:
                        dtype_changes[col] = new_dtype
            
            row_limit = st.number_input(
                "Number of rows to ingest:",
                min_value=1,
                max_value=len(df),
                value=len(df),
                help="Choose how many rows to include (from the top)"
            )

            table_name = st.text_input(
                "Table Name:",
                placeholder="e.g., NSE_Stocks_RELIANCE_Daily",
                help="Follow the naming convention: EXCHANGE_INSTRUMENT_UNDERLYING_[PARAMS]"
            )

            if table_name:
                with st.spinner("🔍 Searching for similar table names..."):
                    similar_tables = fuzzy_search_tables(existing_tables, table_name, limit=5)
                    table_exists = table_name in existing_tables
                
                if similar_tables or table_exists:
                    st.write("**Table Selection:**")
                    
                    dropdown_options = ["Create new table"]
                    if table_exists:
                        dropdown_options.append(f"Append to existing: {table_name}")
                    
                    for similar_table in similar_tables:
                        if similar_table != table_name:
                            dropdown_options.append(f"Append to existing: {similar_table}")
                    
                    selected_option = st.selectbox(
                        "Choose an option:",
                        options=dropdown_options,
                        help="Select whether to append to an existing table or create a new one"
                    )
                    
                    if selected_option == "Create new table" and table_exists:
                        st.error(f"❌ Table '{table_name}' already exists. Please choose a different name or select append option.")
                        st.stop()
                    
                    if selected_option.startswith("Append to existing:"):
                        append_table_name = selected_option.replace("Append to existing: ", "")
                        st.success(f"✅ Will append data to existing table: '{append_table_name}'")
                        
                        try:
                            with st.spinner("Fetching existing table metadata..."):
                                existing_metadata = get_table_metadata(conn, append_table_name)
                            if existing_metadata:
                                col1, col2, col3 = st.columns(3)
                                with col1:
                                    st.metric("Existing Rows", f"{existing_metadata['total_rows']:,}")
                                with col2:
                                    st.metric("Existing Columns", existing_metadata['total_columns'])
                                with col3:
                                    if existing_metadata['latest_timestamp']:
                                        st.metric("Latest Data", str(existing_metadata['latest_timestamp'])[:19])
                        except Exception as e:
                            st.warning(f"Could not fetch metadata: {str(e)}")
                        
                        table_name = append_table_name
                        append_mode = True
                    else:
                        append_mode = False

                    if selected_option: 
                        if st.button("Ingest Data", type="primary"):
                            if not table_name:
                                st.error("Please provide a table name")
                            elif not selected_columns:
                                st.error("Please select at least one column")
                            else:
                                start_time = time.time()
                                progress_container = st.container()
                                
                                with progress_container:
                                    st.write("### Data Ingestion Progress")
                                    
                                    with st.spinner("Preparing data subset and applying filters..."):
                                        final_df = df[selected_columns].head(row_limit).copy()
                                    
                                    if dtype_changes:
                                        conversion_progress = st.progress(0)
                                        conversion_status = st.empty()
                                        total_conversions = len(dtype_changes)
                                        
                                        for idx, (col, new_dtype) in enumerate(dtype_changes.items()):
                                            conversion_status.text(f"Converting column '{col}' to {new_dtype}...")
                                            final_df = convert_column_dtype(final_df, col, new_dtype)
                                            conversion_progress.progress((idx + 1) / total_conversions)
                                        
                                        conversion_status.text("✅ All data type conversions completed!")
                                    
                                    if append_mode:
                                        with st.spinner("🔍 Validating table compatibility..."):
                                            compatibility_issues, missing_cols, extra_cols, common_cols = validate_table_compatibility(conn, table_name, final_df)
                                        
                                        if compatibility_issues:
                                            st.warning("**Compatibility Issues Found:**")
                                            for issue in compatibility_issues:
                                                st.warning(f"• {issue}")
                                        
                                        if missing_cols:
                                            st.info(f"**Columns will be added with NULL values:** {list(missing_cols)}")
                                        
                                        if extra_cols:
                                            st.warning(f"**Columns will be ignored:** {list(extra_cols)}")
                                        
                                        if compatibility_issues and not st.checkbox("Proceed despite compatibility issues"):
                                            st.error("Cannot proceed with incompatible data. Please fix the issues or create a new table.")
                                            st.stop()
                                        
                                        with st.spinner("Preparing data for table append operation..."):
                                            prepared_df = prepare_dataframe_for_append(final_df, common_cols.union(missing_cols), missing_cols, extra_cols)
                                        
                                        st.write(" **Appending data to existing table...**")
                                        success = append_data_to_table(conn, prepared_df, table_name)
                                    else:
                                        file_info = {
                                            'file_name': file_name,
                                            'file_size': file_size,
                                            'original_shape': df.shape,
                                            'final_shape': final_df.shape,
                                            'selected_columns': selected_columns,
                                            'dtype_changes': dtype_changes,
                                        }
                                        
                                        if is_quarterly_file and has_accord_code:
                                            file_info['quarterly_column'] = quarterly_col if quarterly_col != "None" else None
                                        else:
                                            file_info['timestamp_column'] = timestamp_col if timestamp_col != "None" else None
                                        
                                        st.write("**Creating new table and ingesting data...**")
                                        success = ingest_data_to_duckdb(final_df, table_name, file_info, conn)
                                
                                if success:
                                    end_time = time.time()
                                    processing_time = end_time - start_time
                                    
                                    with st.spinner("Logging upload activity..."):
                                        user_info = get_current_user()
                                        
                                        log_entry = {
                                            'timestamp': datetime.now().isoformat(),
                                            'user': f"{user_info['first_name']} {user_info['last_name']}" if user_info else "Unknown",
                                            'file_name': file_name,
                                            'file_size_mb': file_size / (1024*1024),
                                            'table_name': table_name,
                                            'rows_ingested': len(final_df),
                                            'columns_ingested': len(selected_columns),
                                            'processing_time_seconds': processing_time,
                                            'dtype_conversions': dtype_changes,
                                            'is_quarterly_data': is_quarterly_file and has_accord_code,
                                            'append_mode': append_mode
                                        }
                                        
                                        if is_quarterly_file and has_accord_code:
                                            log_entry['quarterly_column'] = quarterly_col if quarterly_col != "None" else None
                                        else:
                                            log_entry['timestamp_column'] = timestamp_col if timestamp_col != "None" else None
                                        
                                        save_upload_log(log_entry)
                                    
                                    action_text = "appended to" if append_mode else "ingested into"
                                    st.success(f"✅ Data successfully {action_text} table '{table_name}'!")
                                    st.info(f"⏱️ Processing time: {processing_time:.2f} seconds")
                                    st.info(f"Processed {len(final_df):,} rows with {len(selected_columns)} columns")
                                    
                                    throughput = len(final_df) / processing_time if processing_time > 0 else 0
                                    st.info(f"Throughput: {throughput:,.0f} rows/second")
                                    
                                    st.markdown("---")
                                    st.subheader("Updated Table Preview")
                                    
                                    try:
                                        with st.spinner("Fetching updated table sample data..."):
                                            first_10, last_10 = get_table_sample_data(conn, table_name)
                                        
                                        tab1, tab2 = st.tabs(["First 10 Rows", "Last 10 Rows"])
                                        
                                        with tab1:
                                            if not first_10.empty:
                                                st.dataframe(first_10, use_container_width=True)
                                            else:
                                                st.info("No data to display")
                                        
                                        with tab2:
                                            if not last_10.empty:
                                                st.dataframe(last_10, use_container_width=True)
                                            else:
                                                st.info("No data to display")
                                        
                                        st.subheader("Download Table")
                                        
                                        col1, col2 = st.columns(2)
                                        
                                        with col1:
                                            if st.button("Download as CSV", type="secondary"):
                                                with st.spinner("Preparing CSV download..."):
                                                    table_data = download_table_data(conn, table_name)
                                                if table_data is not None:
                                                    csv = table_data.to_csv(index=False)
                                                    st.download_button(
                                                        label="Click to Download CSV",
                                                        data=csv,
                                                        file_name=f"{table_name}.csv",
                                                        mime="text/csv"
                                                    )
                                        
                                        with col2:
                                            if st.button("Download as Parquet", type="secondary"):
                                                with st.spinner("Preparing Parquet download..."):
                                                    table_data = download_table_data(conn, table_name)
                                                if table_data is not None:
                                                    parquet_buffer = table_data.to_parquet(index=False)
                                                    st.download_button(
                                                        label="Click to Download Parquet",
                                                        data=parquet_buffer,
                                                        file_name=f"{table_name}.parquet",
                                                        mime="application/octet-stream"
                                                    )
                                    
                                    except Exception as e:
                                        st.error(f"Error displaying table preview: {str(e)}")
                                    
                                    if 'df' in st.session_state:
                                        del st.session_state.df
                                    if 'file_name' in st.session_state:
                                        del st.session_state.file_name
                                    if 'file_size' in st.session_state: 
                                        del st.session_state.file_size
                                    if 'analysis_computed' in st.session_state:
                                        del st.session_state.analysis_computed
                                    if 'cached_analysis' in st.session_state:
                                        del st.session_state.cached_analysis