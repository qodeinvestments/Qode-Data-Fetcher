import os
import pandas as pd
from typing import Tuple, Optional, List, Dict
import streamlit as st
import numpy as np
import plotly.express as px
import plotly.graph_objects as go


def check_file_exists(file_path: str) -> bool:
    return os.path.exists(file_path) and os.path.isfile(file_path)

def get_file_size(file_path: str) -> int:
    return os.path.getsize(file_path)

def load_data_file_with_header(file_path: str = None, uploaded_file = None, header_row: int = 0) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    try:
        if uploaded_file:
            file_extension = uploaded_file.name.split('.')[-1].lower()
            file_name = uploaded_file.name
        elif file_path:
            file_extension = file_path.split('.')[-1].lower()
            file_name = os.path.basename(file_path)
        else:
            raise ValueError("No file provided")
        
        read_params = {'header': header_row}
        
        if uploaded_file:
            if file_extension == 'csv':
                df = pd.read_csv(uploaded_file, **read_params, low_memory=False)
            elif file_extension in ['xlsx', 'xls']:
                df = pd.read_excel(uploaded_file, **read_params, engine='openpyxl' if file_extension == 'xlsx' else 'xlrd')
            elif file_extension == 'parquet':
                df = pd.read_parquet(uploaded_file, engine='pyarrow')
            elif file_extension == 'pkl':
                df = pd.read_pickle(uploaded_file)
            elif file_extension == 'gz':
                df = pd.read_csv(uploaded_file, compression='gzip', **read_params, low_memory=False)
            else:
                raise ValueError(f"Unsupported file format: {file_extension}")
        else:
            if file_extension == 'csv':
                df = pd.read_csv(file_path, **read_params, low_memory=False)
            elif file_extension in ['xlsx', 'xls']:
                df = pd.read_excel(file_path, **read_params, engine='openpyxl' if file_extension == 'xlsx' else 'xlrd')
            elif file_extension == 'parquet':
                df = pd.read_parquet(file_path, engine='pyarrow')
            elif file_extension == 'pkl':
                df = pd.read_pickle(file_path)
            elif file_extension == 'gz':
                df = pd.read_csv(file_path, compression='gzip', **read_params, low_memory=False)
            else:
                raise ValueError(f"Unsupported file format: {file_extension}")
        
        return df, file_name
        
    except Exception as e:
        st.error(f"Error loading file: {str(e)}")
        return None, None

def load_data_preview(file_path: str = None, uploaded_file = None, max_rows: int = 20) -> Optional[pd.DataFrame]:
    try:
        if uploaded_file:
            uploaded_file.seek(0)
            file_extension = uploaded_file.name.split('.')[-1].lower()
        elif file_path:
            file_extension = file_path.split('.')[-1].lower()
        else:
            return None
        
        read_params = {'header': None, 'nrows': max_rows}
        
        if uploaded_file:
            if file_extension == 'csv':
                df = pd.read_csv(uploaded_file, **read_params, low_memory=False)
            elif file_extension in ['xlsx', 'xls']:
                df = pd.read_excel(uploaded_file, **read_params, engine='openpyxl' if file_extension == 'xlsx' else 'xlrd')
            elif file_extension == 'gz':
                df = pd.read_csv(uploaded_file, compression='gzip', **read_params, low_memory=False)
            else:
                return None
            uploaded_file.seek(0)
        else:
            if file_extension == 'csv':
                df = pd.read_csv(file_path, **read_params, low_memory=False)
            elif file_extension in ['xlsx', 'xls']:
                df = pd.read_excel(file_path, **read_params, engine='openpyxl' if file_extension == 'xlsx' else 'xlrd')
            elif file_extension == 'gz':
                df = pd.read_csv(file_path, compression='gzip', **read_params, low_memory=False)
            else:
                return None
        
        return df
        
    except Exception as e:
        st.error(f"Error loading preview: {str(e)}")
        return None
    
def analyze_dataframe(df: pd.DataFrame) -> Dict:
    numeric_columns = df.select_dtypes(include=[np.number]).columns.tolist()
    
    analysis = {
        'shape': df.shape,
        'columns': df.columns.tolist(),
        'dtypes': dict(zip(df.columns, df.dtypes.astype(str))),
        'memory_usage': df.memory_usage(deep=True).sum(),
        'null_counts': df.isnull().sum().to_dict(),
        'numeric_stats': {},
        'duplicate_rows': df.duplicated().sum(),
        'numeric_columns': numeric_columns
    }
    
    if numeric_columns:
        numeric_df = df[numeric_columns]
        stats = numeric_df.describe().to_dict()
        for col in numeric_columns:
            col_data = numeric_df[col].dropna()
            if len(col_data) > 0:
                analysis['numeric_stats'][col] = {
                    'min': stats[col]['min'],
                    'max': stats[col]['max'],
                    'mean': stats[col]['mean'],
                    'std': stats[col]['std'],
                    'median': stats[col]['50%'],
                    'q25': stats[col]['25%'],
                    'q75': stats[col]['75%']
                }
    
    return analysis

def get_column_statistics(df: pd.DataFrame, column: str) -> Dict:
    col_series = df[column]
    col_data = col_series.dropna()
    
    base_stats = {
        'total_count': len(col_series),
        'non_null_count': len(col_data),
        'null_count': col_series.isnull().sum(),
    }
    
    if col_series.dtype in ['object', 'string']:
        value_counts = col_series.value_counts()
        stats = {
            **base_stats,
            'type': 'categorical',
            'unique_count': col_series.nunique(),
            'most_frequent': value_counts.index[0] if len(value_counts) > 0 else 'N/A',
            'most_frequent_count': value_counts.iloc[0] if len(value_counts) > 0 else 0,
            'top_values': value_counts.head(10).to_dict()
        }
    else:
        if len(col_data) > 0:
            describe_stats = col_data.describe()
            stats = {
                **base_stats,
                'type': 'numeric',
                'min': describe_stats['min'],
                'max': describe_stats['max'],
                'mean': describe_stats['mean'],
                'median': describe_stats['50%'],
                'std': describe_stats['std'],
                'variance': col_data.var(),
                'q25': describe_stats['25%'],
                'q75': describe_stats['75%'],
                'iqr': describe_stats['75%'] - describe_stats['25%'],
                'skewness': col_data.skew(),
                'kurtosis': col_data.kurtosis()
            }
        else:
            stats = {**base_stats, 'type': 'numeric'}
    
    return stats

def detect_quarterly_columns(df: pd.DataFrame, file_name: str) -> List[str]:
    if 'quarterly' not in file_name.lower():
        return []
    
    quarterly_candidates = []
    columns_lower = {col: col.lower() for col in df.columns}
    
    for col, col_lower in columns_lower.items():
        if any(term in col_lower for term in ['quarter', 'q1', 'q2', 'q3', 'q4', 'qtr', 'period']):
            quarterly_candidates.append(col)
        elif 'date' in col_lower or 'time' in col_lower:
            try:
                sample = df[col].dropna().head(10)
                dates = pd.to_datetime(sample, errors='coerce')
                if not dates.isna().all():
                    quarterly_candidates.append(col)
            except:
                pass
    
    return quarterly_candidates

def detect_timestamp_columns(df: pd.DataFrame) -> List[str]:
    timestamp_candidates = []
    for col in df.columns:
        col_lower = col.lower()
        if 'time' in col_lower or 'date' in col_lower:
            timestamp_candidates.append(col)
        elif df[col].dtype == 'object':
            try:
                sample = df[col].dropna().head(100)
                pd.to_datetime(sample)
                timestamp_candidates.append(col)
            except:
                pass
    return timestamp_candidates

def detect_accord_code_columns(df: pd.DataFrame) -> List[str]:
    accord_candidates = []
    for col in df.columns:
        col_lower = col.lower()
        if 'accord' in col_lower and 'code' in col_lower:
            accord_candidates.append(col)
    return accord_candidates

def has_accord_code_columns(df: pd.DataFrame) -> bool:
    return len(detect_accord_code_columns(df)) > 0

def convert_column_dtype(df: pd.DataFrame, column: str, target_dtype: str) -> pd.DataFrame:
    df_copy = df.copy()
    try:
        if target_dtype == 'datetime':
            df_copy[column] = pd.to_datetime(df_copy[column], errors='coerce')
        elif target_dtype == 'float32':
            df_copy[column] = pd.to_numeric(df_copy[column], errors='coerce', downcast='float')
        elif target_dtype == 'float64':
            df_copy[column] = pd.to_numeric(df_copy[column], errors='coerce')
        elif target_dtype == 'int32':
            df_copy[column] = pd.to_numeric(df_copy[column], errors='coerce', downcast='integer')
        elif target_dtype == 'int64':
            df_copy[column] = pd.to_numeric(df_copy[column], errors='coerce')
        elif target_dtype == 'string':
            df_copy[column] = df_copy[column].astype('string')
        return df_copy
    except Exception as e:
        import streamlit as st
        st.error(f"Error converting column {column} to {target_dtype}: {str(e)}")
        return df
    
def parse_financial_quarter(period_value):
    try:
        if pd.isna(period_value):
            return "Unknown", "000000"
        
        period_str = str(int(float(period_value)))
        if len(period_str) == 6:
            year = int(period_str[:4])
            month = int(period_str[4:])
            
            quarter_map = {3: ("Q4", year - 1), 6: ("Q1", year), 9: ("Q2", year), 12: ("Q3", year)}
            
            if month in quarter_map:
                quarter, fy_start = quarter_map[month]
            else:
                if month <= 3:
                    quarter, fy_start = "Q4", year - 1
                elif month <= 6:
                    quarter, fy_start = "Q1", year
                elif month <= 9:
                    quarter, fy_start = "Q2", year
                else:
                    quarter, fy_start = "Q3", year
            
            fy_end = fy_start + 1
            return f"FY{fy_start}-{str(fy_end)[2:]} {quarter}", f"{year}{month:02d}"
        else:
            return str(period_value), str(period_value)
    except Exception:
        return f"Invalid_{period_value}", "999999"

def create_quarterly_visualization(df: pd.DataFrame, quarterly_col: str, accord_code_cols: List[str]) -> None:
    if not quarterly_col or quarterly_col == "None" or not accord_code_cols:
        return
    
    try:
        st.subheader("Quarterly Analysis - Unique Companies by Accord Codes")
        
        df_viz = df.copy()
        
        def parse_financial_quarter(period_value):
            try:
                if pd.isna(period_value):
                    return "Unknown", "000000"
                
                period_str = str(int(float(period_value)))
                if len(period_str) == 6:
                    year = int(period_str[:4])
                    month = int(period_str[4:])
                    
                    if month == 3:
                        fy_start = year - 1
                        quarter = "Q4"
                    elif month == 6:
                        fy_start = year
                        quarter = "Q1"
                    elif month == 9:
                        fy_start = year
                        quarter = "Q2"
                    elif month == 12:
                        fy_start = year
                        quarter = "Q3"
                    else:
                        if month <= 3:
                            fy_start = year - 1
                            quarter = "Q4"
                        elif month <= 6:
                            fy_start = year
                            quarter = "Q1"
                        elif month <= 9:
                            fy_start = year
                            quarter = "Q2"
                        else:
                            fy_start = year
                            quarter = "Q3"
                    
                    fy_end = fy_start + 1
                    return f"FY{fy_start}-{str(fy_end)[2:]} {quarter}", f"{year}{month:02d}"
                else:
                    return str(period_value), str(period_value)
            except Exception as e:
                return f"Invalid_{period_value}", "999999"
        
        df_viz = df_viz.dropna(subset=[quarterly_col])
        
        if len(df_viz) == 0:
            st.warning("No valid quarterly data found after removing NaN values.")
            return
        
        df_viz[['Quarter_Label', 'Sort_Key']] = df_viz[quarterly_col].apply(
            lambda x: pd.Series(parse_financial_quarter(x))
        )
        
        df_viz = df_viz.sort_values('Sort_Key')
        
        quarter_order_df = df_viz[['Quarter_Label', 'Sort_Key']].drop_duplicates().sort_values('Sort_Key')
        quarter_order = quarter_order_df['Quarter_Label'].unique().tolist()
        
        st.write("**Chronological Analysis Across All Accord Code Columns:**")
        
        all_quarterly_stats = []
        
        for accord_col in accord_code_cols:
            quarterly_stats = df_viz.groupby('Quarter_Label')[accord_col].agg([
                'nunique',
                'count'
            ]).reset_index()
            quarterly_stats.columns = ['Quarter', 'Unique_Companies', 'Total_Records']
            quarterly_stats['Accord_Code_Column'] = accord_col
            quarterly_stats['Coverage_Ratio'] = (quarterly_stats['Unique_Companies'] / quarterly_stats['Total_Records'] * 100).round(2)
            all_quarterly_stats.append(quarterly_stats)
        
        combined_stats = pd.concat(all_quarterly_stats, ignore_index=True)
        
        if len(quarter_order) > 0:
            combined_stats['Quarter'] = pd.Categorical(
                combined_stats['Quarter'], 
                categories=quarter_order, 
                ordered=True
            )
            combined_stats = combined_stats.sort_values(['Quarter', 'Accord_Code_Column'])
        else:
            st.warning("No valid quarters found for categorical ordering.")
            combined_stats = combined_stats.sort_values(['Quarter', 'Accord_Code_Column'])
        
        fig_main = go.Figure()
        
        colors = px.colors.qualitative.Set1[:len(accord_code_cols)]
        
        for i, accord_col in enumerate(accord_code_cols):
            col_data = combined_stats[combined_stats['Accord_Code_Column'] == accord_col]
            
            fig_main.add_trace(go.Scatter(
                x=col_data['Quarter'],
                y=col_data['Unique_Companies'],
                mode='lines+markers',
                name=f'{accord_col}',
                line=dict(color=colors[i], width=3),
                marker=dict(size=8, color=colors[i]),
                hovertemplate='<b>%{fullData.name}</b><br>' +
                             'Quarter: %{x}<br>' +
                             'Unique Companies: %{y}<br>' +
                             '<extra></extra>',
                customdata=col_data[['Total_Records', 'Coverage_Ratio']].values
            ))
        
        fig_main.update_layout(
            title={
                'text': 'Chronological Trend: Unique Companies by Quarter (Financial Year)',
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 16}
            },
            xaxis_title='Financial Year Quarter',
            yaxis_title='Number of Unique Companies',
            height=500,
            hovermode='x unified',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            xaxis=dict(tickangle=45)
        )
        
        st.plotly_chart(fig_main, use_container_width=True)
        
        with st.expander("📖 Financial Year Quarter Guide", expanded=False):
            st.markdown("""
            **Financial Year Quarter Mapping:**
            - **Q1 (June)**: First quarter of financial year (April-June)
            - **Q2 (September)**: Second quarter of financial year (July-September)  
            - **Q3 (December)**: Third quarter of financial year (October-December)
            - **Q4 (March)**: Fourth quarter of financial year (January-March)
            
            **Example**: 201503 → FY2014-15 Q4 (March 2015, covering Jan-Mar 2015)
            
            **Data Format**: YYYYMM where YYYY is calendar year and MM is the month (03, 06, 09, 12)
            """)
    
    except Exception as e:
        st.error(f"Error creating quarterly visualization: {str(e)}")
        st.error(f"Debug info: Quarter column type: {df[quarterly_col].dtype}, Sample values: {df[quarterly_col].head().tolist()}")