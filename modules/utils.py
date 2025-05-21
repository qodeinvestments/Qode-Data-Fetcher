import os
import psutil
import streamlit as st

class SystemMonitor:
    @staticmethod
    def show_metrics(db_conn):
        st.sidebar.markdown("---")
        st.sidebar.markdown("### System Metrics")
        
        try:
            process = psutil.Process(os.getpid())
            memory_usage = process.memory_info().rss / 1024 / 1024
            cpu_percent = process.cpu_percent(interval=0.1)
            
            col1, col2 = st.sidebar.columns(2)
            col1.metric("Memory (MB)", f"{memory_usage:.1f}")
            col2.metric("CPU (%)", f"{cpu_percent:.1f}")
            
            try:
                db_size = os.path.getsize("qode_engine_data.db") / 1024 / 1024
                col1, col2 = st.sidebar.columns(2)
                col1.metric("DB Size (MB)", f"{db_size:.1f}")
                
                active_objects = db_conn.execute("SELECT count(*) FROM pragma_database_size()").fetchone()[0]
                col2.metric("DB Objects", active_objects)
            except Exception:
                pass
                
        except Exception:
            st.sidebar.text("System metrics unavailable")
            
        st.sidebar.markdown("---")
        st.sidebar.markdown("© 2025 Qode Data Engine")