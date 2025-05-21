import re
import os
import streamlit as st

DEFAULT_PASSWORD = "qodeengine123"

def init_auth():
    session_vars = {
        'logged_in': False,
        'user_name': "",
        'user_id': "",
        'user_folder': "",
        'current_page': 'main'
    }
    
    for key, default_val in session_vars.items():
        if key not in st.session_state:
            st.session_state[key] = default_val

def validate_login(email, password):
    email_pattern = r'^([a-zA-Z]+)\.([a-zA-Z]+)@qodeinvest\.com$'
    match = re.match(email_pattern, email)
    
    if not match:
        return False, "Invalid email format", None, None, None
    
    first_name = match.group(1).capitalize()
    last_name = match.group(2).capitalize()
    
    if password != DEFAULT_PASSWORD:
        return False, "Incorrect password", None, None, None
    
    user_id = f"{first_name.lower()}_{last_name.lower()}"
    user_folder = f"query_history/{user_id}"
    os.makedirs(user_folder, exist_ok=True)
    
    return True, f"Welcome, {first_name} {last_name}!", f"{first_name} {last_name}", user_id, user_folder

def logout():
    st.session_state['logged_in'] = False
    st.session_state['user_name'] = ""
    st.session_state['user_id'] = ""
    st.session_state['user_folder'] = ""
    st.session_state['current_page'] = 'main'

def show_login():
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.image("./logo.jpg", width=100)
        
        st.markdown("## Qode Data Fetcher")
        
        with st.form("login_form"):
            email = st.text_input("Email", placeholder="firstname.lastname@qodeinvest.com")
            password = st.text_input("Password", type="password")
            
            cols = st.columns([3, 2, 3])
            with cols[1]:
                submit_button = st.form_submit_button("Login", use_container_width=True)
            
            if submit_button:
                success, message, user_name, user_id, user_folder = validate_login(email, password)
                if success:
                    st.session_state['logged_in'] = True
                    st.session_state['user_name'] = user_name
                    st.session_state['user_id'] = user_id
                    st.session_state['user_folder'] = user_folder
                    st.success(message)
                    st.rerun()
                else:
                    st.error(message)