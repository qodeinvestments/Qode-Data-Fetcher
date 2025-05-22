import re
import os
import json
import streamlit as st
import hashlib
import time

def load_users():
    """Load users from users.json file"""
    try:
        with open('users.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        st.error("users.json file not found. Please ensure the file exists in the app directory.")
        return {}
    except json.JSONDecodeError:
        st.error("Error reading users.json file. Please check the file format.")
        return {}

def save_session_data(user_data):
    """Save session data to a temporary file"""
    session_file = "temp_session.json"
    session_data = {
        'user_name': user_data['user_name'],
        'user_id': user_data['user_id'],
        'user_folder': user_data['user_folder'],
        'timestamp': time.time(),
        'session_hash': hashlib.md5(f"{user_data['user_id']}{time.time()}".encode()).hexdigest()
    }
    
    try:
        with open(session_file, 'w') as f:
            json.dump(session_data, f)
        return session_data['session_hash']
    except Exception as e:
        st.error(f"Error saving session: {e}")
        return None

def load_session_data():
    """Load session data from temporary file"""
    session_file = "temp_session.json"
    try:
        if os.path.exists(session_file):
            with open(session_file, 'r') as f:
                data = json.load(f)
            
            if time.time() - data.get('timestamp', 0) < 86400:
                return data
            else:
                os.remove(session_file)
    except Exception:
        pass
    return None

def init_auth():
    """Initialize authentication state"""
    session_vars = {
        'logged_in': False,
        'user_name': "",
        'user_id': "",
        'user_folder': "",
        'current_page': 'main',
        'session_hash': ""
    }
    
    for key, default_val in session_vars.items():
        if key not in st.session_state:
            st.session_state[key] = default_val

    if not st.session_state.get('logged_in', False):
        saved_session = load_session_data()
        if saved_session:
            st.session_state['logged_in'] = True
            st.session_state['user_name'] = saved_session['user_name']
            st.session_state['user_id'] = saved_session['user_id']
            st.session_state['user_folder'] = saved_session['user_folder']
            st.session_state['session_hash'] = saved_session['session_hash']

def validate_login(email, password):
    users = load_users()
    
    email_pattern = r'^([a-zA-Z]+)\.([a-zA-Z]+)@qodeinvest\.com$'
    match = re.match(email_pattern, email.lower())
    
    if not match:
        return False, "Invalid email format", None, None, None
    
    if email.lower() not in users:
        return False, "User not found", None, None, None
    
    user_data = users[email.lower()]
    if password != user_data['password']:
        return False, "Incorrect password", None, None, None
    
    first_name = user_data['first_name']
    last_name = user_data['last_name']
    
    user_id = f"{first_name.lower()}_{last_name.lower()}"
    user_folder = f"query_history/{user_id}"
    os.makedirs(user_folder, exist_ok=True)
    
    return True, f"Welcome, {first_name} {last_name}!", f"{first_name} {last_name}", user_id, user_folder

def logout():
    """Logout user and clear session data"""
    st.session_state['logged_in'] = False
    st.session_state['user_name'] = ""
    st.session_state['user_id'] = ""
    st.session_state['user_folder'] = ""
    st.session_state['current_page'] = 'main'
    st.session_state['session_hash'] = ""
    
    session_file = "temp_session.json"
    if os.path.exists(session_file):
        try:
            os.remove(session_file)
        except Exception:
            pass

def show_login():
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.image("./logo.jpg", width=100)
        
        st.markdown("## Qode Data Fetcher")
        
        with st.form("login_form"):
            email = st.text_input("Email", placeholder="firstname.lastname@qodeinvest.com")
            password = st.text_input("Password", type="password")
            remember_me = st.checkbox("Remember me", value=True)
            
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
                    
                    if remember_me:
                        session_hash = save_session_data({
                            'user_name': user_name,
                            'user_id': user_id,
                            'user_folder': user_folder
                        })
                        st.session_state['session_hash'] = session_hash or ""
                    
                    st.success(message)
                    st.rerun()
                else:
                    st.error(message)