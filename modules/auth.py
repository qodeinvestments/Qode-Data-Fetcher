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

def get_session_file_path(user_id):
    """Get unique session file path for each user"""
    sessions_dir = "sessions"
    os.makedirs(sessions_dir, exist_ok=True)
    return os.path.join(sessions_dir, f"session_{user_id}.json")

def get_browser_session_id():
    """Generate a unique session ID for this browser session"""
    if 'browser_session_id' not in st.session_state:
        # Create a unique ID based on timestamp and random component
        st.session_state.browser_session_id = hashlib.md5(
            f"{time.time()}{id(st.session_state)}".encode()
        ).hexdigest()
    return st.session_state.browser_session_id

def save_session_data(user_data):
    """Save session data to a user-specific file"""
    session_file = get_session_file_path(user_data['user_id'])
    browser_session_id = get_browser_session_id()
    
    session_data = {
        'user_name': user_data['user_name'],
        'user_id': user_data['user_id'],
        'user_folder': user_data['user_folder'],
        'timestamp': time.time(),
        'session_hash': hashlib.md5(f"{user_data['user_id']}{time.time()}".encode()).hexdigest(),
        'browser_session_id': browser_session_id
    }
    
    try:
        # Load existing sessions for this user
        existing_sessions = []
        if os.path.exists(session_file):
            try:
                with open(session_file, 'r') as f:
                    existing_data = json.load(f)
                    if isinstance(existing_data, list):
                        existing_sessions = existing_data
                    else:
                        # Convert old format to new format
                        existing_sessions = [existing_data] if existing_data else []
            except json.JSONDecodeError:
                existing_sessions = []
        
        # Remove expired sessions and sessions from the same browser
        current_time = time.time()
        valid_sessions = []
        for session in existing_sessions:
            if (current_time - session.get('timestamp', 0) < 86400 and 
                session.get('browser_session_id') != browser_session_id):
                valid_sessions.append(session)
        
        # Add new session
        valid_sessions.append(session_data)
        
        # Save updated sessions
        with open(session_file, 'w') as f:
            json.dump(valid_sessions, f, indent=2)
        
        return session_data['session_hash']
    except Exception as e:
        st.error(f"Error saving session: {e}")
        return None

def load_session_data():
    """Load session data for current browser session"""
    browser_session_id = get_browser_session_id()
    sessions_dir = "sessions"
    
    if not os.path.exists(sessions_dir):
        return None
    
    try:
        # Check all session files to find one matching this browser session
        for filename in os.listdir(sessions_dir):
            if filename.startswith("session_") and filename.endswith(".json"):
                session_file = os.path.join(sessions_dir, filename)
                
                try:
                    with open(session_file, 'r') as f:
                        data = json.load(f)
                    
                    # Handle both old format (single dict) and new format (list of sessions)
                    sessions = data if isinstance(data, list) else [data] if data else []
                    
                    current_time = time.time()
                    valid_sessions = []
                    found_session = None
                    
                    for session in sessions:
                        # Check if session is valid and not expired
                        if current_time - session.get('timestamp', 0) < 86400:
                            if session.get('browser_session_id') == browser_session_id:
                                found_session = session
                            valid_sessions.append(session)
                    
                    # Update the file with only valid sessions
                    if len(valid_sessions) != len(sessions):
                        if valid_sessions:
                            with open(session_file, 'w') as f:
                                json.dump(valid_sessions, f, indent=2)
                        else:
                            os.remove(session_file)
                    
                    if found_session:
                        return found_session
                        
                except (json.JSONDecodeError, KeyError):
                    # Remove corrupted session file
                    try:
                        os.remove(session_file)
                    except:
                        pass
                    continue
                    
    except Exception:
        pass
    
    return None

def cleanup_expired_sessions():
    """Clean up expired session files"""
    sessions_dir = "sessions"
    if not os.path.exists(sessions_dir):
        return
    
    try:
        current_time = time.time()
        for filename in os.listdir(sessions_dir):
            if filename.startswith("session_") and filename.endswith(".json"):
                session_file = os.path.join(sessions_dir, filename)
                
                try:
                    with open(session_file, 'r') as f:
                        data = json.load(f)
                    
                    sessions = data if isinstance(data, list) else [data] if data else []
                    valid_sessions = []
                    
                    for session in sessions:
                        if current_time - session.get('timestamp', 0) < 86400:
                            valid_sessions.append(session)
                    
                    if valid_sessions:
                        if len(valid_sessions) != len(sessions):
                            with open(session_file, 'w') as f:
                                json.dump(valid_sessions, f, indent=2)
                    else:
                        os.remove(session_file)
                        
                except (json.JSONDecodeError, KeyError):
                    try:
                        os.remove(session_file)
                    except:
                        pass
                        
    except Exception:
        pass

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

    # Clean up expired sessions periodically
    cleanup_expired_sessions()

    # Try to restore session only if not already logged in
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
    user_id = st.session_state.get('user_id', '')
    browser_session_id = get_browser_session_id()
    
    # Remove this browser's session from the user's session file
    if user_id:
        session_file = get_session_file_path(user_id)
        if os.path.exists(session_file):
            try:
                with open(session_file, 'r') as f:
                    data = json.load(f)
                
                sessions = data if isinstance(data, list) else [data] if data else []
                remaining_sessions = []
                
                for session in sessions:
                    if session.get('browser_session_id') != browser_session_id:
                        remaining_sessions.append(session)
                
                if remaining_sessions:
                    with open(session_file, 'w') as f:
                        json.dump(remaining_sessions, f, indent=2)
                else:
                    os.remove(session_file)
                    
            except Exception:
                pass
    
    # Clear session state
    st.session_state['logged_in'] = False
    st.session_state['user_name'] = ""
    st.session_state['user_id'] = ""
    st.session_state['user_folder'] = ""
    st.session_state['current_page'] = 'main'
    st.session_state['session_hash'] = ""

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