import streamlit as st
import json
from typing import Dict, Optional

with open('users.json', 'r') as f:
      USERS = json.load(f)

def verify_credentials(email: str, password: str) -> Optional[Dict]:
    """
    Verify user credentials against the stored user database.
    
    Args:
        email: User email address
        password: User password
        
    Returns:
        User information dict if credentials are valid, None otherwise
    """
    if email in USERS and USERS[email]["password"] == password:
        return USERS[email]
    return None

def is_authenticated() -> bool:
    """
    Check if the current session is authenticated.
    
    Returns:
        True if user is authenticated, False otherwise
    """
    return st.session_state.get('authenticated', False)

def get_current_user() -> Optional[Dict]:
    """
    Get the current authenticated user's information.
    
    Returns:
        User information dict if authenticated, None otherwise
    """
    if is_authenticated():
        return st.session_state.get('user_info')
    return None

def logout():
    """
    Log out the current user by clearing session state.
    """
    for key in ['authenticated', 'user_info', 'email']:
        if key in st.session_state:
            del st.session_state[key]

def login_form():
    """
    Display the login form and handle authentication.
    
    Returns:
        True if authentication is successful, False otherwise
    """
    st.title("Qode Data Fetcher Login")
    st.markdown("---")
    
    with st.form("login_form"):
        st.subheader("Please enter your credentials")
        
        email = st.text_input(
            "Email Address",
            placeholder="your.email@qodeinvest.com",
            help="Enter your Qode Invest email address"
        )
        
        password = st.text_input(
            "Password",
            type="password",
            placeholder="Enter your password",
            help="Enter your password"
        )
        
        submitted = st.form_submit_button("Login", use_container_width=True)
        
        if submitted:
            if not email or not password:
                st.error("Please enter both email and password.")
                return False
            
            user_info = verify_credentials(email, password)
            
            if user_info:
                st.session_state['authenticated'] = True
                st.session_state['user_info'] = user_info
                st.session_state['email'] = email
                
                st.success(f"Welcome, {user_info['first_name']} {user_info['last_name']}!")
                st.rerun()
            else:
                st.error("Invalid email or password. Please try again.")
                return False
    
    return False

def require_authentication():
    """
    Decorator-like function to require authentication before accessing the main app.
    Should be called at the beginning of your main app function.
    
    Returns:
        True if user is authenticated and app should proceed, False otherwise
    """
    if not is_authenticated():
        login_form()
        return False
    return True

def show_user_info_sidebar():
    """
    Display user information and logout option in the sidebar.
    """
    if is_authenticated():
        user_info = get_current_user()
        if user_info:
            with st.sidebar:
                st.markdown("---")
                st.write(f"Logged in as {user_info['first_name']} {user_info['last_name']}")
                
                if st.button("Logout", use_container_width=True):
                    logout()
                    st.rerun()