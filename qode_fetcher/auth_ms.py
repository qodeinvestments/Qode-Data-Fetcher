import streamlit as st
import os
from streamlit import session_state as ss
from streamlit_msal import Msal
from dotenv import load_dotenv

load_dotenv()

client_id = os.getenv("AZURE_CLIENT_ID")
tenant_id = os.getenv("AZURE_TENANT_ID")

def msal_auth_flow():
    if st.session_state.get('logout_requested', False):
        del st.session_state['logout_requested']
        if 'msal_auth_main' in st.session_state:
            del st.session_state['msal_auth_main']
    
    if 'msal_auth_main' not in st.session_state:
        st.session_state['msal_auth_main'] = None
    
    auth_data = Msal.initialize(
        client_id=f"{client_id}",
        authority=f"https://login.microsoftonline.com/{tenant_id}",
        scopes=["User.Read"],
        key="msal_auth_main"
    )
    
    if auth_data and auth_data.get("account"):
        return auth_data
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
        <div style="text-align: center; padding: 2rem;">
            <h2>🔐 Authentication Required</h2>
            <p>Please sign in with your Microsoft account to continue.</p>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("Sign In with Microsoft", 
                    use_container_width=True, 
                    type="primary",
                    help="Click to authenticate with your Microsoft account"):
            try:
                result = Msal.sign_in()
                if result and result.get("account"):
                    account = result["account"]
                    name = account.get("name", "User")
                    username = account.get("username", "")
                    
                    st.session_state['authenticated'] = True
                    st.session_state['email'] = username
                    st.session_state['user_info'] = {
                        'first_name': name.split()[0] if name else "User",
                        'last_name': ' '.join(name.split()[1:]) if name and len(name.split()) > 1 else "",
                        'email': username
                    }
                    
                    st.success("Authentication successful!")
                    st.rerun()
                else:
                    st.error("Authentication failed: No account data received")
            except Exception as e:
                st.error(f"Authentication failed: {str(e)}")
        
        if auth_data and auth_data.get("account"):
            st.markdown("---")
            
            if st.button("🔄 Refresh Token", 
                        use_container_width=True,
                        help="Refresh your authentication token"):
                try:
                    Msal.revalidate()
                    st.success("Token refreshed successfully!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Token refresh failed: {str(e)}")
            
            if st.button("🚪 Sign Out", 
                        use_container_width=True,
                        type="secondary",
                        help="Sign out from your Microsoft account"):
                try:
                    Msal.sign_out()
                    logout()
                    st.success("Signed out successfully!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Sign out failed: {str(e)}")
    
    return auth_data

def is_authenticated() -> bool:
    if st.session_state.get('logout_requested', False):
        return False
    
    session_authenticated = st.session_state.get('authenticated', False)
    msal_authenticated = 'msal_auth_main' in st.session_state and st.session_state['msal_auth_main'] is not None
    
    if not session_authenticated or not msal_authenticated:
        return False
    
    return True

def get_current_user():
    if is_authenticated():
        return st.session_state.get('user_info')
    return None

def logout():
    try:
        Msal.sign_out()
    except Exception as e:
        st.error(f"Error signing out from Microsoft: {str(e)}")
    
    auth_keys = ['authenticated', 'user_info', 'email', 'msal_auth_main']
    for key in auth_keys:
        if key in st.session_state:
            del st.session_state[key]
    
    st.session_state['logout_requested'] = True

def require_authentication():
    if not is_authenticated():
        auth_data = msal_auth_flow()
        if auth_data and auth_data.get("account"):
            account = auth_data["account"]
            name = account.get("name", "User")
            username = account.get("username", "")
            
            st.session_state['authenticated'] = True
            st.session_state['email'] = username
            st.session_state['user_info'] = {
                'first_name': name.split()[0] if name else "User",
                'last_name': ' '.join(name.split()[1:]) if name and len(name.split()) > 1 else "",
                'email': username
            }
            
            st.success(f"Welcome, {name}!")
            st.rerun()
        else:
            pass
        return False
    return True

def show_user_info_sidebar():
    if is_authenticated():
        user_info = get_current_user()
        if user_info:
            with st.sidebar:
                st.markdown("---")
                st.write(f"**Name:** {user_info['first_name']} {user_info['last_name']}")
                st.write(f"**Email:** {user_info['email']}")
                
                if st.button("🚪 Logout", 
                           use_container_width=True, 
                           type="secondary",
                           help="Sign out from your account"):
                    logout()
                    st.success("Logged out successfully!")
                    st.rerun()