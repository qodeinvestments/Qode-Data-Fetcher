import streamlit as st

from msal_streamlit_authentication import msal_authentication


value = msal_authentication(
    auth={
        "clientId": "483288cb-2c40-4e8c-8077-761502a77d29",
        "authority": "https://login.microsoftonline.com/34e64400-bcc0-4bd1-9a1a-d74ae51cf680",
        "redirectUri": "/",
        "postLogoutRedirectUri": "/"
    },
    cache={
        "cacheLocation": "sessionStorage",
        "storeAuthStateInCookie": False
    },
    login_request={
        "scopes": ["34e64400-bcc0-4bd1-9a1a-d74ae51cf680/.default"]
    },
    key=1)
st.write("Received", value)