from truedata_ws.websocket.TD import TD
import requests
import logging
import datetime
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

td_login_id = os.getenv ("TRUEDATA_LOGIN_ID")
td_password = os.getenv("TRUEDATA_LOGIN_PWD")

td_obj = TD(td_login_id, td_password, live_port=None,
            log_level=logging.WARNING, log_format="%(message)s")