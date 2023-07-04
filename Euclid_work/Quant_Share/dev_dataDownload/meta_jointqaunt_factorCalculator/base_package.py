from gm.api import *
import os

current_dir = os.path.abspath(os.path.dirname(__file__))
saved_folder = os.path.join(current_dir, 'prepared_data')

with open(os.path.join(current_dir, 'token.txt'), 'rt', encoding='utf-8') as f:
    token = f.read().strip()
set_token(token)
