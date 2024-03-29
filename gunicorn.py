import os
from env_config import Config


workers = int(os.environ.get('GUNICORN_PROCESSES', '2'))

threads = int(os.environ.get('GUNICORN_THREADS', '4'))

# timeout = int(os.environ.get('GUNICORN_TIMEOUT', '120'))

bind = os.environ.get('GUNICORN_BIND', f"0.0.0.0:{Config().PORT}")


forwarded_allow_ips = '*'

secure_scheme_headers = {'X-Forwarded-Proto': 'https'}
