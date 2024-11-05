# gunicorn_config.py

bind = '0.0.0.0:8000'  # Bind to localhost on port 8000
workers = 3             # Number of worker processes
accesslog = '-'         # Log access to stdout
errorlog = '-'          # Log errors to stdout
loglevel = 'debug'       # Set log level
