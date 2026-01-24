# wsgi.py
import logging
import threading
from waitress import serve

from webserver.data_provider import DataProvider
from webserver.server import create_app

def run_webserver(data_provider: DataProvider, host="127.0.0.1", port=5000):
    app = create_app(data_provider)
    logging.info(f"Starting Dash web server at http://{host}:{port}/ using Waitress.")
    # Serve the underlying Flask server of the Dash app
    t = threading.Thread(
        target=serve,
        args=(app.server,),
        kwargs={"host": host, "port": port},
        daemon=True,
    )
    t.start()
    return t
