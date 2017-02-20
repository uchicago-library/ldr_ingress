from flask import Flask
from .blueprint import BLUEPRINT

app = Flask(__name__)

app.config['PREMIS_ENDPOINT'] = "http://127.0.0.1:8910/"
app.config['MATERIALSUITE_ENDPOINT'] = "http://127.0.0.1:8911/add"
app.config['ACCS_ENDPOINT'] = "http://127.0.0.1:8912/"
app.config.from_envvar("INGRESS_SETTINGS", silent=True)

app.register_blueprint(BLUEPRINT)
