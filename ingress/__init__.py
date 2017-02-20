from flask import Flask
from .blueprint import BLUEPRINT

app = Flask(__name__)

app.config.from_envvar("INGRESS_CONFIG", silent=True)

app.register_blueprint(BLUEPRINT)
