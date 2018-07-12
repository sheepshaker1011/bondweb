from flask import Flask
from config import config
from flask_sqlalchemy import SQLAlchemy

config_name = 'testing'
app = Flask(__name__)  
app.config.from_object(config[config_name])  
config[config_name].init_app(app)  
db = SQLAlchemy(app)
from .models import YaosuNex, Huizong, YaosuOthers
from bondweb import views