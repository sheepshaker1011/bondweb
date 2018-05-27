from flask import Flask, render_template, request, redirect, g, jsonify
import sqlite3
import flask_excel as excel
import pyexcel_xlsx
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, mapper
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy import or_
import pymysql


bond = Flask(__name__)
engine = create_engine("mysql+pymysql://root:f1c2y319931011@127.0.0.1:3306/bond?charset=utf8")
Base = declarative_base()
Base.metadata.reflect(engine)

class TradeHistory(Base):
    __table__ = Base.metadata.tables['TradeHistory']

@bond.route('/', methods = ['GET', 'POST'])
def search():
    return render_template("index.html")

@bond.route('/bondsearch',methods = ['GET', 'POST'])
def search_bond():
    bond_name = request.form['bondname']
    Session_class = sessionmaker(bind=engine)
    session = Session_class()  
    res = session.query(TradeHistory).filter(or_(TradeHistory.bond_name.like("%"+bond_name+"%"),TradeHistory.bond_code.like("%"+bond_name+"%"))).all()
    session.close()
    return render_template("bondsearch.html", res=res)


if __name__ == '__main__':
    excel.init_excel(bond)
    bond.run(host='0.0.0.0', port='8080', debug=True)



