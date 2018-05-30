from flask import Flask, render_template, request, redirect, g, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, mapper
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy import or_
import pymysql


bond = Flask(__name__)
engine = create_engine("mysql+pymysql://root:f1c2y319931011@106.15.109.103:3306/bond?charset=utf8mb4")
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

@bond.route('/compsearch',methods = ['GET', 'POST'])
def search_comp():
    comp_name = request.form['compname']
    Session_class = sessionmaker(bind=engine)
    session = Session_class()  
    res = session.query(TradeHistory).filter(or_(TradeHistory.bid.like("%"+comp_name+"%"),TradeHistory.offer.like("%"+comp_name+"%"))).all()
    session.close()
    return render_template("compsearch.html", res=res)

if __name__ == '__main__':
    bond.run()



