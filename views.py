#!/usr/bin/python
# -*- coding: utf-8 -*-

from flask import Flask, render_template, request, redirect, g, jsonify, flash, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import or_, func, select
from sqlalchemy.orm import sessionmaker
import pymysql
import os, io
import pandas as pd
from bondweb import app
from bondweb import db
from .models import YaosuNex, Huizong, YaosuOthers
#import sys
#reload(sys)
#sys.setdefaultencoding('utf8')


Session = sessionmaker(db.engine)

@app.route('/', methods = ['GET', 'POST'])
def search():
    return render_template("index.html")

@app.route('/bondsearch',methods = ['GET', 'POST'])
def search_bond():
    bond_name = request.form['bondname']
    res1 = db.session.query(YaosuNex).filter(or_(YaosuNex.bond_name.like("%"+bond_name+"%"), YaosuNex.bond_code.like("%"+bond_name+"%")))
    res2 = db.session.query(YaosuOthers).filter(or_(YaosuOthers.bond_name.like("%"+bond_name+"%"), YaosuOthers.bond_code.like("%"+bond_name+"%")))
    res3 = db.session.query(Huizong).filter(or_(Huizong.bond_name.like("%"+bond_name+"%"),Huizong.bond_code.like("%"+bond_name+"%")))
    res = res1.union(res2, res3).all()
    return render_template("bondsearch.html", res=res)

@app.route('/compsearch',methods = ['GET', 'POST'])
def search_comp():
    comp_name = request.form['compname']
    res1 = db.session.query(YaosuNex).filter(or_(YaosuNex.bid.like("%"+comp_name+"%"),YaosuNex.offer.like("%"+comp_name+"%")))
    res2 = db.session.query(YaosuOthers).filter(or_(YaosuOthers.bid.like("%"+comp_name+"%"),YaosuOthers.offer.like("%"+comp_name+"%")))
    res3 = db.session.query(Huizong).filter(or_(Huizong.bid.like("%"+comp_name+"%"),Huizong.offer.like("%"+comp_name+"%")))
    res = res1.union(res2, res3).all()
    return render_template("compsearch.html", res=res)

########## ETL ##########

# for different companys' data formats, return uni-formated data with trade_date appended
def dateAppended1(comp, df_hz):
    compdf = pd.DataFrame(columns=['trade_date', 'rem_period', 'bond_code', 'bond_name', 'bond_rating', 'bond_yield', 'note'])
    comp_today = ''
    for i in range(0,2):
        if isinstance(df_hz[comp][i], float): df_hz[comp][i] = ''
        comp_today += df_hz[comp][i]
    if not (comp_today == ''): 
        df_comp_today = pd.read_csv(io.StringIO(comp_today), header=None, 
                 names=['rem_period', 'bond_code', 'bond_name', 'bond_rating', 'bond_yield'], 
                 delimiter=r"\s+", dtype={'bond_code':str})
        compdf = pd.concat([compdf,df_comp_today])
    return compdf
    
def dateAppended2(comp, df_hz):
    compdf = pd.DataFrame(columns=['trade_date', 'rem_period', 'bond_code', 'bond_name', 'bond_rating', 'bond_yield', 'note'])
    comp_today = ''
    for i in range(0,2):
        if isinstance(df_hz[comp][i], float): df_hz[comp][i] = ''
        comp_today += df_hz[comp][i]
    if not (comp_today == ''):
        df_comp_today = pd.read_csv(io.StringIO(comp_today), header=None, 
                 names=['rem_period', 'bond_name', 'bond_code', 'bond_rating', 'bond_yield'], 
                 delimiter=r"\s+", dtype={'bond_code':str})
        compdf = pd.concat([compdf,df_comp_today])
    return compdf

def dateAppended3(comp, df_hz):
    compdf = pd.DataFrame(columns=['trade_date', 'rem_period', 'bond_code', 'bond_name', 'bond_rating', 'bond_yield', 'note'])
    comp_today = ''
    for i in range(0,2):
        if isinstance(df_hz[comp][i], float): df_hz[comp][i] = ''
        comp_today += df_hz[comp][i]
    if not (comp_today == ''): 
        df_comp_today = pd.read_csv(io.StringIO(comp_today), header=None, 
                 names=['rem_period', 'bond_name', 'bond_code', 'bond_yield', 'bond_rating'], 
                 delimiter=r"\s+", dtype={'bond_code':str})
        compdf = pd.concat([compdf,df_comp_today])
    return compdf

def dateAppended4(comp, df_hz):
    compdf = pd.DataFrame(columns=['trade_date', 'rem_period', 'bond_code', 'bond_name', 'bond_rating', 'bond_yield', 'note'])
    comp_today = ''
    for i in range(0,2):
        if isinstance(df_hz[comp][i], float): df_hz[comp][i] = ''
        comp_today += df_hz[comp][i]
    if not (comp_today == ''): 
        df_comp_today = pd.read_csv(io.StringIO(comp_today), header=None, 
                 names=['rem_period', 'bond_code', 'bond_name', 'bond_yield', 'bond_rating'], 
                 delimiter=r"\s+", dtype={'bond_code':str})
        compdf = pd.concat([compdf,df_comp_today])
    return compdf

def isfloat(x):
    try:
        float(x)
        return True
    except ValueError:
        return False

def update_mysql(list2Write, sqldatabase):
        Temp.__table__.drop(db.engine)
        db.create_all()
        Session = sessionmaker(db.engine)
        session = Session()
        session.execute(Temp.__table__.insert(), list2Write)
        session.commit()
        session.close()



@app.route('/upload', methods=['GET', 'POST'])
def extract_excel():
    file = request.files['file']
    filename = file.filename
    if filename == "每日成交汇总.xlsx":
        path = os.path.join(os.getcwd(), filename)
        file.save(path)

        df1 = pd.read_excel(path, header=None, sheet_name=0)
        if not df1.empty:
            df1 = df1.iloc[:,0:10]
            df1.columns = ['trade_date', 'rem_period', 'bond_code', 'bond_name', 'bond_rating', 'bond_yield', 'note', 'offer', 'bid', 'amount']
            df1['bond_yield'] = df1['bond_yield'].apply(lambda x: (float(x)*100) if (isfloat(x) and float(x)<1) else x)
            df1 = df1[df1['bond_name'].notnull()]
            # 处理 NaT：使用 to_datetime 将非 date 字符转换成 NaT，再用 strftime 将 NaT 转换成可以被 mysql 识别的 None
            df1['trade_date'] = pd.to_datetime(df1['trade_date'], errors="coerce").apply(lambda x: x.strftime('%Y-%m-%d')if not pd.isnull(x) else None)
            df1 = df1.where(df1.notnull(), None)
            list2Write1 = df1.to_dict(orient="records")
            session = Session()
            session.execute(YaosuNex.__table__.insert(), list2Write1)
            session.commit()
            session.close()

        df2 = pd.read_excel(path, header=None, sheet_name=1)	
        if not df2.empty:
            df2 = df2.iloc[:,0:10]
            df2.columns = ['trade_date', 'rem_period', 'bond_code', 'bond_name', 'bond_rating', 'bond_yield', 'note', 'offer', 'bid']
            df2 = df2[df2['bond_name'].notnull()]
            # 处理 NaT：使用 to_datetime 将非 date 字符转换成 NaT，再用 strftime 将 NaT 转换成可以被 mysql 识别的 None
            df2['trade_date'] = pd.to_datetime(df2['trade_date'], errors="coerce").apply(lambda x: x.strftime('%Y-%m-%d')if not pd.isnull(x) else None)
            df2['note'] = '外部'
            df2 = df2.where(df2.notnull(), None)
            list2Write2 = df2.to_dict(orient="records")
            session = Session()
            session.execute(YaosuOthers.__table__.insert(), list2Write2)
            session.commit()
            session.close()        

        df3 = pd.read_excel(path, sheet_name=2, skiprows=3, header=None, usecols="B:F")
        dict_compname = {0:"国际", 1:"国利", 2:"BGC", 3:"平安", 4:"信唐"}
        df3_final = pd.DataFrame(columns=['trade_date', 'rem_period', 'bond_code', 
                                    'bond_name', 'bond_rating', 'bond_yield','note'])
        trade_date = df1['trade_date'][0]
        # uniform format
        for comp in range(0,5):
            if comp in [0]:
                compdf = dateAppended1(comp, df3)
            if comp in [1,2]:
                compdf = dateAppended4(comp, df3)
            if comp in [3]:
                compdf = dateAppended2(comp, df3)
            if comp in [4]:
                compdf = dateAppended3(comp, df3)
            compdf['note'] = dict_compname[comp] # add trading comp's name
            df3_final = pd.concat([df3_final, compdf])

        df3_final['trade_date'] = trade_date
        df3_final = df3_final[df3_final['bond_name'].notnull()]
        df3_final = df3_final.where(df3_final.notnull(), None)
        list2Write3 = df3_final.to_dict(orient="records")
        session = Session()
        session.execute(Huizong.__table__.insert(), list2Write3)
        session.commit()
        session.close()

        os.remove(path)
        flash("上传成功！") 
    else:
        flash("文件有误 :(")
    return redirect("/")



