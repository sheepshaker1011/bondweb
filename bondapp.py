from flask import Flask, render_template, request, redirect, g, jsonify, flash, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, mapper
from sqlalchemy.pool import NullPool
from sqlalchemy import create_engine, MetaData, Table, or_, func
from sqlalchemy import *
import pymysql
import os, io
import pandas as pd

app = Flask(__name__)
#app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
#app.config['SQLALCHEMY_DATABASE_URI'] = 
app.config['SECRET_KEY'] = '123456'

engine = create_engine("mysql+pymysql://root:f1c2y319931011@127.0.0.1:3306/bond?charset=utf8mb4", poolclass=NullPool, echo=True)
Session = sessionmaker(bind=engine)
metadata = MetaData()
fiveComps = Table('fiveComps', metadata, 
     Column('Id', Integer, primary_key=True),
     Column('trade_date', DATE),
     Column('rem_period', String(50)),
      Column('bond_code', String(50)),
      Column('bond_name', String(50)),
      Column('bond_rating', String(50)),
      Column('bond_yield', String(50)),
      Column('note', String(50)),
      Column('offer', String(50)),
      Column('bid', String(50)),
      Column('amount', String(50)))
nex = Table('nex', metadata,
     Column('Id', Integer, primary_key=True),
     Column('trade_date', DATE),
     Column('rem_period', String(50)),
      Column('bond_code', String(50)),
      Column('bond_name', String(50)),
      Column('bond_rating', String(50)),
      Column('bond_yield', String(50)),
      Column('note', String(50)),
      Column('offer', String(50)),
      Column('bid', String(50)),
      Column('amount', String(50)))
waiBu = Table('waiBu', metadata,
     Column('Id', Integer, primary_key=True),
     Column('trade_date', DATE),
     Column('rem_period', String(50)),
      Column('bond_code', String(50)),
      Column('bond_name', String(50)),
      Column('bond_rating', String(50)),
      Column('bond_yield', String(50)),
      Column('note', String(50)),
      Column('offer', String(50)),
      Column('bid', String(50)),
      Column('amount', String(50)))
temp = Table('temp', metadata,
     Column('Id', Integer, primary_key=True),
     Column('trade_date', DATE),
     Column('rem_period', String(50)),
      Column('bond_code', String(50)),
      Column('bond_name', String(50)),
      Column('bond_rating', String(50)),
      Column('bond_yield', String(50)),
      Column('note', String(50)),
      Column('offer', String(50)),
      Column('bid', String(50)),
      Column('amount', String(50)))


@app.route('/', methods = ['GET', 'POST'])
def search():
    return render_template("index.html")

@app.route('/bondsearch',methods = ['GET', 'POST'])
def search_bond():
    bond_name = request.form['bondname']
    session = Session()  
    res1 = session.query(fiveComps).filter(or_(fiveComps.c.bond_name.like("%"+bond_name+"%"), fiveComps.c.bond_code.like("%"+bond_name+"%")))
    res2 = session.query(nex).filter(or_(nex.c.bond_name.like("%"+bond_name+"%"), nex.c.bond_code.like("%"+bond_name+"%")))
    res3 = session.query(waiBu).filter(or_(waiBu.c.bond_name.like("%"+bond_name+"%"),waiBu.c.bond_code.like("%"+bond_name+"%")))
    res = res1.union(res2, res3).all()
    session.close()
    return render_template("bondsearch.html", res=res)

@app.route('/compsearch',methods = ['GET', 'POST'])
def search_comp():
    comp_name = request.form['compname']
    session = Session()  
    res1 = session.query(fiveComps).filter(or_(fiveComps.c.bid.like("%"+comp_name+"%"),fiveComps.c.offer.like("%"+comp_name+"%")))
    res2 = session.query(nex).filter(or_(nex.c.bid.like("%"+comp_name+"%"),nex.c.offer.like("%"+comp_name+"%")))
    res3 = session.query(waiBu).filter(or_(waiBu.c.bid.like("%"+comp_name+"%"),waiBu.c.offer.like("%"+comp_name+"%")))
    res = res1.union(res2, res3).all()
    session.close()
    return render_template("compsearch.html", res=res)

########## upload ##########
# create trade_date from sheetname
def tradeDate(sheetname):
    date = sheetname.split(".")
    srt_date = "2018-"+date[0]+"-"+date[1]
    return srt_date
# designed for different companys' formats, return uni-formated data with trade_date
def dateAppended1(comp, df_hz):
    compdf = pd.DataFrame(columns=['trade_date', 'rem_period', 'bond_code', 'bond_name', 'bond_rating', 'bond_yield'])
    comp_today = ''
    for sheet in df_hz.keys():
        if df_hz[sheet].empty: continue
        comp_today = df_hz[sheet][comp][0]+df_hz[sheet][comp][1]+df_hz[sheet][comp][2]
        if not isinstance(comp_today, str): continue
        df_comp_today = pd.read_csv(io.StringIO(comp_today), header=None, 
                 names=['rem_period', 'bond_code', 'bond_name', 'bond_rating', 'bond_yield'], 
                 delimiter=r"\s+", dtype={'bond_code':str})
        df_comp_today['trade_date'] = tradeDate(sheet)
        compdf = pd.concat([compdf,df_comp_today])
    return compdf
def dateAppended2(comp, df_hz):
    compdf = pd.DataFrame(columns=['trade_date', 'rem_period', 'bond_code', 'bond_name', 'bond_rating', 'bond_yield'])
    comp_today = ''
    for sheet in df_hz.keys():
        if df_hz[sheet].empty: continue
        comp_today = df_hz[sheet][comp][0]+df_hz[sheet][comp][1]+df_hz[sheet][comp][2]
        if not isinstance(comp_today, str): continue
        df_comp_today = pd.read_csv(io.StringIO(comp_today), header=None, 
                 names=['rem_period', 'bond_name', 'bond_code', 'bond_rating', 'bond_yield'], 
                 delimiter=r"\s+", dtype={'bond_code':str})
        df_comp_today['trade_date'] = tradeDate(sheet)
        compdf = pd.concat([compdf,df_comp_today])
    return compdf
def dateAppended3(comp, df_hz):
    compdf = pd.DataFrame(columns=['trade_date', 'rem_period', 'bond_code', 'bond_name', 'bond_rating', 'bond_yield'])
    comp_today = ''
    for sheet in df_hz.keys():
        if df_hz[sheet].empty: continue
        comp_today = df_hz[sheet][comp][0]+df_hz[sheet][comp][1]+df_hz[sheet][comp][2]
        if not isinstance(comp_today, str): continue
        df_comp_today = pd.read_csv(io.StringIO(comp_today), header=None, 
                 names=['rem_period', 'bond_name', 'bond_code', 'bond_yield', 'bond_rating'], 
                 delimiter=r"\s+", dtype={'bond_code':str})
        df_comp_today['trade_date'] = tradeDate(sheet)
        compdf = pd.concat([compdf,df_comp_today])
    return compdf
def dateAppended4(comp, df_hz):
    compdf = pd.DataFrame(columns=['trade_date', 'rem_period', 'bond_code', 'bond_name', 'bond_rating', 'bond_yield'])
    comp_today = ''
    for sheet in df_hz.keys():
        if df_hz[sheet].empty: continue
        comp_today = df_hz[sheet][comp][0]+df_hz[sheet][comp][1]+df_hz[sheet][comp][2]
        if not isinstance(comp_today, str): continue
        df_comp_today = pd.read_csv(io.StringIO(comp_today), header=None, 
                 names=['rem_period', 'bond_code', 'bond_name', 'bond_yield', 'bond_rating'], 
                 delimiter=r"\s+", dtype={'bond_code':str})
        df_comp_today['trade_date'] = tradeDate(sheet)
        compdf = pd.concat([compdf,df_comp_today])
    return compdf

def to_dfhz(path):
    df_hz = pd.read_excel(path, sheet_name=None, 
                      skiprows=3, header=None, parse_cols="B:F")
    dict_compname = {0:"国际", 1:"国利", 2:"BGC", 3:"平安", 4:"信唐"}
    df_hz_final = pd.DataFrame(columns=['trade_date', 'rem_period', 'bond_code', 
                                    'bond_name', 'bond_rating', 'bond_yield','comp'])
    # uniform format
    for comp in range(0,5):
        if comp in [0]:
            compdf = dateAppended1(comp, df_hz)
        if comp in [1,2]:
            compdf = dateAppended4(comp, df_hz)
        if comp in [3]:
            compdf = dateAppended2(comp, df_hz)
        if comp in [4]:
            compdf = dateAppended3(comp, df_hz)
        compdf['comp'] = dict_compname[comp] # add trading comp's name
        df_hz_final = pd.concat([df_hz_final, compdf])

    return df_hz_final

def isfloat(x):
    try:
        float(x)
        return True
    except ValueError:
        return False
def update_mysql(list2Write, sqldatabase):
        session = Session()
        temp.drop(engine, checkfirst=True)
        metadata.create_all(bind=engine)
        session.execute(temp.insert(), list2Write)
        session.commit()
        session.close()
        # compare temp to databsase
        session = Session()
        t = session.query(func.max(sqldatabase.c.trade_date))
        sel = select([temp.c.trade_date, temp.c.rem_period, temp.c.bond_code, temp.c.bond_name, temp.c.bond_rating, temp.c.bond_yield, temp.c.note, temp.c.offer, temp.c.bid, temp.c.amount]).where(temp.c.trade_date>t)
        insert = sqldatabase.insert().from_select(['trade_date','rem_period', 'bond_code', 'bond_name', 'bond_rating', 'bond_yield', 'note', 'offer', 'bid', 'amount'], sel)
        session.execute(insert)
        session.commit()
        session.close()
@app.route('/upload', methods=['GET', 'POST'])
def upload_excel():
    file = request.files['file']
    filename = file.filename
    if "月信用成交汇总" in filename:
        path = os.path.join(os.getcwd(), filename)
        file.save(path)
        df = to_dfhz(path)
        df = df[df['bond_name'].notnull()]
        # 处理 NaT：使用 to_datetime 将非 date 字符转换成 NaT，再用 strftime 将 NaT 转换成可以被 mysql 识别的 None
        df['trade_date'] = pd.to_datetime(df['trade_date'], errors="coerce").apply(lambda x: x.strftime('%Y-%m-%d')if not pd.isnull(x) else None)
        df = df.where(df.notnull(), None)
        list2Write = df.to_dict(orient="records")
        update_mysql(list2Write, fiveComps)
        os.remove(path)
        flash("上传成功！")
    elif "每日成交情况" in filename:
        path = os.path.join(os.getcwd(), filename)
        file.save(path)
        df = pd.read_excel(path, header=None, sheet_name=0)
        df = df.iloc[:,0:10]
        # uniform % and float
        df.columns = ['trade_date', 'rem_period', 'bond_code', 'bond_name', 'bond_rating', 'bond_yield', 'note', 'offer', 'bid', 'amount']
        df['bond_yield'] = df['bond_yield'].apply(lambda x: (float(x)*100) 
                          if (isfloat(x) and float(x)<1) else x)
        df = df[df['bond_name'].notnull()]
        # 处理 NaT：使用 to_datetime 将非 date 字符转换成 NaT，再用 strftime 将 NaT 转换成可以被 mysql 识别的 None
        df['trade_date'] = pd.to_datetime(df['trade_date'], errors="coerce").apply(lambda x: x.strftime('%Y-%m-%d')if not pd.isnull(x) else None)
        df = df.where(df.notnull(), None)
        list2Write = df.to_dict(orient="records")
        update_mysql(list2Write, nex)
        os.remove(path)
        flash("上传成功！")
    elif "外部成交情况" in filename:
        path = os.path.join(os.getcwd(), filename)
        file.save(path)
        df1 = pd.read_excel(path, header=None, sheet_name=0)
        df2 = pd.read_excel(path, header=None, sheet_name=1)
        df = pd.concat([df1, df2])
        df = df.iloc[:,0:9]
        df[10] = None
        # uniform % and float
        df.columns = ['trade_date', 'rem_period', 'bond_code', 
               'bond_name', 'bond_rating', 'bond_yield', 'note',  
                        'offer', 'bid', 'amount']
        df = df[df['bond_name'].notnull()]
        # 处理 NaT：使用 to_datetime 将非 date 字符转换成 NaT，再用 strftime 将 NaT 转换成可以被 mysql 识别的 None
        df['trade_date'] = pd.to_datetime(df['trade_date'], errors="coerce").apply(lambda x: x.strftime('%Y-%m-%d')if not pd.isnull(x) else None)
        df = df.where(df.notnull(), None)
        list2Write = df.to_dict(orient="records")
        update_mysql(list2Write, waiBu)
        os.remove(path)
        flash("上传成功！")
    else:
        flash('文件有误 :(')
    return render_template("index.html")

if __name__ == '__main__':
    app.run()



