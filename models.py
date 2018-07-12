from bondweb import db

class YaosuNex(db.Model):
  __tablename__ = 'YaosuNex'
  Id = db.Column(db.Integer, primary_key=True)
  trade_date = db.Column(db.Date)
  rem_period = db.Column(db.String(50))
  bond_code = db.Column(db.String(50))
  bond_name = db.Column(db.String(50))
  bond_rating = db.Column(db.String(50))
  bond_yield = db.Column(db.String(50))
  note = db.Column(db.String(50))
  offer = db.Column(db.String(50))
  bid = db.Column(db.String(50))
  amount = db.Column(db.String(50))

  def __init__(self, trade_date, rem_period, bond_code, bond_name, bond_rating, bond_yield, note, offer, bid, amount):
    self.trade_date = trade_date
    self.rem_period = rem_period
    self.bond_code = bond_code
    self.bond_name = bond_name
    self.bond_rating = bond_rating
    self.bond_yield = bond_yield
    self.note = note
    self.offer = offer
    self.bid = bid
    self.amount = amount

class YaosuOthers(db.Model):
  __tablename__ = 'YaosuOthers'
  Id = db.Column(db.Integer, primary_key=True)
  trade_date = db.Column(db.Date)
  rem_period = db.Column(db.String(50))
  bond_code = db.Column(db.String(50))
  bond_name = db.Column(db.String(50))
  bond_rating = db.Column(db.String(50))
  bond_yield = db.Column(db.String(50))
  note = db.Column(db.String(50))
  offer = db.Column(db.String(50))
  bid = db.Column(db.String(50))
  amount = db.Column(db.String(50))

  def __init__(self, trade_date, rem_period, bond_code, bond_name, bond_rating, bond_yield, note, offer, bid, amount):
    self.trade_date = trade_date
    self.rem_period = rem_period
    self.bond_code = bond_code
    self.bond_name = bond_name
    self.bond_rating = bond_rating
    self.bond_yield = bond_yield
    self.note = note
    self.offer = offer
    self.bid = bid
    self.amount = amount

class Huizong(db.Model):
  __tablename__ = 'Huizong'
  Id = db.Column(db.Integer, primary_key=True)
  trade_date = db.Column(db.Date)
  rem_period = db.Column(db.String(50))
  bond_code = db.Column(db.String(50))
  bond_name = db.Column(db.String(50))
  bond_rating = db.Column(db.String(50))
  bond_yield = db.Column(db.String(50))
  note = db.Column(db.String(50))
  offer = db.Column(db.String(50))
  bid = db.Column(db.String(50))
  amount = db.Column(db.String(50))

  def __init__(self, trade_date, rem_period, bond_code, bond_name, bond_rating, bond_yield, note, offer, bid, amount):
    self.trade_date = trade_date
    self.rem_period = rem_period
    self.bond_code = bond_code
    self.bond_name = bond_name
    self.bond_rating = bond_rating
    self.bond_yield = bond_yield
    self.note = note
    self.offer = offer
    self.bid = bid
    self.amount = amount

