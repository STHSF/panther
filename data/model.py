#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version: ??
@author: li
@file: model.py
@time: 2019-08-28 16:17
"""
from sqlalchemy import Column, NUMERIC
from sqlalchemy.types import DECIMAL, DATE, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import BigInteger, Column, DateTime, Float, Index, Integer, String, Text, Boolean, text, JSON

Base = declarative_base()  # 生成ORM基类
metadata = Base.metadata


class BalanceMRQ(Base):
    __tablename__ = 'balance_mrq'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(BigInteger)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    ACCOPAYA = Column(NUMERIC(26, 2))
    ACCORECE = Column(NUMERIC(26, 2))
    ADVAPAYM = Column(NUMERIC(26, 2))
    BDSPAYA = Column(NUMERIC(26, 2))
    CAPISURP = Column(NUMERIC(26, 2))
    CONSPROG = Column(NUMERIC(26, 2))
    CURFDS = Column(NUMERIC(26, 2))
    DEFETAXASSET = Column(NUMERIC(26, 2))
    DEVEEXPE = Column(NUMERIC(26, 2))
    DUENONCLIAB = Column(NUMERIC(26, 2))
    ENGIMATE = Column(NUMERIC(26, 2))
    FIXEDASSECLEATOT = Column(NUMERIC(26, 2))
    FIXEDASSENET = Column(NUMERIC(26, 2))
    GOODWILL = Column(NUMERIC(26, 2))
    INTAASSET = Column(NUMERIC(26, 2))
    INTEPAYA = Column(NUMERIC(26, 2))
    INVE = Column(NUMERIC(26, 2))
    LOGPREPEXPE = Column(NUMERIC(26, 2))
    LONGBORR = Column(NUMERIC(26, 2))
    MINYSHARRIGH = Column(NUMERIC(26, 2))
    NOTESPAYA = Column(NUMERIC(26, 2))
    NOTESRECE = Column(NUMERIC(26, 2))
    OTHERRECE = Column(NUMERIC(26, 2))
    PARESHARRIGH = Column(NUMERIC(26, 2))
    PREP = Column(NUMERIC(26, 2))
    RESE = Column(NUMERIC(26, 2))
    RIGHAGGR = Column(NUMERIC(26, 2))
    SHORTTERMBORR = Column(NUMERIC(26, 2))
    TOTALCURRLIAB = Column(NUMERIC(26, 2))
    TOTALNONCASSETS = Column(NUMERIC(26, 2))
    TOTALNONCLIAB = Column(NUMERIC(26, 2))
    TOTASSET = Column(NUMERIC(26, 2))
    TOTCURRASSET = Column(NUMERIC(26, 2))
    TOTLIAB = Column(NUMERIC(26, 2))
    TRADFINASSET = Column(NUMERIC(26, 2))
    UNDIPROF = Column(NUMERIC(26, 2))
    TAXESPAYA = Column(NUMERIC(26, 2))
    OTHERPAY = Column(NUMERIC(26, 2))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class BalanceTTM(Base):
    __tablename__ = 'balance_ttm'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(BigInteger)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    ACCOPAYA = Column(NUMERIC(26, 2))
    ACCORECE = Column(NUMERIC(26, 2))
    ADVAPAYM = Column(NUMERIC(26, 2))
    BDSPAYA = Column(NUMERIC(26, 2))
    CAPISURP = Column(NUMERIC(26, 2))
    CONSPROG = Column(NUMERIC(26, 2))
    CURFDS = Column(NUMERIC(26, 2))
    DEFETAXASSET = Column(NUMERIC(26, 2))
    DEVEEXPE = Column(NUMERIC(26, 2))
    DUENONCLIAB = Column(NUMERIC(26, 2))
    ENGIMATE = Column(NUMERIC(26, 2))
    FIXEDASSECLEATOT = Column(NUMERIC(26, 2))
    FIXEDASSENET = Column(NUMERIC(26, 2))
    GOODWILL = Column(NUMERIC(26, 2))
    INTAASSET = Column(NUMERIC(26, 2))
    INTEPAYA = Column(NUMERIC(26, 2))
    INVE = Column(NUMERIC(26, 2))
    LOGPREPEXPE = Column(NUMERIC(26, 2))
    LONGBORR = Column(NUMERIC(26, 2))
    MINYSHARRIGH = Column(NUMERIC(26, 2))
    NOTESPAYA = Column(NUMERIC(26, 2))
    NOTESRECE = Column(NUMERIC(26, 2))
    OTHERRECE = Column(NUMERIC(26, 2))
    PARESHARRIGH = Column(NUMERIC(26, 2))
    PREP = Column(NUMERIC(26, 2))
    RESE = Column(NUMERIC(26, 2))
    RIGHAGGR = Column(NUMERIC(26, 2))
    SHORTTERMBORR = Column(NUMERIC(26, 2))
    TOTALCURRLIAB = Column(NUMERIC(26, 2))
    TOTALNONCASSETS = Column(NUMERIC(26, 2))
    TOTALNONCLIAB = Column(NUMERIC(26, 2))
    TOTASSET = Column(NUMERIC(26, 2))
    TOTCURRASSET = Column(NUMERIC(26, 2))
    TOTLIAB = Column(NUMERIC(26, 2))
    TRADFINASSET = Column(NUMERIC(26, 2))
    UNDIPROF = Column(NUMERIC(26, 2))
    TAXESPAYA = Column(NUMERIC(26, 2))
    OTHERPAY = Column(NUMERIC(26, 2))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class BalanceReport(Base):
    __tablename__ = 'balance_report'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    ACCOPAYA = Column(NUMERIC(26, 2))
    ACCORECE = Column(NUMERIC(26, 2))
    ADVAPAYM = Column(NUMERIC(26, 2))
    BDSPAYA = Column(NUMERIC(26, 2))
    CAPISURP = Column(NUMERIC(26, 2))
    CONSPROG = Column(NUMERIC(26, 2))
    CURFDS = Column(NUMERIC(26, 2))
    DEFETAXASSET = Column(NUMERIC(26, 2))
    DEVEEXPE = Column(NUMERIC(26, 2))
    DUENONCLIAB = Column(NUMERIC(26, 2))
    ENGIMATE = Column(NUMERIC(26, 2))
    FIXEDASSECLEATOT = Column(NUMERIC(26, 2))
    FIXEDASSENET = Column(NUMERIC(26, 2))
    GOODWILL = Column(NUMERIC(26, 2))
    INTAASSET = Column(NUMERIC(26, 2))
    INTEPAYA = Column(NUMERIC(26, 2))
    INVE = Column(NUMERIC(26, 2))
    LOGPREPEXPE = Column(NUMERIC(26, 2))
    LONGBORR = Column(NUMERIC(26, 2))
    MINYSHARRIGH = Column(NUMERIC(26, 2))
    NOTESPAYA = Column(NUMERIC(26, 2))
    NOTESRECE = Column(NUMERIC(26, 2))
    OTHERRECE = Column(NUMERIC(26, 2))
    PARESHARRIGH = Column(NUMERIC(26, 2))
    PREP = Column(NUMERIC(26, 2))
    RESE = Column(NUMERIC(26, 2))
    RIGHAGGR = Column(NUMERIC(26, 2))
    SHORTTERMBORR = Column(NUMERIC(26, 2))
    TOTALCURRLIAB = Column(NUMERIC(26, 2))
    TOTALNONCASSETS = Column(NUMERIC(26, 2))
    TOTALNONCLIAB = Column(NUMERIC(26, 2))
    TOTASSET = Column(NUMERIC(26, 2))
    TOTCURRASSET = Column(NUMERIC(26, 2))
    TOTLIAB = Column(NUMERIC(26, 2))
    TRADFINASSET = Column(NUMERIC(26, 2))
    UNDIPROF = Column(NUMERIC(26, 2))
    TAXESPAYA = Column(NUMERIC(26, 2))
    OTHERPAY = Column(NUMERIC(26, 2))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class CashFlowMRQ(Base):
    __tablename__ = 'cash_flow_mrq'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    BIZNETCFLOW = Column(NUMERIC(26, 2))
    CASHNETI = Column(NUMERIC(26, 2))
    CASHNETR = Column(NUMERIC(26, 2))
    FINALCASHBALA = Column(NUMERIC(26, 2))
    FINNETCFLOW = Column(NUMERIC(26, 2))
    INVNETCASHFLOW = Column(NUMERIC(26, 2))
    LABORGETCASH = Column(NUMERIC(26, 2))
    MANANETR = Column(NUMERIC(26, 2))
    ASSEDEPR = Column(NUMERIC(26, 2))
    INTAASSEAMOR = Column(NUMERIC(26, 2))
    LONGDEFEEXPENAMOR = Column(NUMERIC(26, 2))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class CashFlowTTM(Base):
    __tablename__ = 'cash_flow_ttm'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    BIZNETCFLOW = Column(NUMERIC(26, 2))
    CASHNETI = Column(NUMERIC(26, 2))
    CASHNETR = Column(NUMERIC(26, 2))
    FINALCASHBALA = Column(NUMERIC(26, 2))
    FINNETCFLOW = Column(NUMERIC(26, 2))
    INVNETCASHFLOW = Column(NUMERIC(26, 2))
    LABORGETCASH = Column(NUMERIC(26, 2))
    MANANETR = Column(NUMERIC(26, 2))
    ASSEDEPR = Column(NUMERIC(26, 2))
    INTAASSEAMOR = Column(NUMERIC(26, 2))
    LONGDEFEEXPENAMOR = Column(NUMERIC(26, 2))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class CashFlowReport(Base):
    __tablename__ = 'cash_flow_report'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    BIZNETCFLOW = Column(NUMERIC(26, 2))
    CASHNETI = Column(NUMERIC(26, 2))
    CASHNETR = Column(NUMERIC(26, 2))
    FINALCASHBALA = Column(NUMERIC(26, 2))
    FINNETCFLOW = Column(NUMERIC(26, 2))
    INVNETCASHFLOW = Column(NUMERIC(26, 2))
    LABORGETCASH = Column(NUMERIC(26, 2))
    MANANETR = Column(NUMERIC(26, 2))
    ASSEDEPR = Column(NUMERIC(26, 2))
    INTAASSEAMOR = Column(NUMERIC(26, 2))
    LONGDEFEEXPENAMOR = Column(NUMERIC(26, 2))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class IncomeTTM(Base):
    __tablename__ = 'income_ttm'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    ASSEIMPALOSS = Column(NUMERIC(26, 2))
    ASSOINVEPROF = Column(NUMERIC(26, 2))
    BIZCOST = Column(NUMERIC(26, 2))
    BIZINCO = Column(NUMERIC(26, 2))
    BIZTAX = Column(NUMERIC(26, 2))
    BIZTOTCOST = Column(NUMERIC(26, 2))
    BIZTOTINCO = Column(NUMERIC(26, 2))
    COMDIVPAYBABLE = Column(NUMERIC(26, 2))
    DEVEEXPE = Column(NUMERIC(26, 2))
    DILUTEDEPS = Column(NUMERIC(30, 6))
    FINEXPE = Column(NUMERIC(26, 2))
    INCOTAXEXPE = Column(NUMERIC(26, 2))
    INTEEXPE = Column(NUMERIC(26, 2))
    INTEINCO = Column(NUMERIC(26, 2))
    MANAEXPE = Column(NUMERIC(26, 2))
    MINYSHARRIGH = Column(NUMERIC(26, 2))
    NETPROFIT = Column(NUMERIC(26, 2))
    NONOEXPE = Column(NUMERIC(26, 2))
    NONOREVE = Column(NUMERIC(26, 2))
    PARENETP = Column(NUMERIC(26, 2))
    PERPROFIT = Column(NUMERIC(26, 2))
    SALESEXPE = Column(NUMERIC(26, 2))
    TOTPROFIT = Column(NUMERIC(26, 2))
    VALUECHGLOSS = Column(NUMERIC(26, 2))
    INVEINCO = Column(NUMERIC(26, 2))
    EXCHGGAIN = Column(NUMERIC(26, 2))
    POUNINCO = Column(NUMERIC(26, 2))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class IncomeMRQ(Base):
    __tablename__ = 'income_mrq'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    ASSEIMPALOSS = Column(NUMERIC(26, 2))
    ASSOINVEPROF = Column(NUMERIC(26, 2))
    BIZCOST = Column(NUMERIC(26, 2))
    BIZINCO = Column(NUMERIC(26, 2))
    BIZTAX = Column(NUMERIC(26, 2))
    BIZTOTCOST = Column(NUMERIC(26, 2))
    BIZTOTINCO = Column(NUMERIC(26, 2))
    COMDIVPAYBABLE = Column(NUMERIC(26, 2))
    DEVEEXPE = Column(NUMERIC(26, 2))
    DILUTEDEPS = Column(NUMERIC(30, 6))
    FINEXPE = Column(NUMERIC(26, 2))
    INCOTAXEXPE = Column(NUMERIC(26, 2))
    INTEEXPE = Column(NUMERIC(26, 2))
    INTEINCO = Column(NUMERIC(26, 2))
    MANAEXPE = Column(NUMERIC(26, 2))
    MINYSHARRIGH = Column(NUMERIC(26, 2))
    NETPROFIT = Column(NUMERIC(26, 2))
    NONOEXPE = Column(NUMERIC(26, 2))
    NONOREVE = Column(NUMERIC(26, 2))
    PARENETP = Column(NUMERIC(26, 2))
    PERPROFIT = Column(NUMERIC(26, 2))
    SALESEXPE = Column(NUMERIC(26, 2))
    TOTPROFIT = Column(NUMERIC(26, 2))
    VALUECHGLOSS = Column(NUMERIC(26, 2))
    INVEINCO = Column(NUMERIC(26, 2))
    EXCHGGAIN = Column(NUMERIC(26, 2))
    POUNINCO = Column(NUMERIC(26, 2))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class IncomeReport(Base):
    __tablename__ = 'income_report'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    ASSEIMPALOSS = Column(NUMERIC(26, 2))
    ASSOINVEPROF = Column(NUMERIC(26, 2))
    BIZCOST = Column(NUMERIC(26, 2))
    BIZINCO = Column(NUMERIC(26, 2))
    BIZTAX = Column(NUMERIC(26, 2))
    BIZTOTCOST = Column(NUMERIC(26, 2))
    BIZTOTINCO = Column(NUMERIC(26, 2))
    COMDIVPAYBABLE = Column(NUMERIC(26, 2))
    DEVEEXPE = Column(NUMERIC(26, 2))
    DILUTEDEPS = Column(NUMERIC(30, 6))
    FINEXPE = Column(NUMERIC(26, 2))
    INCOTAXEXPE = Column(NUMERIC(26, 2))
    INTEEXPE = Column(NUMERIC(26, 2))
    INTEINCO = Column(NUMERIC(26, 2))
    MANAEXPE = Column(NUMERIC(26, 2))
    MINYSHARRIGH = Column(NUMERIC(26, 2))
    NETPROFIT = Column(NUMERIC(26, 2))
    NONOEXPE = Column(NUMERIC(26, 2))
    NONOREVE = Column(NUMERIC(26, 2))
    PARENETP = Column(NUMERIC(26, 2))
    PERPROFIT = Column(NUMERIC(26, 2))
    SALESEXPE = Column(NUMERIC(26, 2))
    TOTPROFIT = Column(NUMERIC(26, 2))
    VALUECHGLOSS = Column(NUMERIC(26, 2))
    INVEINCO = Column(NUMERIC(26, 2))
    EXCHGGAIN = Column(NUMERIC(26, 2))
    POUNINCO = Column(NUMERIC(26, 2))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class IndicatorReport(Base):
    __tablename__ = 'indicator_report'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    ENDPUBLISHDATE = Column(DATE)
    NPCUT = Column(Float(53))
    EPSFULLDILUTED = Column(Float(53))
    EPSBASIC = Column(Float(53))
    EPSBASICEPSCUT = Column(Float(53))
    ROEWEIGHTED = Column(Float(53))
    ROEWEIGHTEDCUT = Column(Float(53))
    EPSFULLDILUTEDCUT = Column(Float(53))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class IndicatorMRQ(Base):
    __tablename__ = 'indicator_mrq'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    ENDPUBLISHDATE = Column(DATE)
    NPCUT = Column(Float(53))
    EPSFULLDILUTED = Column(Float(53))
    EPSBASIC = Column(Float(53))
    EPSBASICEPSCUT = Column(Float(53))
    ROEWEIGHTED = Column(Float(53))
    ROEWEIGHTEDCUT = Column(Float(53))
    EPSFULLDILUTEDCUT = Column(Float(53))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }


class IndicatorTTM(Base):
    __tablename__ = 'indicator_ttm'
    __table_args__ = {"useexisting": True}
    ID = Column(VARCHAR(32), primary_key=True)
    COMPCODE = Column(BigInteger)
    PUBLISHDATE = Column(DATE)
    ENDDATE = Column(DATE)
    REPORTTYPE = Column(Integer)
    REPORTYEAR = Column(Integer)
    REPORTDATETYPE = Column(Integer)
    ENDPUBLISHDATE = Column(DATE)
    NPCUT = Column(Float(53))
    EPSFULLDILUTED = Column(Float(53))
    EPSBASIC = Column(Float(53))
    EPSBASICEPSCUT = Column(Float(53))
    ROEWEIGHTED = Column(Float(53))
    ROEWEIGHTEDCUT = Column(Float(53))
    EPSFULLDILUTEDCUT = Column(Float(53))
    TMSTAMP = Column(Integer)
    creat_time = Column(DATE)
    update_time = Column(DATE)
    __pit_column__ = {
        'pub_date': PUBLISHDATE,
        'filter_date': ENDDATE,
        'index': COMPCODE
    }
