# -*- coding: utf-8 -*-
from sqlalchemy import Column, DateTime, Float, Index, VARCHAR, String, BigInteger, Integer, DATE, DECIMAL
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Market(Base):
    __tablename__ = 'market'
    __table_args__ = (
        Index('market_idx', 'trade_date', 'code', unique=True),
    )
    trade_date = Column(DateTime, primary_key=True, nullable=False)
    code = Column(String, primary_key=True, nullable=False)
    secShortName = Column(String(10))
    preClosePrice = Column(Float(53))
    actPreClosePrice = Column(Float(53))
    openPrice = Column(Float(53))
    highestPrice = Column(Float(53))
    lowestPrice = Column(Float(53))
    closePrice = Column(Float(53))
    turnoverVol = Column(BigInteger)
    turnoverValue = Column(Float(53))
    dealAmount = Column(BigInteger)
    turnoverRate = Column(Float(53))
    accumAdjFactor = Column(Float(53))
    negMarketValue = Column(Float(53))
    marketValue = Column(Float(53))
    chgPct = Column(Float(53))
    PE = Column(Float(53))
    PE1 = Column(Float(53))
    PB = Column(Float(53))
    isOpen = Column(Integer)
    vwap = Column(Float(53))


class Exposure(Base):
    __tablename__ = 'risk_exposure'
    __table_args__ = (
        Index('trade_date', 'code', unique=True),
    )
    trade_date = Column(DateTime, primary_key=True, nullable=False)
    code = Column(String, primary_key=True, nullable=False)
    BETA = Column(Float(53))
    MOMENTUM = Column(Float(53))
    SIZE = Column(Float(53))
    EARNYILD = Column(Float(53))
    RESVOL = Column(Float(53))
    GROWTH = Column(Float(53))
    BTOP = Column(Float(53))
    LEVERAGE = Column(Float(53))
    LIQUIDTY = Column(Float(53))
    SIZENL = Column(Float(53))
    Bank = Column(Float(53))
    RealEstate = Column(Float(53))
    Health = Column(Float(53))
    Transportation = Column(Float(53))
    Mining = Column(Float(53))
    NonFerMetal = Column(Float(53))
    HouseApp = Column(Float(53))
    LeiService = Column(Float(53))
    MachiEquip = Column(Float(53))
    BuildDeco = Column(Float(53))
    CommeTrade = Column(Float(53))
    CONMAT = Column(Float(53))
    Auto = Column(Float(53))
    Textile = Column(Float(53))
    FoodBever = Column(Float(53))
    Electronics = Column(Float(53))
    Computer = Column(Float(53))
    LightIndus = Column(Float(53))
    Utilities = Column(Float(53))
    Telecom = Column(Float(53))
    AgriForest = Column(Float(53))
    CHEM = Column(Float(53))
    Media = Column(Float(53))
    IronSteel = Column(Float(53))
    NonBankFinan = Column(Float(53))
    ELECEQP = Column(Float(53))
    AERODEF = Column(Float(53))
    Conglomerates = Column(Float(53))
    COUNTRY = Column(Float(53))