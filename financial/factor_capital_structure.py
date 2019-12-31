#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version:
@author: li
@file: factor_operation_capacity.py
@time: 2019-05-30
"""
import gc, six
import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
from utilities.calc_tools import CalcTools
from utilities.singleton import Singleton
# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


@six.add_metaclass(Singleton)
class FactorCapitalStructure(object):
    """
    资本结构
    """
    def __init__(self):
        __str__ = 'factor_capital_structure'
        self.name = '财务指标'
        self.factor_type1 = '财务指标'
        self.factor_type2 = '资本结构'
        self.description = '财务指标二级指标-资本结构'

    @staticmethod
    def NonCurrAssetRatio(tp_management, factor_management, dependencies=['total_non_current_assets',
                                                                          'total_assets']):
        """
        :name: 非流动资产比率（MRQ）
        :desc: 非流动资产/总资产*100%（MRQ）
        :unit:
        :view_dimension: 0.01
        """

        management = tp_management.loc[:, dependencies]
        management['NonCurrAssetRatio'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.total_non_current_assets.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        return factor_management

    @staticmethod
    def LongDebtToAsset(tp_management, factor_management, dependencies=['total_non_current_liability',
                                                                        'total_assets']):
        """
        :name: 长期负债与资产总计之比（MRQ）
        :desc: 非流动负债合计MRQ/资产总计MRQ
        :unit:
        :view_dimension: 0.01
        """

        management = tp_management.loc[:, dependencies]
        management['LongDebtToAsset'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.total_non_current_liability.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        return factor_management

    @staticmethod
    def LongBorrToAssert(tp_management, factor_management, dependencies=['longterm_loan',
                                                                         'total_assets']):
        """
        :name: 长期借款与资产总计之比（MRQ）
        :desc: 长期借款MRQ/资产总计MRQ
        :unit:
        :view_dimension: 0.01
        """

        management = tp_management.loc[:, dependencies]
        management['LongBorrToAssert'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.longterm_loan.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        return factor_management

    @staticmethod
    def IntangibleAssetRatio(tp_management, factor_management, dependencies=['intangible_assets',
                                                                             'development_expenditure',
                                                                             'good_will',
                                                                             'total_assets']):
        """
        :name: 无形资产比率（MRQ）
        :desc:（无形资产MRQ+开发支出MRQ+商誉MRQ）/资产总计MRQ           分母为NAN的科目记为0
        """

        management = tp_management.loc[:, dependencies]
        management["ia"] = (management.intangible_assets + management.development_expenditure + management.good_will)
        management['IntangibleAssetRatio'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.ia.values / management.total_assets.values)
        dependencies = dependencies + ['ia']
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        # factor_management['IntangibleAssetRatio'] = management['IntangibleAssetRatio']
        return factor_management

    @staticmethod
    def FixAssetsRt(tp_management, factor_management, dependencies=['fixed_assets_netbook',
                                                                    'construction_materials',
                                                                    'constru_in_process',
                                                                    'total_assets']):
        """
        :name: 固定资产比率（MRQ）
        :desc: (固定资产*MRQ+工程物资MRQ+在建工程MRQ）/资产总计MRQ；分母为NAN的科目记为0
        :unit:
        :view_dimension: 0.01
        """

        management = tp_management.loc[:, dependencies]
        management['FixAssetsRt'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            (management.fixed_assets_netbook.values +
             management.construction_materials.values +
             management.constru_in_process.values) / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        return factor_management

    @staticmethod
    def EquityToAsset(tp_management, factor_management, dependencies=['total_owner_equities',
                                                                      'total_assets']):
        """
        :name: 股东权益比率（MRQ）
        :desc: 股东权益MRQ/资产总计MRQ
        :unit:
        :view_dimension: 0.01
        """

        management = tp_management.loc[:, dependencies]
        management['EquityToAsset'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.total_owner_equities.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        return factor_management

    @staticmethod
    def EquityToFixedAsset(tp_management, factor_management, dependencies=['total_owner_equities',
                                                                           'fixed_assets_netbook',
                                                                           'construction_materials',
                                                                           'constru_in_process']):
        """
        :name: 股东权益与固定资产比率（MRQ）
        :desc: 股东权益MRQ/（固定资产MRQ+工程物资MRQ+在建工程MRQ）分子为NAN的科目记为0
        :unit:
        :view_dimension: 0.01
        """
        management = tp_management.loc[:, dependencies]
        management['EquityToFixedAsset'] = np.where(
            CalcTools.is_zero(management.fixed_assets_netbook.values +
                              management.construction_materials.values +
                              management.constru_in_process.values), 0,
            management.total_owner_equities.values
            / (management.fixed_assets_netbook.values
               + management.construction_materials.values
               + management.constru_in_process.values))
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        return factor_management

    @staticmethod
    def CurAssetsR(tp_management, factor_management, dependencies=['total_current_assets',
                                                                   'total_assets']):
        """
        :name: 流动资产比率（MRQ）
        :desc: 流动资产/总资产*100%（MRQ）
        :unit:
        :view_dimension: 0.01
        """
        management = tp_management.loc[:, dependencies]
        management['CurAssetsR'] = np.where(
            CalcTools.is_zero(management.total_assets.values), 0,
            management.total_current_assets.values / management.total_assets.values)
        management = management.drop(dependencies, axis=1)
        factor_management = pd.merge(factor_management, management, how='outer', on="security_code")
        return factor_management

    @staticmethod
    def CaptCashRecRtTTM(tp_management, factor_management, dependencies=['net_operate_cash_flow_indirect',
                                                                         'total_assets']):
        """
        :name: 资金现金回收率(TTM)
        :desc: 经营活动现金净流量(TTM)/资产总计(MRQ)
        :unit:
        :view_dimension: 0.01
        """
        management = tp_management.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] !=0 and x[1] is not None and x[0] is not None else None
        management['CaptCashRecRtTTM'] = management.apply(func, axis=1)
        management = management.drop(columns=dependencies, axis=1)
        factor_earning = pd.merge(factor_management, management, how='outer', on="security_code")
        return factor_earning

    @staticmethod
    def AssertLibRtTTM(tp_management, factor_management, dependencies=['total_liability',
                                                                       'total_assets']):
        """
        :name: 资产负债率(TTM)
        :desc: 负债合计(TTM)/资产总计(MRQ)
        :unit:
        :view_dimension: 0.01
        """
        management = tp_management.loc[:, dependencies]
        func = lambda x: x[0] / x[1] if x[1] != 0 and x[1] is not None and x[0] is not None else None
        management['AssertLibRtTTM'] = management.apply(func, axis=1)
        management = management.drop(columns=dependencies, axis=1)
        factor_earning = pd.merge(factor_management, management, how='outer', on="security_code")
        return factor_earning