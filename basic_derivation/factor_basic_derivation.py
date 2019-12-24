#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
@version:
@author: li
@file: factor_operation_capacity.py
@time: 2019-05-30
"""
import gc
import sys

sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import six, pdb
import pandas as pd
from pandas.io.json import json_normalize
from utilities.singleton import Singleton

# from basic_derivation import app
# from ultron.cluster.invoke.cache_data import cache_data
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)


@six.add_metaclass(Singleton)
class FactorBasicDerivation(object):
    """
    基础衍生类因子
    """

    def __init__(self):
        __str__ = 'factor_basic_derivation'
        self.name = '基础衍生'
        self.factor_type1 = '基础衍生'
        self.factor_type2 = '基础衍生'
        self.description = '基础衍生类因子'

    @staticmethod
    def EBIT(tp_derivation, factor_derivation, dependencies=['total_profit', 'interest_expense', 'interest_income', 'financial_expense']):
        """
        :name: 息前税后利润(MRQ)
        :desc: [EBIT_反推法]息前税后利润 = 利润总额 + 利息支出 - 利息收入
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]

        if len(management) <= 0:
            return None
        func = lambda x: (x[0] + x[1] - x[2]) if x[1] is not None and x[2] is not None else (x[0] + x[3] if x[3] is not None else None)
        management['EBIT'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def EBITDA(tp_derivation, factor_derivation, dependencies=['total_profit', 'interest_expense', 'income_tax'],
               dependency=['EBIT']):
        """
        :name: 息前税后利润(MRQ)
        :desc: 息前税后利润(MRQ)＝EBIT(反推法)*(if 所得税&利润总额都>0，则1-所得税率，否则为1)，所得税税率 = 所得税/ 利润总额
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management2 = factor_derivation.loc[:, dependency]
        management = pd.merge(management, management2, how='outer', on='security_code')
        management = management.fillna(0)
        if len(management) <= 0:
            return None
        dependencies = dependencies.append(dependency)

        func = lambda x: None if x[0] is None or x[1] is None or x[2] is None or x[0] == 0 else ((x[0] + x[1]) * (1 - x[2] / x[0]) if x[0] >0 and x[1] >0 else 1)
        management['EBITDA'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def FCFF(tp_derivation, factor_derivation, dependencies=['total_profit',
                                                             'interest_expense',
                                                             'income_tax',
                                                             'fixed_assets_depreciation',
                                                             'intangible_assets_amortization',
                                                             'defferred_expense_amortization',
                                                             'total_current_assets',
                                                             'total_current_liability',
                                                             'total_current_assets_PRE',
                                                             'total_current_liability_PRE',
                                                             'fix_intan_other_asset_acqui_cash',
                                                             ]):
        """
        :name: 企业自由现金流量(MRQ)
        :desc: 息前税后利润+折旧与摊销-营运资本增加-资本支出 = 息税前利润(1-所得税率)+ 折旧与摊销-营运资本增加-构建固定无形和长期资产支付的现金， 营运资本 = 流动资产-流动负债
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)
        if len(management) <= 0:
            return None
        func = lambda x: (x[0] + x[1]) - x[1] + x[2] + x[3] + x[4] - (x[5] - x[6]) + (x[7] - x[8]) - x[9] if x[0] is not None and \
                                                                                                    x[1] is not None and \
                                                                                                    x[2] is not None and \
                                                                                                    x[3] is not None and \
                                                                                                    x[4] is not None and \
                                                                                                    x[5] is not None and \
                                                                                                    x[6] is not None and \
                                                                                                    x[7] is not None and \
                                                                                                    x[8] is not None and \
                                                                                                    x[
                                                                                                        9] is not None else None
        management['FCFF'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def FCFE(tp_derivation, factor_derivation, dependencies=['ebit_mrq',
                                                             'income_tax',
                                                             'fixed_assets_depreciation',
                                                             'intangible_assets_amortization',
                                                             'defferred_expense_amortization',
                                                             'total_current_assets',
                                                             'total_current_liability',
                                                             'total_current_assets_PRE',
                                                             'total_current_liability_PRE',
                                                             'fix_intan_other_asset_acqui_cash',
                                                             'borrowing_repayment',
                                                             'cash_from_borrowing',
                                                             'cash_from_bonds_issue']):
        """
        :name: 股东自由现金流量(MRQ)
        :desc: 企业自由现金流量-偿还债务所支付的现金+取得借款收到的现金+发行债券所收到的现金（MRQ)
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)
        if len(management) <= 0:
            return None
        func = lambda x: x[0] - x[1] + x[2] + x[3] + x[4] - (x[5] - x[6]) + (x[7] - x[8]) - x[9] - x[10] + x[11] + x[12] \
            if x[0] is not None and \
               x[1] is not None and \
               x[2] is not None and \
               x[3] is not None and \
               x[4] is not None and \
               x[5] is not None and \
               x[6] is not None and \
               x[7] is not None and \
               x[8] is not None and \
               x[9] is not None and \
               x[10] is not None and \
               x[11] is not None and \
               x[12] is not None else None

        management['FCFE'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NonRecGainLoss(tp_derivation, factor_derivation, dependencies=['np_parent_company_owners', 'np_cut']):
        """
        :name: 非经常性损益(MRQ)
        :desc: 归属母公司净利润(MRQ) - 扣非净利润(MRQ)
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)
        if len(management) <= 0:
            return None
        func = lambda x: x[0] - x[1] if x[0] is not None and x[1] is not None else None
        management['NonRecGainLoss'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetOptInc(tp_derivation, factor_derivation, sw_industry,
                  dependencies_er=['total_operating_revenue', 'total_operating_cost'],
                  dependencies_yh=['commission_income', 'net_profit', 'other_business_profits', 'operating_cost'],
                  dependencies_zq=['commission_income', 'net_profit', 'other_operating_revenue', 'operating_cost'],
                  dependencies_bx=['operating_revenue', 'operating_cost', 'fair_value_variable_income',
                                   'investment_income', 'exchange_income']):
        """
        :name: 经营活动净收益(MRQ)
        :desc: 新准则（一般企业）：营业总收入-营业总成本"
        :unit: 元
        :view_dimension: 10000
        """
        industry2_set = ['430100', '370100', '410400', '450500', '640500', '510100', '620500', '610200', '330200',
                         '280400', '620400', '450200', '270500', '610300', '280300', '360300', '410100', '370400',
                         '280200', '730200', '710200', '720200', '640400', '270300', '110400', '220100', '240300',
                         '270400', '710100', '420100', '420500', '420400', '370600', '720100', '640200', '220400',
                         '330100', '630200', '610100', '370300', '410300', '220300', '640100', '490300', '450300',
                         '220200', '370200', '460200', '420200', '460100', '360100', '620300', '110500', '650300',
                         '420600', '460300', '720300', '270200', '630400', '410200', '280100', '210200', '420700',
                         '650200', '340300', '220600', '110300', '350100', '620100', '210300', '240200', '340400',
                         '240500', '360200', '270100', '230100', '370500', '110100', '460400', '110700', '110200',
                         '630300', '450400', '220500', '730100', '640300', '630100', '240400', '420800', '650100',
                         '350200', '620200', '210400', '420300', '110800', '360400', '650400', '110600', '460500',
                         '430200', '210100', '240100', '250100', '310300', '320200', '310400', '310200', '320100',
                         '260500', '250200', '450100', '470200', '260200', '260400', '260100', '440200', '470400',
                         '310100', '260300', '220700', '470300', '470100', '340100', '340200', '230200']
        dependencies = list(set(dependencies_er + dependencies_yh + dependencies_bx + dependencies_zq))
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)
        management = pd.merge(management, sw_industry, how='outer', on='security_code').set_index('security_code')
        if len(management) <= 0:
            return None
        management_tm = pd.DataFrame()

        func = lambda x: x[0] + x[1] + x[2] - x[3] if x[0] is not None and \
                                                      x[1] is not None and \
                                                      x[2] is not None and \
                                                      x[3] is not None else None
        # 银行 ['440100', '480100']
        management_yh = management[management['industry_code2'].isin(['440100', '480100'])]
        management_yh['NetOptInc'] = management_yh[dependencies_yh].apply(func, axis=1)
        management_tm = management_tm.append(management_yh)

        # 证券['440300'， '490100']
        management_zq = management[management['industry_code2'].isin(['440300', '490100'])]
        management_zq['NetOptInc'] = management_zq[dependencies_zq].apply(func, axis=1)
        management_tm = management_tm.append(management_zq)

        func1 = lambda x: x[0] - x[1] - x[2] - x[3] - x[4] if x[0] is not None and \
                                                              x[1] is not None and \
                                                              x[2] is not None and \
                                                              x[3] is not None and \
                                                              x[4] is not None else None
        # 保险['440400'， '490200']
        management_bx = management[management['industry_code2'].isin(['440400', '490200'])]
        management_bx['NetOptInc'] = management_bx[dependencies_bx].apply(func1, axis=1)
        management_tm = management_tm.append(management_bx)

        func2 = lambda x: x[0] - x[1] if x[0] is not None and x[1] is not None else None
        management_er = management[management['industry_code2'].isin(industry2_set)]
        management_er['NetOptInc'] = management_er[dependencies_er].apply(func2, axis=1)
        management_tm = management_tm.append(management_er)

        dependencies = dependencies + ['industry_code2']
        management_tm = management_tm.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management_tm, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def WorkingCap(tp_derivation, factor_derivation, dependencies=['total_current_assets',
                                                                   'total_current_liability']):
        """
        :name:  运营资本(MRQ)
        :desc:  流动资产（MRQ）-流动负债（MRQ）
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)
        if len(management) <= 0:
            return None
        func = lambda x: x[0] - x[1] if x[0] is not None and x[1] is not None else None
        management['WorkingCap'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def TangibleAssets(tp_derivation, factor_derivation, dependencies=['equities_parent_company_owners',
                                                                       'intangible_assets',
                                                                       'development_expenditure',
                                                                       'good_will',
                                                                       'long_deferred_expense',
                                                                       'deferred_tax_assets']):
        """
        :name: 有形资产(MRQ)
        :desc: 股东权益（不含少数股东权益）- （无形资产 + 开发支出 + 商誉 + 长期待摊费用 + 递延所得税资产）
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)

        if len(management) <= 0:
            return None
        func = lambda x: x[0] - (x[1] + x[2] + x[3] + x[4] + x[5]) if x[0] is not None and \
                                                                      x[1] is not None and \
                                                                      x[2] is not None and \
                                                                      x[3] is not None and \
                                                                      x[4] is not None and \
                                                                      x[5] is not None else None
        management['TangibleAssets'] = management[dependencies].apply(func, axis=1)

        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def RetainedEarnings(tp_derivation, factor_derivation, dependencies=['surplus_reserve_fund',
                                                                         'retained_profit']):
        """
        :name: 留存收益(MRQ)
        :desc: 盈余公积MRQ + 未分配利润MRQ
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)
        if len(management) <= 0:
            return None
        func = lambda x: x[0] + x[1] if x[0] is not None and x[1] is not None else None
        management['RetainedEarnings'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def InterestBearingLiabilities(tp_derivation, factor_derivation, dependencies=['shortterm_loan',
                                                                                   'non_current_liability_in_one_year',
                                                                                   'longterm_loan',
                                                                                   'bonds_payable',
                                                                                   'interest_payable']):
        """
        :name: 带息负债(MRQ)
        :desc: 带息负债 = 短期借款 + 一年内到期的长期负债 + 长期借款 + 应付债券 + 应付利息
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)
        if len(management) <= 0:
            return None
        func = lambda x: x[0] + x[1] + x[2] + x[3] + x[4] if x[0] is not None and \
                                                             x[1] is not None and \
                                                             x[2] is not None and \
                                                             x[3] is not None and \
                                                             x[4] is not None else None
        management['InterestBearingLiabilities'] = management[dependencies].apply(func, axis=1)

        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetDebt(tp_derivation, factor_derivation, dependencies=['shortterm_loan',
                                                                'longterm_loan',
                                                                'non_current_liability_in_one_year',
                                                                'bonds_payable',
                                                                'interest_payable',
                                                                'cash_equivalents']):
        """
        :name: 净债务(MRQ)
        :desc: 净债务 = 带息债务(MRQ) - 货币资金(MRQ)。 其中，带息负债 = 短期借款 + 一年内到期的长期负债 + 长期借款 + 应付债券 + 应付利息
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)

        if len(management) <= 0:
            return None
        func = lambda x: x[0] + x[1] + x[2] + x[3] + x[4] - x[5] if x[0] is not None and \
                                                                    x[1] is not None and \
                                                                    x[2] is not None and \
                                                                    x[3] is not None and \
                                                                    x[4] is not None and \
                                                                    x[5] is not None else None
        management['NetDebt'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)

        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def InterestFreeCurLb(tp_derivation, factor_derivation, dependencies=['notes_payable',
                                                                          'accounts_payable',
                                                                          'advance_peceipts',
                                                                          'interest_payable',
                                                                          'taxs_payable',
                                                                          'other_payable'
                                                                          ]):
        """
        :name: 无息流动负债(MRQ)
        :desc: 无息流动负债包括应付票据、应付账款、预收账款、应交税费、应付利息、其他应付款、其他流动负债
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)
        if len(management) <= 0:
            return None
        # func = lambda x: x[0] + x[1] + x[2] + x[3] + x[4] + x[5] if x[0] is not None and\
        #                                                             x[1] is not None and\
        #                                                             x[2] is not None and\
        #                                                             x[3] is not None and\
        #                                                             x[4] is not None and\
        #                                                             x[5] is not None else None
        func = lambda x: x[0] + x[1] + x[2] + x[3] + x[4] + x[5] if x[0] is not None or \
                                                                    x[1] is not None or \
                                                                    x[2] is not None or \
                                                                    x[3] is not None or \
                                                                    x[4] is not None or \
                                                                    x[5] is not None else None

        management['InterestFreeCurLb'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)

        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def InterestFreeNonCurLb(tp_derivation, factor_derivation, dependencies=['total_non_current_liability',
                                                                             'longterm_loan',
                                                                             'bonds_payable']):
        """
        :name: 无息非流动负债(MRQ)
        :desc: 非流动负债合计 - 长期借款 - 应付债券
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)
        if len(management) <= 0:
            return None
        func = lambda x: x[0] - x[1] - x[2] if x[0] is not None and x[1] is not None and x[2] is not None else None
        management['InterestFreeNonCurLb'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)

        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def DepAndAmo(tp_derivation, factor_derivation, dependencies=['fixed_assets_depreciation',
                                                                  'intangible_assets_amortization',
                                                                  'defferred_expense_amortization']):
        """
        :name: 折旧和摊销(MRQ)
        :desc: 固定资产折旧 + 无形资产摊销 + 长期待摊费用摊销
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)
        if len(management) <= 0:
            return None
        func = lambda x: x[0] + x[1] + x[2] if x[0] is not None and x[1] is not None and x[2] is not None else None
        management['DepAndAmo'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def EquityPC(tp_derivation, factor_derivation, dependencies=['equities_parent_company_owners']):
        """
        :name: 归属于母公司的股东权益(MRQ)
        :desc: 归属于母公司的股东权益(MRQ)
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)

        if len(management) <= 0:
            return None
        management = management.rename(columns={'equities_parent_company_owners': 'EquityPC'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def TotalInvestedCap(tp_derivation, factor_derivation, dependencies=['total_owner_equities',
                                                                         'total_liability',
                                                                         'notes_payable',
                                                                         'accounts_payable',
                                                                         'advance_peceipts',
                                                                         'interest_payable',
                                                                         'taxs_payable',
                                                                         'other_payable',
                                                                         'total_non_current_liability',
                                                                         'longterm_loan',
                                                                         'bonds_payable',
                                                                         ]):
        """
        :name: 全部投入资本(MRQ)
        :desc: 股东权益+（负债合计-无息流动负债-无息长期负债）
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)

        if len(management) <= 0:
            return None
        func = lambda x: x[0] + x[1] - (x[2] + x[3] + x[4] + x[5] + x[6] + x[7]) - (x[8] - x[9] - x[10]) \
            if x[0] is not None and \
               x[1] is not None and \
               x[2] is not None and \
               x[3] is not None and \
               x[4] is not None and \
               x[5] is not None and \
               x[6] is not None and \
               x[7] is not None and \
               x[8] is not None and \
               x[9] is not None and \
               x[10] is not None else None
        management['TotalInvestedCap'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def TotalAssets(tp_derivation, factor_derivation, dependencies=['total_assets']):
        """
        :name: 资产总计(MRQ)
        :desc: 资产总计(MRQ) balance
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'total_assets': 'TotalAssets'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def TotalFixedAssets(tp_derivation, factor_derivation, dependencies=['total_fixed_assets_liquidation']):
        """
        :name: 固定资产合计(MRQ)
        :desc: 固定资产合计(MRQ)
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'total_fixed_assets_liquidation': 'TotalFixedAssets'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def TotalLib(tp_derivation, factor_derivation, dependencies=['total_liability']):
        """
        :name: 负债合计(MRQ)
        :desc: 负债合计(MRQ)
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'total_liability': 'TotalLib'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def ShEquity(tp_derivation, factor_derivation, dependencies=['total_owner_equities']):
        """
        :name: 股东权益(MRQ)
        :desc: 股东权益(MRQ)
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'total_owner_equities': 'ShEquity'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def CashAndCashEqu(tp_derivation, factor_derivation, dependencies=['cash_and_equivalents_at_end']):
        """
        :name: 期末现金及现金等价物(MRQ)
        :desc: 期末现金及现金等价物(MRQ)
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'cash_and_equivalents_at_end': 'CashAndCashEqu'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def SalesTTM(tp_derivation, factor_derivation, dependencies=['total_operating_revenue']):
        """
        :name: 营业总收入(TTM)
        :desc: 根据截止指定日已披露的最新报告期“营业总收入”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'total_operating_revenue': 'SalesTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def TotalOptCostTTM(tp_derivation, factor_derivation, dependencies=['total_operating_cost']):
        """
        :name: 营业总成本(TTM)
        :desc: 根据截止指定日已披露的最新报告期“营业总成本”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'total_operating_cost': 'TotalOptCostTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def OptIncTTM(tp_derivation, factor_derivation, dependencies=['operating_revenue']):
        """
        :name: 营业收入(TTM)
        :desc: 根据截止指定日已披露的最新报告期“营业收入”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'operating_revenue': 'OptIncTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def GrossMarginTTM(tp_derivation, factor_derivation, dependencies=['total_operating_revenue',
                                                                       'total_operating_cost']):
        """
        :name: 毛利(TTM) 营业毛利润
        :desc: 根据截止指定日已披露的最新报告期“毛利”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)
        if len(management) <= 0:
            return None
        func = lambda x: (x[0] - x[1]) / x[1] if x[1] != 0 and x[1] is not None else None
        management['GrossMarginTTM'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)

        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def SalesExpensesTTM(tp_derivation, factor_derivation, dependencies=['sale_expense']):
        """
        :name: 销售费用(TTM)
        :desc: 根据截止指定日已披露的最新报告期“销售费用”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'SALESEsale_expenseXPE': 'SalesExpensesTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def AdmFeeTTM(tp_derivation, factor_derivation, dependencies=['administration_expense']):
        """
        :name: 管理费用(TTM)
        :desc: 根据截止指定日已披露的最新报告期“管理费用”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'administration_expense': 'AdmFeeTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def FinFeeTTM(tp_derivation, factor_derivation, dependencies=['financial_expense']):
        """
        :name: 财务费用(TTM)
        :desc: 根据截止指定日已披露的最新报告期“财务费用”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'financial_expense': 'FinFeeTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def PerFeeTTM(tp_derivation, factor_derivation, dependencies=['sale_expense',
                                                                  'administration_expense',
                                                                  'financial_expense',
                                                                  ]):
        """
        :name: 期间费用(TTM)
        :desc: 根据截止指定日已披露的最新报告期“期间费用”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)
        if len(management) <= 0:
            return None
        func = lambda x: x[0] + x[1] + x[2] if x[0] is not None and x[1] is not None and x[2] is not None else None
        management['PerFeeTTM'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def InterestExpTTM(tp_derivation, factor_derivation, dependencies=['interest_expense']):
        """
        :name: 利息支出(TTM)
        :desc: 根据截止指定日已披露的最新报告期“利息支出”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'interest_expense': 'InterestExpTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def MinorInterestTTM(tp_derivation, factor_derivation, dependencies=['minority_profit']):
        """
        :name: 少数股东损益(TTM)
        :desc: 根据截止指定日已披露的最新报告期“少数股东损益”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'minority_profit': 'MinorInterestTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def AssetImpLossTTM(tp_derivation, factor_derivation, dependencies=['asset_impairment_loss']):
        """
        :name: 资产减值损失(TTM)
        :desc: 根据截止指定日已披露的最新报告期“资产减值损失”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'asset_impairment_loss': 'AssetImpLossTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetIncFromOptActTTM(tp_derivation, factor_derivation, dependencies=['total_operating_revenue',
                                                                            'total_operating_cost']):
        """
        :name: 经营活动净收益(TTM)
        :desc: 根据截止指定日已披露的最新报告期“经营活动净收益”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)
        if len(management) <= 0:
            return None
        func = lambda x: x[0] - x[1] if x[0] is not None and x[1] is not None else None
        management['NetIncFromOptActTTM'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)

        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetIncFromValueChgTTM(tp_derivation, factor_derivation, dependencies=['fair_value_variable_income',
                                                                              'investment_income',
                                                                              'exchange_income',
                                                                              ]):
        """
        :name: 价值变动净收益(TTM)
        :desc: 公允价值变动净收益+投资净收益+汇兑净收益
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)
        if len(management) <= 0:
            return None
        func = lambda x: x[0] + x[1] + x[2] if x[0] is not None and x[1] is not None and x[2] is not None else None
        management['NetIncFromValueChgTTM'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)

        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def OptProfitTTM(tp_derivation, factor_derivation, dependencies=['operating_profit']):
        """
        :name: 营业利润(TTM)
        :desc: 根据截止指定日已披露的最新报告期“营业利润”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'operating_profit': 'OptProfitTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetNonOptIncAndExpTTM(tp_derivation, factor_derivation, dependencies=['non_operating_revenue',
                                                                              'non_operating_expense', ]):
        """
        :name: 营业外收支净额(TTM)
        :desc: 根据截止指定日已披露的最新报告期“营业外收支净额”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)
        if len(management) <= 0:
            return None
        func = lambda x: x[0] - x[1]
        management['NetNonOptIncAndExpTTM'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def EBITTTM(tp_derivation, factor_derivation, dependencies=['total_profit',
                                                                'interest_expense']):
        """
        :name: 息税前利润(TTM)
        :desc: [EBIT_反推]息税前利润 = 利润总额+利息支出
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)

        if len(management) <= 0:
            return None
        func = lambda x: x[0] + x[1]
        management['EBITTTM'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def IncTaxTTM(tp_derivation, factor_derivation, dependencies=['income_tax']):
        """
        :name: 所得税(TTM)
        :desc:根据截止指定日已披露的最新报告期“所得税”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'income_tax': 'IncTaxTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def TotalProfTTM(tp_derivation, factor_derivation, dependencies=['total_profit']):
        """
        :name: 利润总额(TTM)
        :desc: 根据截止指定日已披露的最新报告期“利润总额”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'total_profit': 'TotalProfTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetIncTTM(tp_derivation, factor_derivation, dependencies=['net_profit']):
        """
        :name: 净利润(TTM)
        :desc: 根据截止指定日已披露的最新报告期“净利润（含少数股东权益）”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'net_profit': 'NetIncTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetProfToPSTTM(tp_derivation, factor_derivation, dependencies=['np_parent_company_owners']):
        """
        :name: 归属母公司股东的净利润(TTM)
        :desc: 根据截止指定日已披露的最新报告期“归属母公司股东的净利润”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。注：交易日匹配财报数据披露日，业绩快报数据不参与计算
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'np_parent_company_owners': 'NetProfToPSTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetProfAfterNonRecGainsAndLossTTM(tp_derivation, factor_derivation, dependencies=['np_cut']):
        """
        :name: 扣除非经常性损益后的净利润(TTM)
        :desc: 根据截止指定日已披露的最新报告期“扣除非经常性损益后的净利润”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'np_cut': 'NetProfAfterNonRecGainsAndLossTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def EBITFORPTTM(tp_derivation, factor_derivation, dependencies=['operating_revenue',
                                                                    'operating_tax_surcharges',
                                                                    'operating_cost',
                                                                    'sale_expense',
                                                                    'administration_expense',
                                                                    'rd_expenses',
                                                                    'service_commission_fee',
                                                                    'asset_impairment_loss',
                                                                    'other_earnings',
                                                                    ]):
        """
        缺坏账损失， 存货跌价损失
        :name: EBIT(TTM)
        :desc: (营业收入-营业税金及附加)-(营业成本+利息支出+手续费及佣金支出+销售费用+管理费用+研发费用+坏账损失+存货跌价损失) +其他收益
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)

        if len(management) <= 0:
            return None
        func = lambda x: x[0] - x[1] - (x[2] + x[3] + x[4] + x[5] + x[6] + x[7]) + x[8] \
            if x[0] is not None and \
               x[1] is not None and \
               x[2] is not None and \
               x[3] is not None and \
               x[4] is not None and \
               x[5] is not None and \
               x[6] is not None and \
               x[7] is not None and \
               x[8] is not None else None
        management['EBITFORPTTM'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def EBITDATTM(tp_derivation, factor_derivation, dependencies=['total_profit',
                                                                  'fixed_assets_depreciation',
                                                                  'intangible_assets_amortization',
                                                                  'defferred_expense_amortization'
                                                                  ]):
        """
        :name: EBITDA(TTM)
        :desc: [EBITDA(TTM)_正向]息税前利润 + 当期计提折旧与摊销
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        management = management.fillna(0)

        if len(management) <= 0:
            return None
        func = lambda x: x[0] + x[1] + x[2] + x[3] if x[0] is not None and x[1] is not None and x[2] is not None and x[
            3] is not None else None
        management['EBITDATTM'] = management[dependencies].apply(func, axis=1)
        management = management.drop(dependencies, axis=1)
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def CashRecForSGAndPSTTM(tp_derivation, factor_derivation, dependencies=['goods_sale_and_service_render_cash']):
        """
        :name: 销售商品提供劳务收到的现金(TTM)
        :desc: 根据截止指定日已披露的最新报告期“销售商品提供劳务收到的现金”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'goods_sale_and_service_render_cash': 'CashRecForSGAndPSTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NCFOTTM(tp_derivation, factor_derivation, dependencies=['net_operate_cash_flow']):
        """
        :name: 经营活动现金净流量(TTM)
        :desc: 根据截止指定日已披露的最新报告期“经营活动现金净流量”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'net_operate_cash_flow': 'NCFOTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetCashFlowFromInvActTTM(tp_derivation, factor_derivation, dependencies=['net_invest_cash_flow']):
        """
        :name: 投资活动现金净流量(TTM)
        :desc: 根据截止指定日已披露的最新报告期“投资活动现金净流量”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'net_invest_cash_flow': 'NetCashFlowFromInvActTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetCashFlowFromFundActTTM(tp_derivation, factor_derivation, dependencies=['net_finance_cash_flow']):
        """
        :name: 筹资活动现金净流量(TTM)
        :desc: 根据截止指定日已披露的最新报告期“筹资活动现金净流量”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'net_finance_cash_flow': 'NetCashFlowFromFundActTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def NetCashFlowTTM(tp_derivation, factor_derivation, dependencies=['cash_equivalent_increase_indirect']):
        """
        :name: 现金净流量(TTM)
        :desc: 根据截止指定日已披露的最新报告期“现金及现金等价物净增加额”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'cash_equivalent_increase_indirect': 'NetCashFlowTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation

    @staticmethod
    def BusTaxAndSuchgTTM(tp_derivation, factor_derivation, dependencies=['operating_tax_surcharges']):
        """
        :name: 营业税金及附加(TTM)
        :desc: 根据截止指定日已披露的最新报告期“营业税金及附加”计算：（1）最新报告期是年报。则TTM=年报；（2）最新报告期不是年报，Q则TTM=本期+（上年年报-上年同期合并数），如果上年年报非空，本期、上年同期台并数存在空值，则返回上年年报。
        :unit: 元
        :view_dimension: 10000
        """
        management = tp_derivation.loc[:, dependencies]
        if len(management) <= 0:
            return None
        management = management.rename(columns={'operating_tax_surcharges': 'BusTaxAndSuchgTTM'})
        factor_derivation = pd.merge(factor_derivation, management, how='outer', on="security_code")
        return factor_derivation
