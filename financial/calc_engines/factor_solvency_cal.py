# -*- coding: utf-8 -*-

import pdb, importlib, inspect, time, datetime, json
# from PyFin.api import advanceDateByCalendar
# from data.polymerize import DBPolymerize
from data.storage_engine import StorageEngine
import time
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from financial import factor_solvency

# from data.model import IndicatorTTM
from data.model import IncomeTTM

from vision.db.signletion_engine import get_fin_consolidated_statements_pit, get_fundamentals, query
from vision.table.industry_daily import IndustryDaily
from vision.table.fin_cash_flow import FinCashFlow
from vision.table.fin_balance import FinBalance
from vision.table.fin_income import FinIncome
from vision.table.fin_indicator import FinIndicator

from vision.table.fin_income_ttm import FinIncomeTTM
from vision.table.fin_cash_flow_ttm import FinCashFlowTTM
from vision.table.fin_balance_ttm import FinBalanceTTM

from vision.table.valuation import Valuation
from utilities.sync_util import SyncUtil

# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
# from ultron.cluster.invoke.cache_data import cache_data


class CalcEngine(object):
    def __init__(self, name, url, methods=[{'packet': 'financial.factor_solvency', 'class': 'FactorSolvency'}, ]):
        self._name = name
        self._methods = methods
        self._url = url

    def get_trade_date(self, trade_date, n, days=365):
        """
        获取当前时间前n年的时间点，且为交易日，如果非交易日，则往前提取最近的一天。
        :param days:
        :param trade_date: 当前交易日
        :param n:
        :return:
        """
        syn_util = SyncUtil()
        trade_date_sets = syn_util.get_all_trades('001002', '19900101', trade_date)
        trade_date_sets = trade_date_sets['TRADEDATE'].values

        time_array = datetime.strptime(str(trade_date), "%Y%m%d")
        time_array = time_array - timedelta(days=days) * n
        date_time = int(datetime.strftime(time_array, "%Y%m%d"))
        if str(date_time) < min(trade_date_sets):
            # print('date_time %s is out of trade_date_sets' % date_time)
            return str(date_time)
        else:
            while str(date_time) not in trade_date_sets:
                date_time = date_time - 1
            # print('trade_date pre %s year %s' % (n, date_time))
            return str(date_time)

    def _func_sets(self, method):
        # 私有函数和保护函数过滤
        return list(filter(lambda x: not x.startswith('_') and callable(getattr(method, x)), dir(method)))

    def loading_data(self, trade_date):
        """
        获取基础数据
        按天获取当天交易日所有股票的基础数据
        :param trade_date: 交易日
        :return:
        """
        # 转换时间格式
        time_array = datetime.strptime(trade_date, "%Y-%m-%d")
        trade_date = datetime.strftime(time_array, '%Y%m%d')
        # 读取目前涉及到的因子
        columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']
        # MRQ data
        balance_mrq_sets = get_fin_consolidated_statements_pit(FinBalance,
                                                               [FinBalance.bonds_payable,
                                                                FinBalance.total_assets,  # 资产总计
                                                                FinBalance.total_non_current_liability,  # 非流动负债合计
                                                                FinBalance.total_current_assets,  # 流动资产合计
                                                                FinBalance.total_current_liability,  # 流动负债合计
                                                                FinBalance.total_liability,  # 负债合计
                                                                FinBalance.fixed_assets_netbook,  # 固定资产
                                                                FinBalance.equities_parent_company_owners,
                                                                # 归属于母公司股东权益合计
                                                                FinBalance.shortterm_loan,  # 短期借款
                                                                FinBalance.non_current_liability_in_one_year,
                                                                # 一年内到期的非流动负债
                                                                FinBalance.longterm_loan,  # 长期借款
                                                                FinBalance.interest_payable,  # 应付债券
                                                                FinBalance.total_owner_equities,  # 所有者权益（或股东权益）合计
                                                                FinBalance.inventories,  # 存货
                                                                FinBalance.intangible_assets,  # 无形资产
                                                                FinBalance.development_expenditure,  # 开发支出
                                                                FinBalance.good_will,  # 商誉
                                                                FinBalance.long_deferred_expense,  # 长期待摊费用
                                                                FinBalance.deferred_tax_assets,  # 递延所得税资产
                                                                FinBalance.cash_equivalents,  # 货币资金
                                                                FinBalance.trading_assets,  # 交易性金融资产
                                                                FinBalance.bill_receivable,  # 应收票据
                                                                FinBalance.account_receivable,  # 应收账款
                                                                FinBalance.other_receivable,  # 其他应收款
                                                                FinBalance.total_non_current_assets,  # 非流动资产合计
                                                                ], dates=[trade_date])
        for col in columns:
            if col in list(balance_mrq_sets.keys()):
                balance_mrq_sets = balance_mrq_sets.drop(col, axis=1)

        balance_mrq_sets = balance_mrq_sets.rename(columns={
            'total_liability': 'total_liability',  # 负债合计
            'total_assets': 'total_assets',  # 资产总计
            'total_current_liability': 'total_current_liability',  # 流动负债合计
            'total_current_assets': 'total_current_assets',  # 流动资产合计
            'inventories': 'inventories',  # 存货
            'cash_equivalents': 'cash_equivalents',  # 货币资金
            'trading_assets': 'trading_assets',  # 交易性金融资产
            'bill_receivable': 'bill_receivable',  # 应收票据
            'account_receivable': 'account_receivable',  # 应收账款
            'other_receivable': 'other_receivable',  # 其他应收款
            'equities_parent_company_owners': 'equities_parent_company_owners',  # 归属于母公司股东权益合计
            'intangible_assets': 'intangible_assets',  # 无形资产
            'development_expenditure': 'development_expenditure',  # 开发支出
            'good_will': 'good_will',  # 商誉
            'long_deferred_expense': 'long_deferred_expense',  # 长期待摊费用
            'deferred_tax_assets': 'deferred_tax_assets',  # 递延所得税资产
            'non_current_liability_in_one_year': 'non_current_liability_in_one_year',  # 一年内到期的非流动负债
            'shortterm_loan': 'shortterm_loan',  # 短期借款
            'longterm_loan': 'longterm_loan',  # 长期借款
            'bonds_payable': 'bonds_payable',  # 应付债券
            'interest_payable': 'interest_payable',  # 应付利息
            'total_non_current_liability': 'total_non_current_liability',  # 非流动负债合计
            'total_non_current_assets': 'total_non_current_assets',  # 非流动资产合计
            'fixed_assets_netbook': 'fixed_assets',  # 固定资产
            'total_owner_equities': 'total_owner_equities',  # 所有者权益（或股东权益）合计
        })
        cash_flow_mrq_sets = get_fin_consolidated_statements_pit(FinCashFlow,
                                                                 [FinCashFlow.net_operate_cash_flow,
                                                                  ], dates=[trade_date])
        for col in columns:
            if col in list(cash_flow_mrq_sets.keys()):
                cash_flow_mrq_sets = cash_flow_mrq_sets.drop(col, axis=1)
        cash_flow_mrq_sets = cash_flow_mrq_sets.rename(
            columns={'net_operate_cash_flow': 'net_operate_cash_flow_mrq',  # 经营活动现金流量净额
                     })

        mrq_solvency = pd.merge(cash_flow_mrq_sets, balance_mrq_sets, on='security_code')

        # ttm data
        income_ttm_sets = get_fin_consolidated_statements_pit(FinIncomeTTM,
                                                              [FinIncomeTTM.total_profit,  # 利润总额
                                                               FinIncomeTTM.financial_expense,  # 财务费用
                                                               FinIncomeTTM.interest_income,  # 利息收入
                                                               ], dates=[trade_date])
        for col in columns:
            if col in list(income_ttm_sets.keys()):
                income_ttm_sets = income_ttm_sets.drop(col, axis=1)
        income_ttm_sets = income_ttm_sets.rename(columns={'total_profit': 'total_profit',  # 利润总额
                                                          'financial_expense': 'financial_expense',  # 财务费用
                                                          'interest_income': 'interest_income',  # 利息收入
                                                          })

        balance_ttm_sets = get_fin_consolidated_statements_pit(FinBalanceTTM,
                                                               [FinBalanceTTM.total_current_liability,  # 流动负债合计
                                                                FinBalanceTTM.non_current_liability_in_one_year,
                                                                # 一年内到期的非流动负债
                                                                ], dates=[trade_date])
        for col in columns:
            if col in list(balance_ttm_sets.keys()):
                balance_ttm_sets = balance_ttm_sets.drop(col, axis=1)
        balance_ttm_sets = balance_ttm_sets.rename(columns={
            'total_current_liability': 'total_current_liability_ttm',  # 流动负债合计
            'non_current_liability_in_one_year': 'non_current_liability_in_one_year_ttm',  # 一年内到期的非流动负债
        })

        cash_flow_ttm_sets = get_fin_consolidated_statements_pit(FinCashFlowTTM,
                                                                 [FinCashFlowTTM.net_operate_cash_flow,
                                                                  # 经营活动现金流量净额
                                                                  FinCashFlowTTM.cash_and_equivalents_at_end,
                                                                  # 期末现金及现金等价物余额
                                                                  ], dates=[trade_date])
        for col in columns:
            if col in list(cash_flow_ttm_sets.keys()):
                cash_flow_ttm_sets = cash_flow_ttm_sets.drop(col, axis=1)
        cash_flow_ttm_sets = cash_flow_ttm_sets.rename(columns={
            'net_operate_cash_flow': 'net_operate_cash_flow',  # 经营活动现金流量净额
            'cash_and_equivalents_at_end': 'cash_and_equivalents_at_end',  # 期末现金及现金等价物余额
        })

        # indicator_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorTTM,
        #                                                                      [IndicatorTTM.NDEBT,
        #                                                                       ], dates=[trade_date])
        # for col in columns:
        #     if col in list(indicator_ttm_sets.keys()):
        #         indicator_ttm_sets = indicator_ttm_sets.drop(col, axis=1)
        # indicator_ttm_sets = indicator_ttm_sets.rename(columns={'NDEBT': 'net_liability',  # 净负债
        #                                                         })

        ttm_solvency = pd.merge(balance_ttm_sets, cash_flow_ttm_sets, how='outer', on="security_code")
        ttm_solvency = pd.merge(ttm_solvency, income_ttm_sets, how='outer', on="security_code")
        # ttm_solvency = pd.merge(ttm_solvency, indicator_ttm_sets, how='outer', on="security_code")

        column = ['trade_date']
        valuation_sets = get_fundamentals(query(Valuation.security_code,
                                                Valuation.trade_date,
                                                Valuation.market_cap, )
                                          .filter(Valuation.trade_date.in_([trade_date])))
        for col in column:
            if col in list(valuation_sets.keys()):
                valuation_sets = valuation_sets.drop(col, axis=1)

        tp_solvency = pd.merge(ttm_solvency, valuation_sets, how='outer', on='security_code')
        tp_solvency = pd.merge(tp_solvency, mrq_solvency, how='outer', on='security_code')
        return tp_solvency

    def process_calc_factor(self, trade_date, tp_solvency):
        tp_solvency = tp_solvency.set_index('security_code')
        solvency = factor_solvency.FactorSolvency()

        # 读取目前涉及到的因子
        solvency_sets = pd.DataFrame()
        solvency_sets['security_code'] = tp_solvency.index
        solvency_sets = solvency_sets.set_index('security_code')

        # MRQ计算
        solvency_sets = solvency.BondsToAsset(tp_solvency, solvency_sets)
        solvency_sets = solvency.BookLev(tp_solvency, solvency_sets)
        solvency_sets = solvency.CurrentRatio(tp_solvency, solvency_sets)
        solvency_sets = solvency.DA(tp_solvency, solvency_sets)
        solvency_sets = solvency.DTE(tp_solvency, solvency_sets)
        solvency_sets = solvency.EquityRatio(tp_solvency, solvency_sets)
        solvency_sets = solvency.EquityPCToIBDebt(tp_solvency, solvency_sets)
        solvency_sets = solvency.EquityPCToTCap(tp_solvency, solvency_sets)
        solvency_sets = solvency.IntBDToCap(tp_solvency, solvency_sets)
        solvency_sets = solvency.LDebtToWCap(tp_solvency, solvency_sets)
        solvency_sets = solvency.MktLev(tp_solvency, solvency_sets)
        solvency_sets = solvency.QuickRatio(tp_solvency, solvency_sets)
        solvency_sets = solvency.SupQuickRatio(tp_solvency, solvency_sets)
        solvency_sets = solvency.TNWorthToIBDebt(tp_solvency, solvency_sets)
        solvency_sets = solvency.TNWorthToNDebt(tp_solvency, solvency_sets)
        solvency_sets = solvency.OPCToDebt(tp_solvency, solvency_sets)
        solvency_sets = solvency.OptCFToCurrLiability(tp_solvency, solvency_sets)

        # TTM计算
        solvency_sets = solvency.InterestCovTTM(tp_solvency, solvency_sets)
        solvency_sets = solvency.OptCFToLiabilityTTM(tp_solvency, solvency_sets)
        solvency_sets = solvency.OptCFToIBDTTM(tp_solvency, solvency_sets)
        solvency_sets = solvency.OptCFToNetDebtTTM(tp_solvency, solvency_sets)
        solvency_sets = solvency.OPCToDebtTTM(tp_solvency, solvency_sets)
        solvency_sets = solvency.CashRatioTTM(tp_solvency, solvency_sets)
        solvency_sets = solvency_sets.reset_index()
        solvency_sets['trade_date'] = str(trade_date)
        solvency_sets.replace([-np.inf, np.inf, None], np.nan, inplace=True)
        return solvency_sets

    def local_run(self, trade_date):
        print('当前交易日: %s' % trade_date)
        tic = time.time()
        tp_solvency = self.loading_data(trade_date)
        print('data load time %s' % (time.time() - tic))

        storage_engine = StorageEngine(self._url)
        result = self.process_calc_factor(trade_date, tp_solvency)
        print('cal_time %s' % (time.time() - tic))
        storage_engine.update_destdb(str(self._methods[-1]['packet'].split('.')[-1]), trade_date, result)
        # storage_engine.update_destdb('factor_solvency', trade_date, result)

    # def remote_run(self, trade_date):
    #     total_data = self.loading_data(trade_date)
    #     #存储数据
    #     session = str(int(time.time() * 1000000 + datetime.datetime.now().microsecond))
    #     cache_data.set_cache(session, 'alphax', total_data.to_json(orient='records'))
    #     distributed_factor.delay(session, json.dumps(self._methods), self._name)
    #
    # def distributed_factor(self, total_data):
    #     mkt_df = self.calc_factor_by_date(total_data,trade_date)
    #     result = self.calc_factor('alphax.alpha191','Alpha191',mkt_df,trade_date)

# @app.task
# def distributed_factor(session, trade_date, packet_sets, name):
#     calc_engines = CalcEngine(name, packet_sets)
#     content = cache_data.get_cache(session, factor_name)
#     total_data = json_normalize(json.loads(content))
#     calc_engines.distributed_factor(total_data)
#

# # @app.task()
# def factor_calculate(**kwargs):
#     print("solvency_kwargs: {}".format(kwargs))
#     date_index = kwargs['date_index']
#     session = kwargs['session']
#     content1 = cache_data.get_cache(session + str(date_index) + "1", date_index)
#     tp_solvency = json_normalize(json.loads(str(content1, encoding='utf8')))
#     tp_solvency.set_index('security_code', inplace=True)
#     print("len_tp_cash_flow_data {}".format(len(tp_solvency)))
#     calculate(date_index, tp_solvency)
