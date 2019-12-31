# -*- coding: utf-8 -*-

import pdb, importlib, inspect, time, datetime, json
# from PyFin.api import advanceDateByCalendar
# from data.polymerize import DBPolymerize
from data.storage_engine import StorageEngine
import time
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from financial import factor_per_share_indicators

from data.model import BalanceMRQ, BalanceTTM, FinBalance
from data.model import FinCashFlowTTM, FinCashFlow
from data.model import FinIndicator
from data.model import FinIncome, FinIncomeTTM

from vision.db.signletion_engine import get_fin_consolidated_statements_pit, get_fundamentals, query
from vision.table.industry_daily import IndustryDaily
from vision.table.fin_cash_flow import FinCashFlow
from vision.table.fin_balance import FinBalance
from vision.table.fin_income import FinIncome
from vision.table.fin_indicator import FinIndicator

from vision.table.valuation import Valuation
from utilities.sync_util import SyncUtil

# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)


# from ultron.cluster.invoke.cache_data import cache_data


class CalcEngine(object):
    def __init__(self, name, url,
                 methods=[{'packet': 'financial.factor_pre_share_indicators', 'class': 'FactorPerShareIndicators'}, ]):
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
        # Report data
        cash_flow_sets = get_fin_consolidated_statements_pit(FinCashFlow,
                                                             [FinCashFlow.cash_and_equivalents_at_end,  # 期末现金及现金等价物余额
                                                              ], dates=[trade_date])
        for col in columns:
            if col in list(cash_flow_sets.keys()):
                cash_flow_sets = cash_flow_sets.drop(col, axis=1)
        cash_flow_sets = cash_flow_sets.rename(
            columns={'cash_and_equivalents_at_end': 'cash_and_equivalents_at_end',  # 期末现金及现金等价物余额
                     })

        income_sets = get_fin_consolidated_statements_pit(FinIncome,
                                                          [FinIncome.operating_revenue,  # 营业收入
                                                           FinIncome.total_operating_revenue,  # 营业总收入
                                                           FinIncome.operating_profit,  # 营业利润
                                                           FinIncome.diluted_eps,  # 稀释每股收益
                                                           ], dates=[trade_date])
        for col in columns:
            if col in list(income_sets.keys()):
                income_sets = income_sets.drop(col, axis=1)
        income_sets = income_sets.rename(columns={'operating_revenue': 'operating_revenue',  # 营业收入
                                                  'total_operating_revenue': 'total_operating_revenue',  # 营业总收入
                                                  'operating_profit': 'operating_profit',  # 营业利润
                                                  'diluted_eps': 'diluted_eps',  # 稀释每股收益
                                                  })
        balance_sets = get_fin_consolidated_statements_pit(FinBalance,
                                                           [FinBalance.equities_parent_company_owners,  # 归属于母公司的所有者权益
                                                            FinBalance.capital_reserve_fund,
                                                            FinBalance.surplus_reserve_fund,
                                                            FinBalance.retained_profit,
                                                            ], dates=[trade_date])
        for col in columns:
            if col in list(balance_sets.keys()):
                balance_sets = balance_sets.drop(col, axis=1)
        balance_sets = balance_sets.rename(
            columns={'equities_parent_company_owners': 'total_owner_equities',  # 归属于母公司的所有者权益
                     'capital_reserve_fund': 'capital_reserve_fund',  # 资本公积
                     'surplus_reserve_fund': 'surplus_reserve_fund',  # 盈余公积
                     'retained_profit': 'retained_profit',  # 未分配利润
                     })

        indicator_sets = get_fin_consolidated_statements_pit(FinIndicator,
                                                             [
                                                                 # FinIndicator.FCFE,  # 股东自由现金流量
                                                                 # FinIndicator.FCFF,  # 企业自由现金流量
                                                                 FinIndicator.eps_basic,  # 基本每股收益
                                                                 # FinIndicator.DPS,  # 每股股利（税前）
                                                             ], dates=[trade_date])
        for col in columns:
            if col in list(indicator_sets.keys()):
                indicator_sets = indicator_sets.drop(col, axis=1)
        indicator_sets = indicator_sets.rename(columns={
            # 'FCFE': 'shareholder_fcfps',  # 股东自由现金流量
            # 'FCFF': 'enterprise_fcfps',  # 企业自由现金流量
            'eps_basic': 'basic_eps',  # 基本每股收益
            # 'DPS': 'dividend_receivable',  # 每股股利（税前）
        })

        # TTM data
        cash_flow_ttm_sets = get_fin_consolidated_statements_pit(FinCashFlowTTM,
                                                                 [FinCashFlowTTM.cash_equivalent_increase_indirect,
                                                                  # 现金及现金等价物净增加额
                                                                  FinCashFlowTTM.net_operate_cash_flow,  # 经营活动现金流量净额
                                                                  ], dates=[trade_date])
        for col in columns:
            if col in list(cash_flow_ttm_sets.keys()):
                cash_flow_ttm_sets = cash_flow_ttm_sets.drop(col, axis=1)
        cash_flow_ttm_sets = cash_flow_ttm_sets.rename(
            columns={'cash_equivalent_increase_indirect': 'cash_equivalent_increase_ttm',  # 现金及现金等价物净增加额
                     'net_operate_cash_flow': 'net_operate_cash_flow_ttm',  # 经营活动现金流量净额
                     })

        income_ttm_sets = get_fin_consolidated_statements_pit(FinIncomeTTM,
                                                              [FinIncomeTTM.np_parent_company_owners,  # 归属于母公司所有者的净利润
                                                               FinIncomeTTM.operating_profit,  # 营业利润
                                                               FinIncomeTTM.operating_revenue,  # 营业收入
                                                               FinIncomeTTM.total_operating_revenue,  # 营业总收入
                                                               ], dates=[trade_date])
        for col in columns:
            if col in list(income_ttm_sets.keys()):
                income_ttm_sets = income_ttm_sets.drop(col, axis=1)
        income_ttm_sets = income_ttm_sets.rename(
            columns={'np_parent_company_owners': 'np_parent_company_owners_ttm',  # 归属于母公司所有者的净利润
                     'operating_profit': 'operating_profit_ttm',  # 营业利润
                     'operating_revenue': 'operating_revenue_ttm',  # 营业收入
                     'total_operating_revenue': 'total_operating_revenue_ttm',  # 营业总收入
                     })

        column = ['trade_date']
        valuation_data = get_fundamentals(query(Valuation.security_code,
                                                Valuation.trade_date,
                                                Valuation.capitalization,
                                                ).filter(Valuation.trade_date.in_([trade_date])))
        for col in column:
            if col in list(valuation_data.keys()):
                valuation_data = valuation_data.drop(col, axis=1)

        valuation_sets = pd.merge(cash_flow_sets, income_sets, on='security_code').reindex()
        valuation_sets = pd.merge(balance_sets, valuation_sets, on='security_code').reindex()
        valuation_sets = pd.merge(indicator_sets, valuation_sets, on='security_code').reindex()
        valuation_sets = pd.merge(cash_flow_ttm_sets, valuation_sets, on='security_code').reindex()
        valuation_sets = pd.merge(income_ttm_sets, valuation_sets, on='security_code').reindex()
        valuation_sets = pd.merge(valuation_data, valuation_sets, on='security_code').reindex()

        return valuation_sets

    def process_calc_factor(self, trade_date, valuation_sets):
        per_share = factor_per_share_indicators.FactorPerShareIndicators()
        factor_share_indicators = pd.DataFrame()
        factor_share_indicators['security_code'] = valuation_sets['security_code']
        valuation_sets = valuation_sets.set_index('security_code')
        factor_share_indicators = factor_share_indicators.set_index('security_code')

        factor_share_indicators = per_share.EPS(valuation_sets, factor_share_indicators)
        factor_share_indicators = per_share.DilutedEPSTTM(valuation_sets, factor_share_indicators)
        factor_share_indicators = per_share.CashEquPS(valuation_sets, factor_share_indicators)
        # factor_share_indicators = per_share.DivPS(valuation_sets, factor_share_indicators)
        factor_share_indicators = per_share.EPSTTM(valuation_sets, factor_share_indicators)
        factor_share_indicators = per_share.NetAssetPS(valuation_sets, factor_share_indicators)
        factor_share_indicators = per_share.TotalRevPSTTM(valuation_sets, factor_share_indicators)
        factor_share_indicators = per_share.TotalRevPS(valuation_sets, factor_share_indicators)
        factor_share_indicators = per_share.OptRevPSTTM(valuation_sets, factor_share_indicators)
        factor_share_indicators = per_share.OptRevPS(valuation_sets, factor_share_indicators)
        factor_share_indicators = per_share.OptProfitPSTTM(valuation_sets, factor_share_indicators)
        factor_share_indicators = per_share.OptProfitPS(valuation_sets, factor_share_indicators)
        factor_share_indicators = per_share.CapReservesPS(valuation_sets, factor_share_indicators)
        factor_share_indicators = per_share.SurplusReservePS(valuation_sets, factor_share_indicators)
        factor_share_indicators = per_share.UndividedProfitPS(valuation_sets, factor_share_indicators)
        factor_share_indicators = per_share.RetainedEarningsPS(factor_share_indicators, factor_share_indicators)
        factor_share_indicators = per_share.OptCFPSTTM(valuation_sets, factor_share_indicators)
        factor_share_indicators = per_share.CFPSTTM(valuation_sets, factor_share_indicators)
        factor_share_indicators = per_share.EnterpriseFCFPS(valuation_sets, factor_share_indicators)
        factor_share_indicators = per_share.ShareholderFCFPS(valuation_sets, factor_share_indicators)

        factor_share_indicators = factor_share_indicators.reset_index()
        factor_share_indicators['trade_date'] = str(trade_date)
        factor_share_indicators.replace([-np.inf, np.inf, None], np.nan, inplace=True)
        return factor_share_indicators

    def local_run(self, trade_date):
        print('当前交易日: %s' % trade_date)
        tic = time.time()
        valuation_sets = self.loading_data(trade_date)
        print('data load time %s' % (time.time() - tic))

        storage_engine = StorageEngine(self._url)
        result = self.process_calc_factor(trade_date, valuation_sets)
        print('cal_time %s' % (time.time() - tic))
        storage_engine.update_destdb(str(self._methods[-1]['packet'].split('.')[-1]), trade_date, result)
        # storage_engine.update_destdb('factor_pre_share_indicators', trade_date, result)

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
#     print("per_share_kwargs: {}".format(kwargs))
#     date_index = kwargs['date_index']
#     session = kwargs['session']
#     content = cache_data.get_cache(session + str(date_index), date_index)
#     total_pre_share_data = json_normalize(json.loads(str(content, encoding='utf8')))
#     print("len_total_per_share_data {}".format(len(total_pre_share_data)))
#     calculate(date_index, total_pre_share_data)
