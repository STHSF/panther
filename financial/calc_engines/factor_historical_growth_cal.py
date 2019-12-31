# -*- coding: utf-8 -*-

import pdb, importlib, inspect, time, datetime, json
# from PyFin.api import advanceDateByCalendar
# from data.polymerize import DBPolymerize
from data.storage_engine import StorageEngine
import time
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from financial import factor_historical_growth

from vision.db.signletion_engine import get_fin_consolidated_statements_pit, get_fundamentals, query
from vision.table.industry_daily import IndustryDaily
from vision.table.fin_cash_flow import FinCashFlow
from vision.table.fin_balance import FinBalance
from vision.table.fin_income import FinIncome
from vision.table.fin_indicator import FinIndicator

from vision.table.fin_balance_ttm import FinBalanceTTM
from vision.table.fin_indicator_ttm import FinIndicatorTTM
from vision.table.fin_income_ttm import FinIncomeTTM
from vision.table.fin_cash_flow_ttm import FinCashFlowTTM

from utilities.sync_util import SyncUtil

# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
# from ultron.cluster.invoke.cache_data import cache_data


class CalcEngine(object):
    def __init__(self, name, url,
                 methods=[{'packet': 'financial.factor_historical_growth', 'class': 'FactorHistoricalGrowth'}, ]):
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
        trade_date_pre_year = self.get_trade_date(trade_date, 1)
        trade_date_pre_year_2 = self.get_trade_date(trade_date, 2)
        trade_date_pre_year_3 = self.get_trade_date(trade_date, 3)
        trade_date_pre_year_4 = self.get_trade_date(trade_date, 4)
        trade_date_pre_year_5 = self.get_trade_date(trade_date, 5)

        columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']
        # report data
        balance_sets = get_fin_consolidated_statements_pit(FinBalance,
                                                           [FinBalance.total_assets,  # 总资产（资产合计）
                                                            FinBalance.total_owner_equities,  # 股东权益合计
                                                            ], dates=[trade_date])

        if len(balance_sets) <= 0 or balance_sets is None:
            balance_sets = pd.DataFrame({'security_code': [], 'total_assets': [], 'total_owner_equities': []})

        for column in columns:
            if column in list(balance_sets.keys()):
                balance_sets = balance_sets.drop(column, axis=1)
        balance_sets = balance_sets.rename(columns={'total_assets': 'total_assets',  # 资产总计
                                                    'total_owner_equities': 'total_owner_equities',  # 股东权益合计
                                                    })

        balance_sets_pre_year = get_fin_consolidated_statements_pit(FinBalance,
                                                                    [FinBalance.total_assets,  # 总资产（资产合计）
                                                                     FinBalance.total_owner_equities,  # 股东权益合计
                                                                     ], dates=[trade_date_pre_year])
        if len(balance_sets_pre_year) <= 0 or balance_sets_pre_year is None:
            balance_sets_pre_year = pd.DataFrame({'security_code': [], 'total_assets': [], 'total_owner_equities': []})

        for column in columns:
            if column in list(balance_sets_pre_year.keys()):
                balance_sets_pre_year = balance_sets_pre_year.drop(column, axis=1)
        balance_sets_pre_year = balance_sets_pre_year.rename(columns={"total_assets": "total_assets_pre_year",
                                                                      "total_owner_equities": "total_owner_equities_pre_year"})

        balance_sets = pd.merge(balance_sets, balance_sets_pre_year, on='security_code')
        # print('get_balabce_sets')

        # ttm 计算
        ttm_factor_sets = get_fin_consolidated_statements_pit(FinIncomeTTM,
                                                              [FinIncomeTTM.operating_revenue,  # 营业收入
                                                               FinIncomeTTM.operating_profit,  # 营业利润
                                                               FinIncomeTTM.total_profit,  # 利润总额
                                                               FinIncomeTTM.net_profit,  # 净利润
                                                               FinIncomeTTM.operating_cost,  # 营业成本
                                                               FinIncomeTTM.np_parent_company_owners],  # 归属于母公司所有者的净利润
                                                              dates=[trade_date])

        if len(ttm_factor_sets) <= 0 or ttm_factor_sets is None:
            ttm_factor_sets = pd.DataFrame(
                {'security_code': [], 'operating_revenue': [], 'operating_profit': [], 'total_profit': [],
                 'net_profit': [], 'operating_cost': [], 'np_parent_company_owners': []})

        for column in columns:
            if column in list(ttm_factor_sets.keys()):
                ttm_factor_sets = ttm_factor_sets.drop(column, axis=1)

        ttm_cash_flow_sets = get_fin_consolidated_statements_pit(FinCashFlowTTM,
                                                                 [FinCashFlowTTM.net_finance_cash_flow,  # 筹资活动产生的现金流量净额
                                                                  FinCashFlowTTM.net_operate_cash_flow,  # 经营活动产生的现金流量净额
                                                                  FinCashFlowTTM.net_invest_cash_flow,  # 投资活动产生的现金流量净额
                                                                  FinCashFlowTTM.cash_equivalent_increase_indirect,
                                                                  # 现金及现金等价物的净增加额
                                                                  ], dates=[trade_date])

        if len(ttm_cash_flow_sets) <= 0 or ttm_cash_flow_sets is None:
            ttm_cash_flow_sets = pd.DataFrame(
                {'security_code': [], 'net_finance_cash_flow': [], 'net_operate_cash_flow': [],
                 'net_invest_cash_flow': [], 'cash_equivalent_increase_indirect': []})

        for column in columns:
            if column in list(ttm_cash_flow_sets.keys()):
                ttm_cash_flow_sets = ttm_cash_flow_sets.drop(column, axis=1)
        ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_cash_flow_sets, how='outer', on='security_code')

        ttm_indicator_sets = get_fin_consolidated_statements_pit(FinIndicatorTTM,
                                                                 [FinIndicatorTTM.np_cut,
                                                                  ], dates=[trade_date])

        if len(ttm_indicator_sets) <= 0 or ttm_indicator_sets is None:
            ttm_indicator_sets = pd.DataFrame({'security_code': [], 'np_cut': []})

        for column in columns:
            if column in list(ttm_indicator_sets.keys()):
                ttm_indicator_sets = ttm_indicator_sets.drop(column, axis=1)

        ttm_indicator_sets = ttm_indicator_sets.rename(columns={'np_cut': 'ni_attr_p_cut'})
        ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_indicator_sets, how='outer', on='security_code')
        # field_key = ttm_cash_flow_sets.keys()
        # for i in field_key:
        #     ttm_factor_sets[i] = ttm_cash_flow_sets[i]

        ttm_factor_sets = ttm_factor_sets.rename(
            columns={"operating_revenue": "operating_revenue",
                     "operating_profit": "operating_profit",
                     "total_profit": "total_profit",
                     "net_profit": "net_profit",
                     "operating_cost": "operating_cost",
                     "np_parent_company_owners": "np_parent_company_owners",
                     "net_finance_cash_flow": "net_finance_cash_flow",
                     "net_operate_cash_flow": "net_operate_cash_flow",
                     "net_invest_cash_flow": "net_invest_cash_flow",
                     'cash_equivalent_increase_indirect': 'n_change_in_cash'
                     })
        ttm_income_sets_pre = get_fin_consolidated_statements_pit(FinIncomeTTM,
                                                                  [FinIncomeTTM.operating_revenue,  # 营业收入
                                                                   FinIncomeTTM.operating_profit,  # 营业利润
                                                                   FinIncomeTTM.total_profit,  # 利润总额
                                                                   FinIncomeTTM.net_profit,  # 净利润
                                                                   FinIncomeTTM.operating_cost,  # 营业成本
                                                                   FinIncomeTTM.np_parent_company_owners  # 归属于母公司所有者的净利润
                                                                   ], dates=[trade_date_pre_year])

        if len(ttm_income_sets_pre) <= 0 or ttm_income_sets_pre is None:
            ttm_income_sets_pre = pd.DataFrame(
                {'security_code': [], 'operating_revenue': [], 'operating_profit': [], 'total_profit': [],
                 'net_profit': [], 'operating_cost': [], 'np_parent_company_owners': []})

        for column in columns:
            if column in list(ttm_income_sets_pre.keys()):
                ttm_income_sets_pre = ttm_income_sets_pre.drop(column, axis=1)

        ttm_factor_sets_pre = ttm_income_sets_pre.rename(
            columns={"operating_revenue": "operating_revenue_pre_year",
                     "operating_profit": "operating_profit_pre_year",
                     "total_profit": "total_profit_pre_year",
                     "net_profit": "net_profit_pre_year",
                     "operating_cost": "operating_cost_pre_year",
                     "np_parent_company_owners": "np_parent_company_owners_pre_year",
                     })
        ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre, how='outer', on='security_code')

        ttm_indicator_sets_pre = get_fin_consolidated_statements_pit(FinIndicatorTTM,
                                                                     [FinIndicatorTTM.np_cut,
                                                                      ], dates=[trade_date_pre_year])

        if len(ttm_indicator_sets_pre) <= 0 or ttm_indicator_sets_pre is None:
            ttm_indicator_sets_pre = pd.DataFrame({'security_code': [], 'np_cut': []})

        for column in columns:
            if column in list(ttm_indicator_sets_pre.keys()):
                ttm_indicator_sets_pre = ttm_indicator_sets_pre.drop(column, axis=1)
        ttm_indicator_sets_pre = ttm_indicator_sets_pre.rename(columns={'np_cut': 'ni_attr_p_cut_pre'})
        ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_indicator_sets_pre, how='outer', on='security_code')

        ttm_cash_flow_sets_pre = get_fin_consolidated_statements_pit(FinCashFlowTTM,
                                                                     [FinCashFlowTTM.net_finance_cash_flow,
                                                                      # 筹资活动产生的现金流量净额
                                                                      FinCashFlowTTM.net_operate_cash_flow,
                                                                      # 经营活动产生的现金流量净额
                                                                      FinCashFlowTTM.net_invest_cash_flow,
                                                                      # 投资活动产生的现金流量净额
                                                                      FinCashFlowTTM.cash_equivalent_increase_indirect,
                                                                      # 现金及现金等价物的净增加额
                                                                      ], dates=[trade_date_pre_year])

        if len(ttm_cash_flow_sets_pre) <= 0 or ttm_cash_flow_sets_pre is None:
            ttm_cash_flow_sets_pre = pd.DataFrame({'security_code': [], 'net_finance_cash_flow': [],
                                                   'net_operate_cash_flow': [], 'net_invest_cash_flow': [],
                                                   'cash_equivalent_increase_indirect': []})

        for column in columns:
            if column in list(ttm_cash_flow_sets_pre.keys()):
                ttm_cash_flow_sets_pre = ttm_cash_flow_sets_pre.drop(column, axis=1)

        ttm_cash_flow_sets_pre = ttm_cash_flow_sets_pre.rename(
            columns={"net_finance_cash_flow": "net_finance_cash_flow_pre_year",
                     "net_operate_cash_flow": "net_operate_cash_flow_pre_year",
                     "net_invest_cash_flow": "net_invest_cash_flow_pre_year",
                     'cash_equivalent_increase_indirect': 'n_change_in_cash_pre_year',
                     })

        ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_cash_flow_sets_pre, how='outer', on='security_code')
        # print('get_ttm_factor_sets_pre')

        # ttm 连续
        ttm_factor_sets_pre_year_2 = get_fin_consolidated_statements_pit(FinIncomeTTM,
                                                                         [FinIncomeTTM.net_profit,
                                                                          FinIncomeTTM.operating_revenue,
                                                                          FinIncomeTTM.operating_cost,
                                                                          ], dates=[trade_date_pre_year_2])

        if len(ttm_factor_sets_pre_year_2) <= 0 or ttm_factor_sets_pre_year_2 is None:
            ttm_factor_sets_pre_year_2 = pd.DataFrame(
                {'security_code': [], 'net_profit': [], 'operating_revenue': [], 'operating_cost': []})

        for column in columns:
            if column in list(ttm_factor_sets_pre_year_2.keys()):
                ttm_factor_sets_pre_year_2 = ttm_factor_sets_pre_year_2.drop(column, axis=1)

        ttm_factor_sets_pre_year_2 = ttm_factor_sets_pre_year_2.rename(
            columns={"operating_revenue": "operating_revenue_pre_year_2",
                     "operating_cost": "operating_cost_pre_year_2",
                     "net_profit": "net_profit_pre_year_2",
                     })
        ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year_2, how='outer', on="security_code")
        # field_key = ttm_factor_sets_pre_year_2.keys()
        # for i in field_key:
        #     ttm_factor_sets[i] = ttm_factor_sets_pre_year_2[i]
        # print('get_ttm_factor_sets_2')

        ttm_factor_sets_pre_year_3 = get_fin_consolidated_statements_pit(FinIncomeTTM,
                                                                         [FinIncomeTTM.net_profit,
                                                                          FinIncomeTTM.operating_revenue,
                                                                          FinIncomeTTM.operating_cost,
                                                                          ], dates=[trade_date_pre_year_3])

        if len(ttm_factor_sets_pre_year_3) <= 0 or ttm_factor_sets_pre_year_3 is None:
            ttm_factor_sets_pre_year_3 = pd.DataFrame(
                {'security_code': [], 'net_profit': [], 'operating_revenue': [], 'operating_cost': []})

        for column in columns:
            if column in list(ttm_factor_sets_pre_year_3.keys()):
                ttm_factor_sets_pre_year_3 = ttm_factor_sets_pre_year_3.drop(column, axis=1)
        ttm_factor_sets_pre_year_3 = ttm_factor_sets_pre_year_3.rename(
            columns={"operating_revenue": "operating_revenue_pre_year_3",
                     "operating_cost": "operating_cost_pre_year_3",
                     "net_profit": "net_profit_pre_year_3",
                     })
        ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year_3, how='outer', on="security_code")
        # field_key = ttm_factor_sets_pre_year_3.keys()
        # for i in field_key:
        #     ttm_factor_sets[i] = ttm_factor_sets_pre_year_3[i]
        # print('get_ttm_factor_sets_3')

        ttm_factor_sets_pre_year_4 = get_fin_consolidated_statements_pit(FinIncomeTTM,
                                                                         [FinIncomeTTM.net_profit,
                                                                          FinIncomeTTM.operating_revenue,
                                                                          FinIncomeTTM.operating_cost,
                                                                          ], dates=[trade_date_pre_year_4])
        if len(ttm_factor_sets_pre_year_4) <= 0 or ttm_factor_sets_pre_year_4 is None:
            ttm_factor_sets_pre_year_4 = pd.DataFrame(
                {'security_code': [], 'net_profit': [], 'operating_revenue': [], 'operating_cost': []})

        for column in columns:
            if column in list(ttm_factor_sets_pre_year_4.keys()):
                ttm_factor_sets_pre_year_4 = ttm_factor_sets_pre_year_4.drop(column, axis=1)
        ttm_factor_sets_pre_year_4 = ttm_factor_sets_pre_year_4.rename(
            columns={"operating_revenue": "operating_revenue_pre_year_4",
                     "operating_cost": "operating_cost_pre_year_4",
                     "net_profit": "net_profit_pre_year_4",
                     })
        ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year_4, how='outer', on="security_code")
        # field_key = ttm_factor_sets_pre_year_4.keys()
        # for i in field_key:
        #     ttm_factor_sets[i] = ttm_factor_sets_pre_year_4[i]
        # print('get_ttm_factor_sets_4')

        ttm_factor_sets_pre_year_5 = get_fin_consolidated_statements_pit(FinIncomeTTM,
                                                                         [FinIncomeTTM.net_profit,
                                                                          FinIncomeTTM.operating_revenue,
                                                                          FinIncomeTTM.operating_cost,
                                                                          ], dates=[trade_date_pre_year_5])
        if len(ttm_factor_sets_pre_year_5) <= 0 or ttm_factor_sets_pre_year_5 is None:
            ttm_factor_sets_pre_year_5 = pd.DataFrame(
                {'security_code': [], 'net_profit': [], 'operating_revenue': [], 'operating_cost': []})

        for column in columns:
            if column in list(ttm_factor_sets_pre_year_5.keys()):
                ttm_factor_sets_pre_year_5 = ttm_factor_sets_pre_year_5.drop(column, axis=1)
        ttm_factor_sets_pre_year_5 = ttm_factor_sets_pre_year_5.rename(
            columns={"operating_revenue": "operating_revenue_pre_year_5",
                     "operating_cost": "operating_cost_pre_year_5",
                     "net_profit": "net_profit_pre_year_5", })

        ttm_factor_sets = pd.merge(ttm_factor_sets, ttm_factor_sets_pre_year_5, how='outer', on="security_code")
        # field_key = ttm_factor_sets_pre_year_5.keys()
        # for i in field_key:
        #     ttm_factor_sets[i] = ttm_factor_sets_pre_year_5[i]
        # print('get_ttm_factor_sets_5')

        growth_sets = pd.merge(ttm_factor_sets, balance_sets, how='outer', on='security_code')
        return growth_sets

    def process_calc_factor(self, trade_date, growth_sets):
        growth_sets = growth_sets.set_index('security_code')
        growth = factor_historical_growth.FactorHistoricalGrowth()
        if len(growth_sets) <= 0:
            print("%s has no data" % trade_date)
            return
        historical_growth_sets = pd.DataFrame()
        historical_growth_sets['security_code'] = growth_sets.index
        historical_growth_sets = historical_growth_sets.set_index('security_code')

        historical_growth_sets = growth.NetAsset1YChg(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.TotalAsset1YChg(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.ORev1YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.OPft1YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.GrPft1YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.NetPft1YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.NetPftAP1YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.NetPft3YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.NetPft5YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.ORev3YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.ORev5YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.NetCF1YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.NetPftAPNNRec1YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.StdUxpErn1YTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.StdUxpGrPft1YTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.FCF1YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.OCF1YChgTTM(growth_sets, historical_growth_sets)
        historical_growth_sets = growth.ICF1YChgTTM(growth_sets, historical_growth_sets)

        historical_growth_sets = historical_growth_sets.reset_index()
        historical_growth_sets['trade_date'] = str(trade_date)
        historical_growth_sets.replace([-np.inf, np.inf, None], np.nan, inplace=True)
        return historical_growth_sets

    def local_run(self, trade_date):
        print('当前交易日: %s' % trade_date)
        tic = time.time()
        growth_sets = self.loading_data(trade_date)
        print('data load time %s' % (time.time() - tic))

        storage_engine = StorageEngine(self._url)
        result = self.process_calc_factor(trade_date, growth_sets)
        print('cal_time %s' % (time.time() - tic))
        storage_engine.update_destdb(str(self._methods[-1]['packet'].split('.')[-1]), trade_date, result)
        # storage_engine.update_destdb('factor_historical_growth', trade_date, result)

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
#
# @app.task
# def distributed_factor(session, trade_date, packet_sets, name):
#     calc_engines = CalcEngine(name, packet_sets)
#     content = cache_data.get_cache(session, factor_name)
#     total_data = json_normalize(json.loads(content))
#     calc_engines.distributed_factor(total_data)
#

# # @app.task()
# def factor_calculate(**kwargs):
#     print("growth_kwargs: {}".format(kwargs))
#     date_index = kwargs['date_index']
#     session = kwargs['session']
#     content = cache_data.get_cache(session, "growth" + str(date_index))
#     total_growth_data = json_normalize(json.loads(str(content, encoding='utf8')))
#     print("len_total_growth_data {}".format(len(total_growth_data)))
#     calculate(date_index, total_growth_data)
