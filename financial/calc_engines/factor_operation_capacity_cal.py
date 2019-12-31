# -*- coding: utf-8 -*-

import pdb, importlib, inspect, time, datetime, json
# from PyFin.api import advanceDateByCalendar
# from data.polymerize import DBPolymerize
from data.storage_engine import StorageEngine
import time
import pandas as pd
import numpy as np
from datetime import datetime
from financial import factor_operation_capacity

from vision.db.signletion_engine import get_fin_consolidated_statements_pit, get_fundamentals, query
from vision.table.industry_daily import IndustryDaily
from vision.table.fin_cash_flow import FinCashFlow
from vision.table.fin_balance import FinBalance
from vision.table.fin_income import FinIncome
from vision.table.fin_indicator import FinIndicator

from vision.table.fin_income_ttm import FinIncomeTTM
from vision.table.fin_cash_flow_ttm import FinCashFlowTTM

from vision.db.signletion_engine import *
from data.sqlengine import sqlEngine


# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)


# from ultron.cluster.invoke.cache_data import cache_data


class CalcEngine(object):
    def __init__(self, name, url, methods=[{'packet': 'financial.factor_operation_capacity', 'class': 'CalcEngine'}, ]):
        self._name = name
        self._methods = methods
        self._url = url

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
        engine = sqlEngine()
        columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']
        ttm_cash_flow = get_fin_consolidated_statements_pit(FinCashFlowTTM,
                                                            [FinCashFlowTTM.net_operate_cash_flow,
                                                             # 经营活动现金流量净额
                                                             FinCashFlowTTM.cash_and_equivalents_at_end,
                                                             # 期末现金及现金等价物余额
                                                             ], dates=[trade_date])
        for column in columns:
            if column in list(ttm_cash_flow.keys()):
                ttm_cash_flow = ttm_cash_flow.drop(column, axis=1)

        ttm_income = get_fin_consolidated_statements_pit(FinIncomeTTM,
                                                         [FinIncomeTTM.operating_cost,  # 营业成本
                                                          FinIncomeTTM.operating_revenue,  # 营业收入
                                                          FinIncomeTTM.total_operating_revenue,  # 营业总收入
                                                          ], dates=[trade_date])
        for column in columns:
            if column in list(ttm_income.keys()):
                ttm_income = ttm_income.drop(column, axis=1)

        ttm_balance = get_fin_consolidated_statements_pit(FinBalanceTTM,
                                                          [FinBalanceTTM.account_receivable,  # 应收账款
                                                           FinBalanceTTM.bill_receivable,  # 应收票据
                                                           FinBalanceTTM.advance_payment,  # 预付款项
                                                           FinBalanceTTM.inventories,  # 存货
                                                           FinBalanceTTM.total_current_assets,  # 流动资产合计
                                                           FinBalanceTTM.fixed_assets_net,  # 固定资产
                                                           FinBalanceTTM.construction_materials,  # 工程物资
                                                           FinBalanceTTM.constru_in_process,  # 在建工程
                                                           FinBalanceTTM.total_assets,  # 资产总计
                                                           FinBalanceTTM.advance_peceipts,  # 预收款项
                                                           FinBalanceTTM.accounts_payable,  # 应付账款
                                                           FinBalanceTTM.notes_payable,  # 应付票据
                                                           FinBalanceTTM.total_owner_equities,  # 所有者权益(或股东权益)合计
                                                           ], dates=[trade_date])
        for column in columns:
            if column in list(ttm_balance.keys()):
                ttm_balance = ttm_balance.drop(column, axis=1)

        ttm_operation_capacity = pd.merge(ttm_cash_flow, ttm_income, on='security_code')
        ttm_operation_capacity = pd.merge(ttm_balance, ttm_operation_capacity, on='security_code')
        return ttm_operation_capacity

    def process_calc_factor(self, trade_date, ttm_operation_capacity):
        ttm_operation_capacity = ttm_operation_capacity.set_index('security_code')
        capacity = factor_operation_capacity.FactorOperationCapacity()

        # 因子计算
        factor_management = pd.DataFrame()
        factor_management['security_code'] = ttm_operation_capacity.index
        factor_management = factor_management.set_index('security_code')

        factor_management = capacity.AccPayablesRateTTM(ttm_operation_capacity, factor_management)
        factor_management = capacity.AccPayablesDaysTTM(ttm_operation_capacity, factor_management)
        factor_management = capacity.ARRateTTM(ttm_operation_capacity, factor_management)
        factor_management = capacity.ARDaysTTM(ttm_operation_capacity, factor_management)
        factor_management = capacity.InvRateTTM(ttm_operation_capacity, factor_management)
        factor_management = capacity.InvDaysTTM(ttm_operation_capacity, factor_management)
        factor_management = capacity.CashCovCycle(factor_management)
        factor_management = capacity.CurAssetsRtTTM(ttm_operation_capacity, factor_management)
        factor_management = capacity.FixAssetsRtTTM(ttm_operation_capacity, factor_management)
        factor_management = capacity.OptCycle(factor_management)
        factor_management = capacity.NetAssetTurnTTM(ttm_operation_capacity, factor_management)
        factor_management = capacity.TotaAssetRtTTM(ttm_operation_capacity, factor_management)

        factor_management = factor_management.reset_index()
        factor_management['trade_date'] = str(trade_date)
        factor_management.replace([-np.inf, np.inf, None], np.nan, inplace=True)
        return factor_management

    def local_run(self, trade_date):
        print('当前交易日: %s' % trade_date)
        tic = time.time()
        ttm_operation_capacity = self.loading_data(trade_date)
        print('data load time %s' % (time.time() - tic))

        storage_engine = StorageEngine(self._url)
        result = self.process_calc_factor(trade_date, ttm_operation_capacity)
        print('cal_time %s' % (time.time() - tic))
        storage_engine.update_destdb(str(self._methods[-1]['packet'].split('.')[-1]), trade_date, result)
        # storage_engine.update_destdb('factor_operation_capacity', trade_date, result)

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
#     print("management_kwargs: {}".format(kwargs))
#     date_index = kwargs['date_index']
#     session = kwargs['session']
#     content1 = cache_data.get_cache(session + str(date_index) + "1", date_index)
#     ttm_operation_capacity = json_normalize(json.loads(str(content1, encoding='utf8')))
#     ttm_operation_capacity.set_index('security_code', inplace=True)
#     print("len_tp_management_data {}".format(len(ttm_operation_capacity)))
#     calculate(date_index, ttm_operation_capacity)
