# -*- coding: utf-8 -*-
import sys
sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import pdb,importlib,inspect,time,datetime,json
# from PyFin.api import advanceDateByCalendar
# from data.polymerize import DBPolymerize
from data.storage_engine import StorageEngine
import time
import pandas as pd
import numpy as np
from datetime import datetime
from basic_derivation import factor_basic_derivation
from datetime import timedelta, datetime

from data.model import BalanceMRQ
from data.model import CashFlowMRQ, CashFlowTTM
from data.model import IncomeMRQ, IncomeTTM, IndicatorTTM, IndicatorMRQ

from vision.db.signletion_engine import *
from vision.table.industry_daily import IndustryDaily
from data.sqlengine import sqlEngine
# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
# from ultron.cluster.invoke.cache_data import cache_data


class CalcEngine(object):
    def __init__(self, name, url, methods=[{'packet': 'basic_derivation.factor_basic_derivation',
                                            'class': 'FactorBasicDerivation'},]):
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
        return list(filter(lambda x: not x.startswith('_') and callable(getattr(method,x)), dir(method)))

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
        trade_date_pre = self.get_trade_date(trade_date, 3, days=31)
        # 读取目前涉及到的因子
        columns = ['COMPCODE', 'PUBLISHDATE', 'ENDDATE', 'symbol', 'company_id', 'trade_date']
        engine = sqlEngine()
        cash_flow_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowMRQ,
                                                                         [CashFlowMRQ.FINALCASHBALA,
                                                                          CashFlowMRQ.ASSEDEPR,  # 固定资产折旧
                                                                          CashFlowMRQ.INTAASSEAMOR,  # 无形资产摊销
                                                                          CashFlowMRQ.ACQUASSETCASH,  # 购建固定资产、无形资产和其他...
                                                                          CashFlowMRQ.LONGDEFEEXPENAMOR,  # 长期待摊费用摊销
                                                                          CashFlowMRQ.DEBTPAYCASH,        # 偿还债务支付的现金
                                                                          CashFlowMRQ.RECEFROMLOAN,       # 取得借款收到的现金
                                                                          CashFlowMRQ.ISSBDRECECASH,      # 发行债券所收到的现金
                                                                          ], dates=[trade_date])
        for col in columns:
            if col in list(cash_flow_sets.keys()):
                cash_flow_sets = cash_flow_sets.drop(col, axis=1)

        balance_sets = engine.fetch_fundamentals_pit_extend_company_id(BalanceMRQ,
                                                                       [BalanceMRQ.SHORTTERMBORR,
                                                                        BalanceMRQ.DUENONCLIAB,
                                                                        BalanceMRQ.LONGBORR,
                                                                        BalanceMRQ.BDSPAYA,
                                                                        BalanceMRQ.PARESHARRIGH,
                                                                        BalanceMRQ.TOTASSET,
                                                                        BalanceMRQ.FIXEDASSECLEATOT,  # 固定资产合计
                                                                        BalanceMRQ.TOTLIAB,
                                                                        BalanceMRQ.RIGHAGGR,          # 股东权益合计
                                                                        BalanceMRQ.INTAASSET,
                                                                        # BalanceMRQ.DEVEEXPE,        # 研发费用, Income 中也有
                                                                        BalanceMRQ.GOODWILL,
                                                                        BalanceMRQ.LOGPREPEXPE,
                                                                        BalanceMRQ.DEFETAXASSET,
                                                                        BalanceMRQ.MINYSHARRIGH,    # 少数股东权益[MINYSHARRIGH]利润表中也有
                                                                        BalanceMRQ.TOTCURRASSET,    # 流动资产合计
                                                                        BalanceMRQ.TOTLIAB,         # 负债合计
                                                                        BalanceMRQ.TOTALCURRLIAB,   # 流动负债合计
                                                                        BalanceMRQ.RESE,            # 盈余公积
                                                                        BalanceMRQ.UNDIPROF,        # 未分配利润
                                                                        BalanceMRQ.CURFDS,          # 货币资金
                                                                        BalanceMRQ.ACCOPAYA,        # 应付帐款
                                                                        BalanceMRQ.ADVAPAYM,        # 预收款项
                                                                        BalanceMRQ.NOTESPAYA,       # 应付票据
                                                                        BalanceMRQ.INTEPAYA,        # 应付利息
                                                                        BalanceMRQ.TOTALNONCLIAB,   # 非流动负债合计
                                                                        BalanceMRQ.TAXESPAYA,       # 应交税费
                                                                        BalanceMRQ.OTHERPAY,        # 其他应付款
                                                                        ], dates=[trade_date])
        for col in columns:
            if col in list(balance_sets.keys()):
                balance_sets = balance_sets.drop(col, axis=1)
        balance_sets = balance_sets.rename(columns={
            'SHORTTERMBORR': 'shortterm_loan',  # 短期借款
            'DUENONCLIAB': 'non_current_liability_in_one_year',  # 一年内到期的非流动负债
            'LONGBORR': 'longterm_loan',      # 长期借款
            'BDSPAYA': 'bonds_payable',       # 应付债券
            'INTEPAYA': 'interest_payable',   # 应付利息
        })
        tp_detivation = pd.merge(cash_flow_sets, balance_sets, how='outer', on='security_code')

        balance_sets_pre = engine.fetch_fundamentals_pit_extend_company_id(BalanceMRQ,
                                                                           [BalanceMRQ.TOTCURRASSET,   # 流动资产合计
                                                                            BalanceMRQ.TOTALCURRLIAB,   # 流动负债合计
                                                                            ], dates=[trade_date_pre])

        for col in columns:
            if col in list(balance_sets_pre.keys()):
                balance_sets_pre = balance_sets_pre.drop(col, axis=1)
        balance_sets_pre = balance_sets_pre.rename(columns={
            'TOTCURRASSET': 'TOTCURRASSET_PRE',
            'TOTALCURRLIAB': 'TOTALCURRLIAB_PRE',
        })
        tp_detivation = pd.merge(balance_sets_pre, tp_detivation, how='outer', on='security_code')

        indicator_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorMRQ,
                                                                         [IndicatorMRQ.NPCUT,
                                                                          IndicatorMRQ.EBIT,  # 息税前利润
                                                                          ], dates=[trade_date])
        for col in columns:
            if col in list(indicator_sets.keys()):
                indicator_sets = indicator_sets.drop(col, axis=1)
        indicator_sets = indicator_sets.rename(columns={'EBIT': 'ebit_mrq'})
        tp_detivation = pd.merge(indicator_sets, tp_detivation, how='outer', on='security_code')

        income_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeMRQ,
                                                                      [IncomeMRQ.INCOTAXEXPE,   # 所得税
                                                                       IncomeMRQ.BIZTOTCOST,    # 营业总成本
                                                                       IncomeMRQ.BIZTOTINCO,    # 营业总收入
                                                                       IncomeMRQ.NETPROFIT,     # 净利润
                                                                       IncomeMRQ.TOTPROFIT,     # 利润总额
                                                                       IncomeMRQ.INTEEXPE,      # 利息支出
                                                                       IncomeMRQ.DEVEEXPE,      # 研发费用
                                                                       IncomeMRQ.VALUECHGLOSS,      # 公允价值变动收益
                                                                       IncomeMRQ.INVEINCO,          # 投资收益
                                                                       IncomeMRQ.EXCHGGAIN,         # 汇兑收益
                                                                       IncomeMRQ.BIZCOST,           # 营业成本
                                                                       IncomeMRQ.BIZINCO,           # 营业收入
                                                                       IncomeMRQ.POUNINCO,          #
                                                                       IncomeMRQ.POUNEXPE,          # 手续费及佣金支出
                                                                       IncomeMRQ.OTHERINCO,         # 其他收益
                                                                       IncomeMRQ.OTHERBIZPROF,      # 其他业务利润
                                                                       IncomeMRQ.OTHERBIZINCO,      # 其他业务收入
                                                                       ], dates=[trade_date])
        for col in columns:
            if col in list(income_sets.keys()):
                income_sets = income_sets.drop(col, axis=1)
        tp_detivation = pd.merge(income_sets, tp_detivation, how='outer', on='security_code')

        # income ttm
        income_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
                                                                          [IncomeTTM.BIZTOTINCO,      # 营业总收入
                                                                           IncomeTTM.BIZTOTCOST,      # 营业总成本
                                                                           IncomeTTM.BIZINCO,         # 营业收入
                                                                           IncomeTTM.BIZCOST,         # 营业成本
                                                                           IncomeTTM.SALESEXPE,       # 销售费用
                                                                           IncomeTTM.MANAEXPE,        # 管理费用
                                                                           IncomeTTM.FINEXPE,         # 财务费用
                                                                           IncomeTTM.INTEEXPE,        # 利息支出
                                                                           IncomeTTM.DEVEEXPE,        # 研发费用
                                                                           IncomeTTM.ASSEIMPALOSS,    # 资产减值损失
                                                                           IncomeTTM.PERPROFIT,       # 营业利润
                                                                           IncomeTTM.TOTPROFIT,       # 利润总额
                                                                           IncomeTTM.NETPROFIT,       # 净利润
                                                                           IncomeTTM.PARENETP,        # 归属母公司股东的净利润
                                                                           IncomeTTM.BIZTAX,          # 营业税金及附加
                                                                           IncomeTTM.NONOREVE,
                                                                           IncomeTTM.OTHERINCO,       # 其他收益
                                                                           IncomeTTM.POUNEXPE,        # 手续费及佣金支出
                                                                           IncomeTTM.NONOEXPE,
                                                                           IncomeTTM.MINYSHARRIGH,    # 少数股东权益
                                                                           IncomeTTM.INCOTAXEXPE,
                                                                           IncomeTTM.VALUECHGLOSS,    # 公允价值变动收益
                                                                           IncomeTTM.INVEINCO,        # 投资收益
                                                                           IncomeTTM.EXCHGGAIN,       # 汇兑收益

                                                                           ], dates=[trade_date])
        for col in columns:
            if col in list(income_ttm_sets.keys()):
                income_ttm_sets = income_ttm_sets.drop(col, axis=1)
        income_ttm_sets = income_ttm_sets.rename(columns={'MINYSHARRIGH': 'minority_profit'})

        # cash flow ttm
        cash_flow_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
                                                                             [CashFlowTTM.MANANETR,
                                                                              CashFlowTTM.LABORGETCASH,
                                                                              CashFlowTTM.INVNETCASHFLOW,
                                                                              CashFlowTTM.FINNETCFLOW,
                                                                              CashFlowTTM.CASHNETI,
                                                                              ], dates=[trade_date])
        for col in columns:
            if col in list(cash_flow_ttm_sets.keys()):
                cash_flow_ttm_sets = cash_flow_ttm_sets.drop(col, axis=1)
        ttm_derivation = pd.merge(income_ttm_sets, cash_flow_ttm_sets, how='outer', on='security_code')

        # indicator ttm
        indicator_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorTTM,
                                                                             [IndicatorTTM.NPCUT,
                                                                              ], dates=[trade_date])
        for col in columns:
            if col in list(indicator_ttm_sets.keys()):
                indicator_ttm_sets = indicator_ttm_sets.drop(col, axis=1)
        ttm_derivation = pd.merge(indicator_ttm_sets, ttm_derivation, how='outer', on='security_code')

        # 获取申万二级分类
        sw_indu = get_fundamentals(query(IndustryDaily.security_code,
                                         IndustryDaily.industry_code2,
                                         ).filter(IndustryDaily.trade_date.in_([trade_date])))



        return tp_detivation, ttm_derivation, sw_indu

    def process_calc_factor(self, trade_date, tp_derivation, ttm_derivation, sw_industry):
        tp_derivation = tp_derivation.set_index('security_code')
        ttm_derivation = ttm_derivation.set_index('security_code')

        # 读取目前涉及到的因子
        derivation = factor_basic_derivation.FactorBasicDerivation()
        # 因子计算
        factor_derivation = pd.DataFrame()
        factor_derivation['security_code'] = tp_derivation.index
        factor_derivation = factor_derivation.set_index('security_code')

        factor_derivation = derivation.FCFF(tp_derivation, factor_derivation)
        factor_derivation = derivation.FCFE(tp_derivation, factor_derivation)
        factor_derivation = derivation.NonRecGainLoss(tp_derivation, factor_derivation)
        factor_derivation = derivation.NetOptInc(tp_derivation, factor_derivation, sw_industry)
        factor_derivation = derivation.WorkingCap(tp_derivation, factor_derivation)
        factor_derivation = derivation.TangibleAssets(tp_derivation, factor_derivation)
        factor_derivation = derivation.RetainedEarnings(tp_derivation, factor_derivation)
        factor_derivation = derivation.InterestBearingLiabilities(tp_derivation, factor_derivation)
        factor_derivation = derivation.NetDebt(tp_derivation, factor_derivation)
        factor_derivation = derivation.InterestFreeCurLb(tp_derivation, factor_derivation)
        factor_derivation = derivation.InterestFreeNonCurLb(tp_derivation, factor_derivation)
        factor_derivation = derivation.DepAndAmo(tp_derivation, factor_derivation)
        factor_derivation = derivation.EquityPC(tp_derivation, factor_derivation)
        factor_derivation = derivation.TotalInvestedCap(tp_derivation, factor_derivation)
        factor_derivation = derivation.TotalAssets(tp_derivation, factor_derivation)
        factor_derivation = derivation.TotalFixedAssets(tp_derivation, factor_derivation)
        factor_derivation = derivation.TotalLib(tp_derivation, factor_derivation)
        factor_derivation = derivation.ShEquity(tp_derivation, factor_derivation)
        factor_derivation = derivation.CashAndCashEqu(tp_derivation, factor_derivation)
        factor_derivation = derivation.EBIT(tp_derivation, factor_derivation)
        # TTM
        factor_derivation = derivation.SalesTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.TotalOptCostTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.OptIncTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.GrossMarginTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.SalesExpensesTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.AdmFeeTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.FinFeeTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.PerFeeTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.InterestExpTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.MinorInterestTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.AssetImpLossTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NetIncFromOptActTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NetIncFromValueChgTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.OptProfitTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NetNonOptIncAndExpTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.EBITTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.IncTaxTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.TotalProfTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NetIncTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NetProfToPSTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NetProfAfterNonRecGainsAndLossTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.EBITFORPTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.EBITDATTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.CashRecForSGAndPSTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NCFOTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NetCashFlowFromInvActTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NetCashFlowFromFundActTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.NetCashFlowTTM(ttm_derivation, factor_derivation)
        factor_derivation = derivation.BusTaxAndSuchgTTM(ttm_derivation, factor_derivation)

        factor_derivation = factor_derivation.reset_index()
        factor_derivation['trade_date'] = str(trade_date)
        factor_derivation.replace([-np.inf, np.inf, None], np.nan, inplace=True)
        return factor_derivation

    def local_run(self, trade_date):
        print('当前交易日: %s' % trade_date)
        tic = time.time()
        tp_detivation, ttm_derivation, sw_industry = self.loading_data(trade_date)
        print('data load time %s' % (time.time()-tic))

        storage_engine = StorageEngine(self._url)
        result = self.process_calc_factor(trade_date, tp_detivation, ttm_derivation, sw_industry)
        print('cal_time %s' % (time.time() - tic))
        storage_engine.update_destdb(str(self._methods[-1]['packet'].split('.')[-1]), trade_date, result)
        # storage_engine.update_destdb('factor_basic_derivation', trade_date, result)

        
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
#     factor_name = kwargs['factor_name']
#     content1 = cache_data.get_cache(session + str(date_index) + "1", date_index)
#     content2 = cache_data.get_cache(session + str(date_index) + "2", date_index)
#     tp_derivation = json_normalize(json.loads(str(content1, encoding='utf8')))
#     ttm_derivation = json_normalize(json.loads(str(content2, encoding='utf8')))
#     tp_derivation.set_index('security_code', inplace=True)
#     ttm_derivation.set_index('security_code', inplace=True)
#     print("len_tp_management_data {}".format(len(tp_derivation)))
#     print("len_ttm_management_data {}".format(len(ttm_derivation)))
#     # total_cash_flow_data = {'tp_management': tp_derivation, 'ttm_management': ttm_derivation}
#     calculate(date_index, tp_derivation, ttm_derivation, factor_name)

