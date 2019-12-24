# -*- coding: utf-8 -*-
import sys

sys.path.append('../')
sys.path.append('../../')
sys.path.append('../../../')
import pdb, importlib, inspect, time, datetime, json
# from PyFin.api import advanceDateByCalendar
# from data.polymerize import DBPolymerize
from data.storage_engine import StorageEngine
import time
import pandas as pd
import numpy as np
from datetime import datetime
from basic_derivation import factor_basic_derivation
from datetime import timedelta, datetime

from vision.db.signletion_engine import get_fin_consolidated_statements_pit
from vision.table.industry_daily import IndustryDaily
from vision.table.fin_cash_flow import FinCashFlow
from vision.table.fin_balance import FinBalance
from vision.table.fin_income import FinIncome
from vision.table.fin_indicator import FinIndicator
from utilities.sync_util import SyncUtil

from data.sqlengine import sqlEngine


# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_rows', None)
# from ultron.cluster.invoke.cache_data import cache_data


class CalcEngine(object):
    def __init__(self, name, url, methods=[{'packet': 'basic_derivation.factor_basic_derivation',
                                            'class': 'FactorBasicDerivation'}, ]):
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
        trade_date_pre = self.get_trade_date(trade_date, 3, days=30)
        # 读取目前涉及到的因子
        columns = ['COMPCODE', 'publish_date', 'report_date', 'symbol', 'company_id', 'trade_date']
        # cash flow report
        cash_flow_sets = get_fin_consolidated_statements_pit(FinCashFlow,
                                                             [FinCashFlow.cash_and_equivalents_at_end,
                                                              FinCashFlow.fixed_assets_depreciation,  # 固定资产折旧
                                                              FinCashFlow.intangible_assets_amortization,  # 无形资产摊销
                                                              FinCashFlow.fix_intan_other_asset_acqui_cash, # 购建固定资产、无形资产和其他...
                                                              FinCashFlow.defferred_expense_amortization,  # 长期待摊费用摊销
                                                              FinCashFlow.borrowing_repayment,  # 偿还债务支付的现金
                                                              FinCashFlow.cash_from_borrowing,  # 取得借款收到的现金
                                                              FinCashFlow.cash_from_bonds_issue,  # 发行债券所收到的现金
                                                              ], db_filters=[], dates=[trade_date])
        for col in columns:
            if col in list(cash_flow_sets.keys()):
                cash_flow_sets = cash_flow_sets.drop(col, axis=1)
        pdb.set_trace()

        # balance mrq
        balance_sets = get_fin_consolidated_statements_pit(FinBalance,
                                                           [FinBalance.shortterm_loan,  # 短期借款
                                                            FinBalance.non_current_liability_in_one_year,  # 一年内到期的非流动负债
                                                            FinBalance.longterm_loan,  # 长期借款
                                                            FinBalance.bonds_payable,  # 应付债券
                                                            FinBalance.equities_parent_company_owners,  # 归属于母公司股东权益合计
                                                            FinBalance.shortterm_bonds_payable,  # 应付短期债券
                                                            FinBalance.total_assets,  # 资产总计
                                                            FinBalance.total_fixed_assets_liquidation,  # 固定资产及清理合计
                                                            FinBalance.total_owner_equities,  # 所有者权益(或股东权益)合计
                                                            FinBalance.intangible_assets,  # 无形资产
                                                            FinBalance.development_expenditure,  # 开发支出
                                                            FinBalance.good_will,  # 商誉
                                                            FinBalance.long_deferred_expense,  # 长期待摊费用
                                                            FinBalance.deferred_tax_assets,  # 递延所得税资产
                                                            FinBalance.minority_interests,  # 少数股东权益
                                                            FinBalance.total_current_assets,  # 流动资产合计
                                                            FinBalance.total_liability,  # 负债合计
                                                            FinBalance.total_current_liability,  # 流动负债合计
                                                            FinBalance.surplus_reserve_fund,  # 盈余公积
                                                            FinBalance.retained_profit,  # 未分配利润
                                                            FinBalance.cash_equivalents,  # 货币资金
                                                            FinBalance.accounts_payable,  # 应付帐款
                                                            FinBalance.advance_peceipts,  # 预收款项
                                                            FinBalance.notes_payable,  # 应付票据
                                                            FinBalance.interest_payable,  # 应付利息
                                                            FinBalance.total_non_current_liability,  # 非流动负债合计
                                                            FinBalance.taxs_payable,  # 应交税费
                                                            FinBalance.other_payable,  # 其他应付款
                                                            ], db_filters=[], dates=[trade_date])

        for col in columns:
            if col in list(balance_sets.keys()):
                balance_sets = balance_sets.drop(col, axis=1)
        tp_detivation = pd.merge(cash_flow_sets, balance_sets, how='outer', on='security_code')
        pdb.set_trace()

        # Balance MRQ数据
        balance_sets_pre = get_fin_consolidated_statements_pit(FinBalance,
                                                               [FinBalance.total_current_assets,  # 流动资产合计
                                                                FinBalance.cash_equivalents,  # 流动负债合计
                                                                FinBalance.total_current_liability,  # 流动负债合计
                                                                FinBalance.shortterm_bonds_payable,  # 流动负债合计
                                                                FinBalance.shortterm_loan,  # 流动负债合计
                                                                FinBalance.non_current_liability_in_one_year,  # 流动负债合计
                                                                ], dates=[trade_date_pre])

        for col in columns:
            if col in list(balance_sets_pre.keys()):
                balance_sets_pre = balance_sets_pre.drop(col, axis=1)
        balance_sets_pre = balance_sets_pre.rename(columns={
            'total_current_assets': 'total_current_assets_pre',
            'cash_equivalents': 'cash_equivalents_pre',
            'total_current_liability': 'total_current_liability_pre',
            'shortterm_loan': 'shortterm_loan_pre',
            'shortterm_bonds_payable': 'shortterm_bonds_payable_pre',
            'non_current_liability_in_one_year': 'non_current_liability_in_one_year_pre',
        })
        tp_detivation = pd.merge(balance_sets_pre, tp_detivation, how='outer', on='security_code')
        pdb.set_trace()

        # incicator mrq 数据
        indicator_sets = get_fin_consolidated_statements_pit(FinIndicator,
                                                             [FinIndicator.np_cut, # 扣除非经常性损益的净利润
                                                              ], dates=[trade_date])
        for col in columns:
            if col in list(indicator_sets.keys()):
                indicator_sets = indicator_sets.drop(col, axis=1)
        tp_detivation = pd.merge(indicator_sets, tp_detivation, how='outer', on='security_code')
        pdb.set_trace()

        # income mrq数据
        income_sets = get_fin_consolidated_statements_pit(FinIncome,
                                                          [FinIncome.income_tax,  # 所得税费用
                                                           FinIncome.total_operating_cost,  # 营业总成本
                                                           FinIncome.total_operating_revenue,  # 营业总收入
                                                           FinIncome.net_profit,  # 净利润
                                                           FinIncome.np_parent_company_owners,  # 归属母公司股东的净利润
                                                           FinIncome.total_profit,  # 利润总额
                                                           FinIncome.interest_expense,  # 利息支出
                                                           FinIncome.interest_income,  # 利息收入
                                                           FinIncome.financial_expense,  # 财务费用
                                                           FinIncome.rd_expenses,  # 研发费用
                                                           FinIncome.fair_value_variable_income,  # 公允价值变动收益
                                                           FinIncome.investment_income,  # 投资收益
                                                           FinIncome.exchange_income,  # 汇兑收益
                                                           FinIncome.operating_cost,  # 营业成本
                                                           FinIncome.operating_revenue,  # 营业收入
                                                           FinIncome.commission_income,  # 手续费及佣金收入
                                                           FinIncome.service_commission_fee,  # 手续费及佣金支出
                                                           FinIncome.other_earnings,  # 其他收益
                                                           FinIncome.other_business_profits,  # 其他业务利润
                                                           FinIncome.other_operating_revenue,  # 其他业务收入
                                                           ],db_filters=[], dates=[trade_date])
        for col in columns:
            if col in list(income_sets.keys()):
                income_sets = income_sets.drop(col, axis=1)
        tp_detivation = pd.merge(income_sets, tp_detivation, how='outer', on='security_code')
        pdb.set_trace()

        # # income ttm数据
        # income_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IncomeTTM,
        #                                                                   [IncomeTTM.total_operating_revenue,  # 营业总收入
        #                                                                    IncomeTTM.total_operating_cost,  # 营业总成本
        #                                                                    IncomeTTM.operating_revenue,  # 营业收入
        #                                                                    IncomeTTM.operating_cost,  # 营业成本
        #                                                                    IncomeTTM.sale_expense,  # 销售费用
        #                                                                    IncomeTTM.administration_expense,  # 管理费用
        #                                                                    IncomeTTM.financial_expense,  # 财务费用
        #                                                                    IncomeTTM.interest_expense,  # 利息支出
        #                                                                    IncomeTTM.development_expenditure,  # 研发费用
        #                                                                    IncomeTTM.asset_impairment_loss,  # 资产减值损失
        #                                                                    IncomeTTM.operating_profit,  # 营业利润
        #                                                                    IncomeTTM.total_profit,  # 利润总额
        #                                                                    IncomeTTM.net_profit,  # 净利润
        #                                                                    IncomeTTM.np_parent_company_owners,
        #                                                                    # 归属母公司股东的净利润
        #                                                                    IncomeTTM.operating_tax_surcharges,
        #                                                                    # 营业税金及附加
        #                                                                    IncomeTTM.non_operating_revenue,  # 营业外收入
        #                                                                    IncomeTTM.other_earnings,  # 其他收益
        #                                                                    IncomeTTM.service_commission_fee,  # 手续费及佣金支出
        #                                                                    IncomeTTM.non_operating_expense,  # 营业外支出
        #                                                                    IncomeTTM.minority_profit,  # 少数股东损益
        #                                                                    IncomeTTM.income_tax,
        #                                                                    IncomeTTM.fair_value_variable_income,
        #                                                                    # 公允价值变动收益
        #                                                                    IncomeTTM.investment_income,  # 投资收益
        #                                                                    IncomeTTM.exchange_income,  # 汇兑收益
        #
        #                                                                    ], dates=[trade_date])
        # for col in columns:
        #     if col in list(income_ttm_sets.keys()):
        #         income_ttm_sets = income_ttm_sets.drop(col, axis=1)
        # income_ttm_sets = income_ttm_sets.rename(columns={'minority_profit': 'minority_profit'})
        #
        # # cash flow ttm
        # cash_flow_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(CashFlowTTM,
        #                                                                      [CashFlowTTM.net_operate_cash_flow,   #  经营活动产生的现金流量净额
        #                                                                       CashFlowTTM.goods_sale_and_service_render_cash,  # 销售商品、提供劳务收到的现金
        #                                                                       CashFlowTTM.net_invest_cash_flow,    #  投资活动产生的现金流量净额
        #                                                                       CashFlowTTM.net_finance_cash_flow,   #  筹资活动产生的现金流量净额
        #                                                                       CashFlowTTM.cash_equivalent_increase_indirect,  # 现金及现金等价物的净增加额
        #                                                                       ], dates=[trade_date])
        # for col in columns:
        #     if col in list(cash_flow_ttm_sets.keys()):
        #         cash_flow_ttm_sets = cash_flow_ttm_sets.drop(col, axis=1)
        # ttm_derivation = pd.merge(income_ttm_sets, cash_flow_ttm_sets, how='outer', on='security_code')
        #
        # # indicator ttm
        # indicator_ttm_sets = engine.fetch_fundamentals_pit_extend_company_id(IndicatorTTM,
        #                                                                      [IndicatorTTM.np_cut,
        #                                                                       ], dates=[trade_date])
        # for col in columns:
        #     if col in list(indicator_ttm_sets.keys()):
        #         indicator_ttm_sets = indicator_ttm_sets.drop(col, axis=1)
        # ttm_derivation = pd.merge(indicator_ttm_sets, ttm_derivation, how='outer', on='security_code')

        # 获取申万二级分类
        sw_indu = get_fundamentals(query(IndustryDaily.security_code,
                                         IndustryDaily.industry_code2,
                                         ).filter(IndustryDaily.trade_date.in_([trade_date])))

        # return tp_detivation, ttm_derivation, sw_indu
        return tp_detivation, sw_indu

    def process_calc_factor(self, trade_date, tp_derivation, ttm_derivation, sw_industry):
        tp_derivation = tp_derivation.set_index('security_code')
        ttm_derivation = ttm_derivation.set_index('security_code')

        # 读取目前涉及到的因子
        derivation = factor_basic_derivation.FactorBasicDerivation()
        # 因子计算
        factor_derivation = pd.DataFrame()
        factor_derivation['security_code'] = tp_derivation.index
        factor_derivation = factor_derivation.set_index('security_code')
        factor_derivation = derivation.EBIT(tp_derivation, factor_derivation)
        factor_derivation = derivation.EBITDA(tp_derivation, factor_derivation)

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
        # tp_detivation, ttm_derivation, sw_industry = self.loading_data(trade_date)
        tp_detivation, sw_industry = self.loading_data(trade_date)
        print('data load time %s' % (time.time() - tic))

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
