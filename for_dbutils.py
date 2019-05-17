import datetime
import math
import time
import itertools
from contextlib import closing
from collections import OrderedDict
import json
import numpy as np
import pandas as pd
import MySQLdb
import pymssql
from sqlalchemy import create_engine
import dfutils

WIC_DB_HOST = 'wic-risk-database.cwi02trt7ww1.us-east-1.rds.amazonaws.com'
DB_USER = 'root'
DB_PASSWORD = 'waterislandcapital'
DB_NAME = 'wic'
engine = create_engine("mysql://" + DB_USER + ":" + DB_PASSWORD + "@" + WIC_DB_HOST + "/wic")

#from wic
class wic():
    
    def optimized_execute(query, commit=False, retrieve_column_names=False, connection_timeout=250, extra_values=None):
        with closing(MySQLdb.connect(host=WIC_DB_HOST, user=DB_USER, passwd=DB_PASSWORD, db=DB_NAME)) as wic_cnx:
            with closing(wic_cnx.cursor()) as wic_cursor:

                if extra_values is not None:
                    wic_cursor.execute(query, (extra_values,))
                else:
                    wic_cursor.execute(query)
                if commit:
                    wic_cnx.commit()
                    return
                fetched_res = wic_cursor.fetchall()  # fetch (and discard) remaining rows

                if retrieve_column_names:
                    return fetched_res, [i[0] for i in wic_cursor.description]

                return fetched_res
            
    def get_max_date(table_name):
        query = "SELECT MAX(df.Date) FROM wic." + table_name + " AS df "
        results = wic.optimized_execute(query)
        if len(results) == 0:
            return None

        return results[0][0]

    def get_tradegroups_total_pnl(fundcode=None, tg=None, start_date_yyyy_mm_dd=None):
        slicer = dfutils.df_slicer()
        if (start_date_yyyy_mm_dd is not None) & (type(start_date_yyyy_mm_dd) != str):
                start_date = start_date_yyyy_mm_dd.strftime('%Y%m%d')
        elif start_date_yyyy_mm_dd is None:
            start_date = '20150101'

        query = "SELECT `DATE`, Fund, TradeGroup, pnl, cumpnl FROM " + DB_NAME + ".`tradegroups_pnl_cache`"
        if fundcode is not None and tg is not None:
            query += " WHERE Fund = '" + fundcode + "' AND TradeGroup ='" + tg + "' AND `DATE` > " + start_date
        elif fundcode is not None:
            query += " WHERE Fund = '" + fundcode + "' AND `DATE` > " + start_date
        elif tg is not None:
            query += " WHERE TradeGroup = '" + tg + "' AND `DATE` > " + start_date
        else:
            query += " WHERE `DATE` > " + start_date

        res = wic.optimized_execute(query)
        cols = ['Date', 'Fund', 'TradeGroup', 'Total P&L', 'Cumulative Total P&L']
        df = pd.DataFrame(list(res), columns=cols)
        df['Date'] = df['Date'].apply(lambda x: pd.to_datetime(x))
        df['Total P&L'] = df['Total P&L'].astype(float)
        df['Cumulative Total P&L'] = df['Cumulative Total P&L'].astype(float)
        return df
    
    def get_tradegroups_snapshot():
        # region query
        query = "SELECT Fund, Sleeve, TradeGroup, Analyst, LongShort, InceptionDate, EndDate, Status," \
                "`Metrics in Bet JSON`,`Metrics in Bet notes JSON`,`Metrics in NAV JSON`,`Metrics in NAV notes JSON` " \
                "FROM " + DB_NAME + ".tradegroups_snapshot2"
        # endregion

        res = wic.optimized_execute(query)
        cols = ['Fund', 'Sleeve', 'TradeGroup', 'Analyst', 'LongShort', 'InceptionDate', 'EndDate', 'Status',
                'Metrics in Bet JSON', 'Metrics in Bet notes JSON', 'Metrics in NAV JSON', 'Metrics in NAV notes JSON']
        return pd.DataFrame(list(res), columns=cols)

    def get_current_tradegroup_and_fund():
        query = "SELECT Fund, TradeGroup FROM " + DB_NAME + ".tradegroups_snapshot2"

        res = wic.optimized_execute(query)
        cols=['Fund', 'TradeGroup']
        return pd.DataFrame(list(res), columns=cols)

    def delete_table_by_certain_date(date_yyyy_mm_dd, table_name):
        delete_query = "DELETE from " + DB_NAME + ".`" + table_name + "` WHERE Date = '" + date_yyyy_mm_dd + "' "
        try:
            wic.optimized_execute(delete_query, commit=True)
            return "Success"

        except Exception as e:
            return e
     
#from tradar
class tradar():
    
    def optimized_execute(query, commit=False):
        try:
            with closing(pymssql.connect(host='NYDC-WTRTRD01', user='paz', password='Welcome2',
                                         database='TradarBE')) as trdr_pnl_conn:
                with closing(trdr_pnl_conn.cursor()) as trdr_pnl_cursor:
                    trdr_pnl_cursor.execute(query)
                    if commit:
                        trdr_pnl_conn.commit()
                        return

                    fetched_res = trdr_pnl_cursor.fetchall()  # fetch (and discard) remaining rows\
                    return fetched_res
        except Exception as e:
            print e

    def get_securities_pnl_by_tradegroup_and_fund_current(limit_to_tradegroups=[], limit_to_funds=[],
                                                  is_tg_names_in_tradar_format=False, start_date_yyyymmdd=None,
                                                  end_date_yyyymmdd=None, rollup_pnl=False):
        slicer = dfutils.df_slicer()
        if (start_date_yyyymmdd is not None) & (type(start_date_yyyymmddd) != str):
                start_date_yyyymmdd = start_date_yyyymmdd.strftime('%Y%m%d')
        elif start_date_yyyymmdd is None:
            start_date_yyyymmdd = '20170101'
        if end_date_yyyymmdd is None:
            now = datetime.datetime.now()
            end_date = slicer.prev_n_business_days(1, now)
            end_date_yyyymmdd = end_date.strftime('%Y%m%d')
        elif type(end_date_yyyymmdd) != str:
            end_date_yyyymmdd = end_date_yyyymmdd.strftime('%Y%m%d')

        tg_filter_clause = ""
        if len([tg for tg in limit_to_tradegroups if tg is not None]) > 0:
            tg_filter_clause = " AND B.strat IN (" + ",".join([("'" + tg + "'")
                                                               for tg in limit_to_tradegroups 
                                                               if tg is not None]) + ") "
        if is_tg_names_in_tradar_format is True: 
            tg_filter_clause = tg_filter_clause.replace('B.strat', 'SUBSTRING(B.strat,0,21)')

        if len([fund for fund in limit_to_funds if fund is not None]) > 0:
            fund_code_filter_clause = " AND F.fund IN (" + ",".join([("'" + fund + "'")
                                                                     for fund in limit_to_funds
                                                                     if fund is not None]) + ") "

        # region rollup query
        rollup_sql_query = "DECLARE @Holidays TABLE ([Date] DATE); " \
                            "DECLARE @StartDt INT; " \
                            "DECLARE @EndDt INT; " \
                            "INSERT INTO @Holidays VALUES " \
                            "('2015-01-01'), " \
                            "('2015-01-19'), " \
                            "('2015-02-16'), " \
                            "('2015-04-03'), " \
                            "('2015-05-25'), " \
                            "('2015-07-03'), " \
                            "('2015-09-07'), " \
                            "('2015-10-12'), " \
                            "('2015-11-11'), " \
                            "('2015-11-26'), " \
                            "('2015-12-25'), " \
                            "('2016-01-01'), " \
                            "('2016-01-18'), " \
                            "('2016-02-15'), " \
                            "('2016-03-25'), " \
                            "('2016-05-30'), " \
                            "('2016-07-04'), " \
                            "('2016-09-05'), " \
                            "('2016-11-24'), " \
                            "('2016-12-25'), " \
                            "('2017-01-02'), " \
                            "('2017-01-16'), " \
                            "('2017-02-20'), " \
                            "('2017-04-14'), " \
                            "('2017-05-29'), " \
                            "('2017-07-04'), " \
                            "('2017-09-04'), " \
                            "('2017-10-09'), " \
                            "('2017-11-23'), " \
                            "('2017-12-25'); " \
                            "SET @StartDt = " + start_date_yyyymmdd + "; " \
                            "SET @EndDt = " + end_date_yyyymmdd + "; " \
                            "WITH X AS( " \
                            "SELECT " \
                            "CAST(CONVERT(varchar(8), A.timeKey) AS DATE) as [Date], " \
                            "F.fund, " \
                            "B.strat AS TradeGroup, " \
                            "S.ticker, " \
                            "ST.groupingName as sectype, " \
                            "SUM(pnlFC) as PNL " \
                            "FROM performanceAttributionFact AS A " \
                            "INNER JOIN TradeStrat AS B ON A.stratKey = B.stratId " \
                            "INNER JOIN TradeFund AS F ON A.fundKey = F.fundId " \
                            "INNER JOIN Sec AS S ON S.secId = A.secIdKey " \
                            "INNER JOIN SecType AS ST ON ST.sectype = S.sectype " \
                            "WHERE A.timeKey >= @StartDt and A.timeKey <= @EndDt " \
                            + tg_filter_clause + \
                            fund_code_filter_clause + \
                            "GROUP BY A.timeKey, B.strat, F.fund, S.ticker, ST.groupingName " \
                            ") " \
                            ", PNL AS ( " \
                            "SELECT " \
                            "X.[Date],  " \
                            "CASE " \
                            "WHEN DATEPART(dw,X.[Date]) IN (7,1) THEN 'WEEKEND' " \
                            "WHEN X.[Date] IN (SELECT * FROM @Holidays) THEN 'HOLIDAY' " \
                            "ELSE 'BUSINESS DAY' END AS [WeekDay], " \
                            "X.Fund, " \
                            "X.TradeGroup, " \
                            "X.Ticker, " \
                            "X.sectype, " \
                            "X.PNL " \
                            "FROM X " \
                            "), " \
                            "Position2EndDate AS ( " \
                            "SELECT " \
                            "X.fund, " \
                            "X.TradeGroup, " \
                            "X.ticker, " \
                            "X.sectype, " \
                            "MAX(X.[Date]) AS MaxDate, " \
                            "CASE DATEPART(dw,MAX(X.[Date])) " \
                            "WHEN 6 THEN DATEADD(DAY,3,MAX(X.[Date])) " \
                            "WHEN 7 THEN DATEADD(DAY,2,MAX(X.[Date])) " \
                            "ELSE DATEADD(DAY,1,MAX(X.[Date])) END AS NextBusinessDay " \
                            "FROM PNL AS X " \
                            "GROUP BY X.fund, X.TradeGroup, X.ticker, X.sectype " \
                            "), " \
                            "Position2EndDate_BD AS ( " \
                            "SELECT " \
                            "X.fund, " \
                            "X.TradeGroup, " \
                            "X.ticker, " \
                            "X.sectype, " \
                            "X.MaxDate, " \
                            "CASE " \
                            "WHEN NextBusinessDay IN (SELECT * FROM @Holidays) THEN " \
                            "CASE DATEPART(dw,NextBusinessDay) " \
                            "WHEN 6 THEN DATEADD(DAY,3,NextBusinessDay) " \
                            "ELSE DATEADD(DAY,1,NextBusinessDay) END " \
                            "ELSE NextBusinessDay " \
                            "END AS NextBusinessDay_Holiday_Proof " \
                            "FROM Position2EndDate AS X " \
                            "), " \
                            "PNL_DT AS ( " \
                            "SELECT " \
                            "A1.[Date], " \
                            "A1.[WeekDay],  " \
                            "A1.fund, " \
                            "A1.TradeGroup, " \
                            "A1.ticker, " \
                            "A1.sectype, " \
                            "A1.PNL, " \
                            "MAX(A2.[Date]) AS [Last Business Day], " \
                            "DATEDIFF(DAY,MAX(A2.[Date]),A1.[Date]) AS [Last BD days diff] " \
                            "FROM PNL AS A1 " \
                            "LEFT OUTER JOIN PNL AS A2 ON A1.fund = A2.fund and A1.TradeGroup = A2.TradeGroup " \
                            "AND A1.ticker = A2.ticker AND A1.sectype = A2.sectype AND A2.[Date] < A1.[Date] " \
                            "AND A2.[WeekDay] = 'BUSINESS DAY' " \
                            "GROUP BY A1.[Date],A1.[WeekDay],A1.fund, A1.TradeGroup, A1.ticker, A1.sectype, A1.PNL " \
                            "), " \
                            "T AS ( " \
                            "SELECT " \
                            "CASE WHEN A1.[Date] = B.MaxDate AND A1.[WeekDay] <> 'BUSINESS DAY' " \
                            "THEN B.NextBusinessDay_Holiday_Proof ELSE A1.[Date] END AS [Date], " \
                            "CASE WHEN A1.[Date] = B.MaxDate AND A1.[WeekDay] <> 'BUSINESS DAY' " \
                            "THEN 'BUSINESS DAY' ELSE A1.[WeekDay] END AS [WeekDay], " \
                            "A1.fund, " \
                            "A1.TradeGroup, " \
                            "A1.ticker, " \
                            "A1.sectype, " \
                            "A1.[Last Business Day], " \
                            "A1.PNL, " \
                            "ISNULL(SUM(A2.PNL),0) AS ROLLED_UP_PNL, " \
                            "A1.PNL + ISNULL(SUM(A2.PNL),0) AS TOT_PNL,  " \
                            "CASE WHEN A1.[Date] = B.MaxDate AND A1.[WeekDay] <> 'BUSINESS DAY' " \
                            "THEN 'Closed on non-BD. Shifted to next BD' ELSE NULL END AS 'Notes' " \
                            "FROM PNL_DT AS A1 " \
                            "LEFT OUTER JOIN PNL_DT AS A2 " \
                            "ON A1.fund = A2.fund AND A1.TradeGroup = A2.TradeGroup AND A1.ticker = A2.ticker " \
                            "AND A1.sectype = A2.sectype AND " \
                            "(A2.[Date] < A1.[Date] AND (A2.[Date] > A1.[Last Business Day] " \
                            "OR A1.[Last Business Day] IS NULL)) " \
                            "AND (A1.[Last BD days diff] <= 4 OR A1.[Last BD days diff] IS NULL) " \
                            "INNER JOIN Position2EndDate_BD AS B ON A1.fund = B.fund AND A1.TradeGroup = B.TradeGroup " \
                            "AND A1.ticker = B.ticker AND A1.sectype = B.sectype " \
                            "WHERE A1.[WeekDay] = 'BUSINESS DAY' OR A1.[Date] = B.MaxDate " \
                            "GROUP BY A1.[Date],A1.[WeekDay], A1.fund,A1.TradeGroup, A1.ticker, " \
                            "A1.sectype, A1.[Last Business Day],A1.[Last BD days diff],A1.PNL, B.MaxDate, " \
                            "B.NextBusinessDay_Holiday_Proof " \
                            ") " \
                            "SELECT " \
                            "T.[Date],  " \
                            "T.fund, " \
                            "T.TradeGroup, " \
                            "T.ticker, " \
                            "T.sectype, " \
                            "T.TOT_PNL " \
                            "FROM T " \
                            "ORDER BY T.fund, T.TradeGroup, T.ticker,T.sectype, T.[Date] "

        # endregion
        # region non-rollup query
        # don't take today's pnl - tradar's garbage data
        non_rollup_sql_query = \
            "SELECT " \
            "CAST(CONVERT(VARCHAR(8), A.timeKey) AS DATE) AS [DATE], " \
            "F.fund, " \
            "B.strat AS TradeGroup, " \
            "S.ticker, " \
            "ST.groupingName AS sectype, " \
            "SUM(pnlFC) AS PNL " \
            "FROM performanceAttributionFact AS A " \
            "INNER JOIN TradeStrat AS B ON A.stratKey = B.stratId " \
            "INNER JOIN TradeFund AS F ON A.fundKey = F.fundId " \
            "INNER JOIN Sec AS S ON S.secId = A.secIdKey " \
            "INNER JOIN SecType AS ST ON ST.sectype = S.sectype " \
            "WHERE A.timeKey >= " + start_date_yyyymmdd + " AND A.timeKey <= " \
            + end_date_yyyymmdd + \
            tg_filter_clause + \
            fund_code_filter_clause + \
            "GROUP BY A.timeKey, B.strat, F.fund,S.ticker,ST.groupingName " \
            "ORDER BY F.fund, B.strat,S.ticker,A.timeKey "
        # endregion
        query = rollup_sql_query if rollup_pnl else non_rollup_sql_query
        cols = ['Date', 'Fund', 'TradeGroup', 'Ticker', 'SecType', 'Total P&L']
        try:
            query_result = tradar.optimized_execute(query)
        except:
            return pd.DataFrame(columns=cols)

        df = pd.DataFrame(query_result, columns=cols)
        df['Date'] = df['Date'].apply(lambda x: pd.to_datetime(x))
        df['Total P&L'] = df['Total P&L'].astype(float)
        df['Cumulative Total P&L'] = None

        return df
    
   
    
#from northpoint - I know that you will fix these

class northpoint():
     
    def optimized_execute(query, commit=False):
    '''
    Executes the search query in Northpoint
    '''
    
    with closing(pymssql.connect(host='10.16.1.16', user='readonly_user', password='readonly_user',
                                 database='PnLAppDb')) as np_pnl_conn:
        with closing(np_pnl_conn.cursor()) as np_pnl_cursor:
            np_pnl_cursor.execute(query)
            if commit:
                np_pnl_conn.commit()
                return

            fetched_res = np_pnl_cursor.fetchall()  # fetch (and discard) remaining rows\
            return fetched_res
        
    def get_position_calculated_values_history_max_date():  # done
        query = "SELECT MAX(PCVH.TradeDate) FROM [PnLAppDb].[dbo].[PositionCalculatedValuesHistory] AS PCVH "
        results = northpoint.optimized_execute(query)
        if len(results) == 0:
            return None

        return results[0][0]
    
    
    def get_position_calculated_values_history(start_date_yyyy_mm_dd=None, end_date_yyyy_mm_dd=None,
                                               limit_to_tradegroups=[], is_tg_names_in_tradar_format=False,
                                               fund_code=None, NP_qty_rollup=True):
        if start_date_yyyy_mm_dd is None or end_date_yyyy_mm_dd is None:
            start_date_yyyy_mm_dd = '2017-01-01'
            end_date_yyyy_mm_dd = get_position_calculated_values_history_max_date()
        tg_filter_clause = ""
        if len([tg for tg in limit_to_tradegroups if tg is not None]) > 0:
            tg_filter_clause = " AND PCVH.TradeGroup IN (" + ",".join([("'" + tg + "'") 
                                                                       for tg in limit_to_tradegroups 
                                                                       if tg is not None]) + ") "
            if is_tg_names_in_tradar_format: 
                tg_filter_clause = tg_filter_clause.replace('PCVH.TradeGroup', 'SUBSTRING(PCVH.TradeGroup,0,21)')

        fund_code_filtuer_clause = " " if fund_code is None else " AND PCVH.FundCode = '" + fund_code + "' "
        # region query
        # New Fund by added bv Kshitij "WATER ISLAND MERGER ARBITRAGE INSTITUTIONAL":"MACO"
        query = "SELECT " \
                "PCVH.TradeDate, " \
                "ISNULL(PCVH.FundCode, " \
                "CASE F.FundName " \
                "WHEN 'Columbia' THEN 'CAM' " \
                "WHEN 'Litman Gregory' THEN 'LG' " \
                "WHEN 'The Arbitrage Credit Opportunities Fund' THEN 'TACO' " \
                "WHEN 'The Arbitrage Event-Driven Fund' THEN 'AED' " \
                "WHEN 'The Arbitrage Fund' THEN 'ARB' " \
                "WHEN 'TransAmerica' THEN 'TAF' " \
                "WHEN 'WIC Arbitrage Partners' THEN 'WIC' " \
                "WHEN 'The Arbitrage Tactical Equity Fund' THEN 'TAQ' " \
                "WHEN 'Water Island Event-Driven Fund' THEN 'WED' " \
                "WHEN 'Water Island Capital Lev Arb Fund' THEN 'WED' " \
                "WHEN 'WATER ISLAND MERGER ARBITRAGE INSTITUTIONAL' THEN 'MACO' " \
                "WHEN 'Morningstar Alternatives Fund' THEN 'MALT' " \
                "END " \
                ") AS FundCode, " \
                "SPT_TRDGRP.SecName AS TradeGroup, " \
                "SPT_TRDGRP.SecName AS TradeGroup_Tradar_Name, " \
                "PCVH.[Marketing GROUP] AS Sleeve,   " \
                "ISNULL(PCVH.[Bucket],'NA') AS Bucket, " \
                "PCVH.TradeGroupId, " \
                "SPT.Ticker, " \
                "SPT.SecType, " \
                "ISNULL(SPT.MarketCapCategory, 'N/A') AS MarketCapCategory, " \
                "PCVH.SecurityID, " \
                "PCVH.DealTermsCash, " \
                "PCVH.DealTermsStock, " \
                "PCVH.DealValue, " \
                "PCVH.DealClosingDate, " \
                "CASE " \
                "WHEN SPT.SecType = 'FxFwd' THEN 0.0 " \
                "WHEN SPT.SecType <> 'FxFwd' AND PCVH.[Marketing GROUP] = 'Equity Special Situations'  THEN 1.0 " \
                "WHEN SPT.SecType <> 'FxFwd' AND PCVH.[Marketing GROUP] = 'Opportunistic'  THEN 1.0 " \
                "WHEN SPT.SecType <> 'FxFwd' AND PCVH.[Marketing GROUP] = 'Merger Arbitrage'  THEN " \
                "CASE WHEN ISNULL(PCVH.AlphaHedge,'NA') IN ('Alpha','Alpha Hedge') THEN " \
                "CASE PCVH.DealValue WHEN 0 THEN 0.0 ELSE PCVH.DealTermsStock/PCVH.DealValue END " \
                "ELSE 1.0 END " \
                "WHEN SPT.SecType <> 'FxFwd' AND PCVH.[Marketing GROUP] = 'Credit Opportunities'  THEN " \
                "CASE WHEN SPT.SecType IN ('EQ','ExchOpt') THEN 1.0 ELSE 0.0 END " \
                "ELSE NULL END " \
                "AS [Equity Risk Factor], " \
                "PCVH.DV_01, " \
                "PCVH.CR_01, " \
                "PCVH.Adj_CR_01, " \
                "ISNULL(SPT.UltimateCountry,'N/A') AS UltimateCountry, " \
                "ISNULL(PCVH.AlphaHedge,'NA') AS AlphaHedge, " \
                "SUM(PCVH.ExposureLong_USD) AS Exposure_Long, " \
                "SUM(PCVH.ExposureShort_USD) AS Exposure_Short, " \
                "SUM(PCVH.ExposureLong_USD+PCVH.ExposureShort_USD) AS Exposure_Net, " \
                "CASE WHEN NAV.NAV = 0 THEN NULL " \
                "ELSE 100.0*(SUM(PCVH.ExposureLong_USD+PCVH.ExposureShort_USD)/NAV.NAV) END " \
                "AS [Exposure_Net(%)], " \
                "SUM(PCVH.MktValLong_USD+PCVH.MktValShort_USD) AS NetMktVal, " \
                "CASE WHEN ISNULL(PCVH.AlphaHedge,'NA') IN ('Alpha','Alpha Hedge') " \
                "THEN ABS(SUM(PCVH.MktValLong_USD+PCVH.MktValShort_USD)) ELSE NULL END AS Capital, " \
                "CASE WHEN ISNULL(PCVH.AlphaHedge,'NA') IN ('Alpha','Alpha Hedge') " \
                "THEN 100.0*(ABS(SUM(PCVH.MktValLong_USD+PCVH.MktValShort_USD))/NAV.NAV) ELSE NULL " \
                "END AS [Capital % of NAV], " \
                "100.0*SUM(PCVH.Base_Case_NAV_Impact) AS BaseCaseNavImpact, " \
                "100.0*SUM(PCVH.Outlier_NAV_Impact) AS OutlierNavImpact, " \
                "SUM(PCVH.Qty) AS QTY, " \
                "ISNULL(PCVH.TradeGroupLongShortFlag,'NA') AS LongShort, " \
                "ISNULL(SPT.GICSSectorName,'NA') AS Sector, " \
                "ISNULL(SPT.GICSIndustryName,'NA') AS Industry, " \
                "NAV.NAV " \
                "FROM [PnLAppDb].[dbo].[PositionCalculatedValuesHistory] AS PCVH " \
                "INNER JOIN PnLAppDb.dbo.Funds AS F ON PCVH.Portfolio = F.FundID " \
                "INNER JOIN SecurityMaster.dbo.SecurityPivotTable AS SPT ON PCVH.SecurityID = SPT.ID " \
                "INNER JOIN SecurityMaster.dbo.SecurityPivotTable AS SPT_TRDGRP ON PCVH.TradeGroupId = SPT_TRDGRP.ID " \
                "LEFT OUTER JOIN PnLAppDb.pnl.DailyNAV AS NAV ON PCVH.Portfolio = NAV.FundId " \
                "AND PCVH.TradeDate = NAV.[DATE] " \
                "WHERE  TradeDate >= '" + start_date_yyyy_mm_dd + "' AND TradeDate <= '" + end_date_yyyy_mm_dd + "' " \
                + tg_filter_clause + \
                fund_code_filtuer_clause + \
                "GROUP BY " \
                "F.FundName, PCVH.FundCode,PCVH.TradeDate, PCVH.[Marketing GROUP], SPT_TRDGRP.SecName, " \
                "PCVH.TradeGroupId, SPT.Ticker, SPT.BloombergGlobalId, SPT.BloombergGID, " \
                "SPT.SecType, ISNULL(SPT.MarketCapCategory, 'N/A'), " \
                "PCVH.SecurityId, SPT.UltimateCountry, " \
                "ISNULL(PCVH.AlphaHedge,'NA'),PCVH.TradeGroupLongShortFlag, ISNULL(SPT.GICSSectorName,'NA'), " \
                "ISNULL(SPT.GICSIndustryName,'NA'), ISNULL(PCVH.TradeGroupCatalystRating, 'NA'), " \
                "ISNULL(PCVH.[Bucket],'NA'), PCVH.DealTermsCash, PCVH.DealTermsStock, " \
                "PCVH.DealValue, PCVH.DealClosingDate, " \
                "PCVH.DV_01, PCVH.CR_01, PCVH.Adj_CR_01, NAV.NAV, PCVH.Analyst " \
                "ORDER BY PCVH.TradeDate "
        # endregion
        results = northpoint.optimized_execute(query)
        df = pd.DataFrame(results, columns=["Date", "FundCode", "TradeGroup", "TradeGroup_Tradar_Name", "Sleeve",
                                            "Bucket", "TradeGroupId", "Ticker", "SecType", "MarketCapCategory",
                                            "SecurityId", "DealTermsCash", "DealTermsStock", "DealValue", 
                                            "DealClosingDate", "Equity_Risk_Factor", "DV01", "CR01", "Adj_CR01",
                                            "UltimateCountry", "AlphaHedge", "Exposure_Long", "Exposure_Short",
                                            "Exposure_Net", "Exposure_Net(%)", "NetMktVal", "Capital($)", 
                                            "Capital(%)", "BaseCaseNavImpact", "OutlierNavImpact", "Qty", 
                                            "LongShort", "Sector", "Industry", "Fund_NAV"])

        float_cols = ['NetMktVal', 'Capital($)', 'Capital(%)', 'Exposure_Net', 'Exposure_Net(%)', 'BaseCaseNavImpact']
        df[float_cols] = df[float_cols].astype(float)
        df['Equity_Risk_Exp'] = df['Equity_Risk_Factor'].astype(float)*df['Exposure_Net'].astype(float)
        df[['DV01', 'CR01', 'Adj_CR01']] = df[['DV01', 'CR01', 'Adj_CR01']].fillna(0).astype(float)
        df["Date"] = df["Date"].apply(lambda x: pd.to_datetime(x))
        df["DealClosingDate"] = df["DealClosingDate"].apply(lambda x: pd.to_datetime(x))

        as_of_dt = datetime.datetime.strptime(end_date_yyyy_mm_dd, '%Y-%m-%d')

        def duration_calc(days):
            if days <= 90:
                return '0M-3M'
            if days <= 180:
                return '3M-6M'
            if days <= 365:
                return '6M-12M'
            return 'Yr+'

        df["DealDuration"] = df["DealClosingDate"].apply(lambda x: None if pd.isnull(x) else
                                                         duration_calc((as_of_dt - pd.to_datetime(x)).days))

        # region ADJUSTING FOR POSTED TRADES AFTER TRADE DATE - AFFECTING QTY ONLY
        # adjust for end_date_yyyy_mm_dd - take trades with tradedate = end_date_yyyy_mm_dd and posted_date
        #AFTER end_date_yyyy_mm_dd
        # adjust the Qty into the pcvh_df
        if NP_qty_rollup:
            qty_adjust_query = "SELECT " \
                                "A.TradeDate AS [DATE], " \
                                "CASE A.Fund " \
                                "WHEN 'Columbia' THEN 'CAM' " \
                                "WHEN 'Litman Gregory' THEN 'LG' " \
                                "WHEN 'The Arbitrage Credit Opportunities Fund' THEN 'TACO' " \
                                "WHEN 'The Arbitrage Event-Driven Fund' THEN 'AED' " \
                                "WHEN 'The Arbitrage Fund' THEN 'ARB' " \
                                "WHEN 'TransAmerica' THEN 'TAF' " \
                                "WHEN 'WIC Arbitrage Partners' THEN 'WIC' " \
                                "WHEN 'The Arbitrage Tactical Equity Fund' THEN 'TAQ' " \
                                "WHEN 'Water Island Event-Driven Fund' THEN 'WED' " \
                                "WHEN 'Water Island Capital Lev Arb Fund' THEN 'LEV' " \
                                "WHEN 'WATER ISLAND MERGER ARBITRAGE INSTITUTIONAL COMMINGLED MASTER FUND LP' THEN 'MACO' " \
                                "WHEN 'Morningstar Alternatives Fund' THEN 'MALT' " \
                                "END AS FundCode, " \
                                "A.TradeGroupId, " \
                                "A.SecurityId, " \
                                "SUM((CASE SPT.SecType WHEN 'ExchOpt' THEN 100.0 ELSE 1.0 END)*A.Shares) AS Qty " \
                                "FROM " \
                                "[PnLAppDb].[dbo].[vTradesFlatView] AS A " \
                                "INNER JOIN SecurityMaster.dbo.SecurityPivotTable AS SPT ON A.SecurityId = SPT.ID " \
                                "WHERE PostedDate >= DATEADD(DAY,1,TradeDate) AND TradeDate >= '" \
                                + start_date_yyyy_mm_dd + "' " \
                                "GROUP BY A.TradeDate, A.SecurityId, A.Ticker, A.Fund, A.TradeGroupId, SPT.SecType "
            results = optimized_execute(qty_adjust_query)
            qty_adjust_df = pd.DataFrame(results, columns=['Date', 'FundCode', 'TradeGroupId', 'SecurityId', 'Qty_adj'])
            qty_adjust_df[['TradeGroupId', 'SecurityId', 'Qty_adj']] = qty_adjust_df[['TradeGroupId',
                                                                                      'SecurityId', 'Qty_adj']].astype(float)
            qty_adjust_df['Date'] = qty_adjust_df['Date'].apply(lambda x: pd.to_datetime(x))

            df[['TradeGroupId', 'SecurityId', 'Qty']] = df[['TradeGroupId', 'SecurityId', 'Qty']].astype(float)
            df = pd.merge(df, qty_adjust_df, how='left', on=['Date', 'FundCode', 'TradeGroupId', 'SecurityId'])
            df['Qty'] = df['Qty'] + df['Qty_adj'].fillna(0)
            del df['Qty_adj']
        # endregion
        print('PCVH retrieval completed....')
        return df
    
    def get_NAV_df_by_date(fund_name = None, start_date_yyyy_mm_dd=None):
        if start_date_yyyy_mm_dd is None:
            start_date_yyyy_mm_dd = '20150101'
        elif (start_date_yyyy_mm_dd is not None) & (type(start_date_yyyy_mm_dd) != str):
            start_date_yyyy_mm_dd = start_date_yyyy_mm_dd.strftime('%Y%m%d')

        query = "SELECT A.[DATE], A.NAV, " \
                "CASE B.FundName " \
                "WHEN 'Columbia' THEN 'CAM' " \
                "WHEN 'Litman Gregory' THEN 'LG' " \
                "WHEN 'The Arbitrage Credit Opportunities Fund' THEN 'TACO' " \
                "WHEN 'The Arbitrage Event-Driven Fund' THEN 'AED' " \
                "WHEN 'The Arbitrage Fund' THEN 'ARB' " \
                "WHEN 'TransAmerica' THEN 'TAF' " \
                "WHEN 'WIC Arbitrage Partners' THEN 'WIC' " \
                "WHEN 'The Arbitrage Tactical Equity Fund' THEN 'TAQ' " \
                "WHEN 'Water Island Event-Driven Fund' THEN 'WED' " \
                "WHEN 'Water Island Capital Lev Arb Fund' THEN 'LEV' " \
                "WHEN 'WATER ISLAND MERGER ARBITRAGE INSTITUTIONAL' THEN 'MACO' " \
                "WHEN 'Morningstar Alternatives Fund' THEN 'MALT' " \
                "END AS FundCode " \
                "FROM PnLAppDb.pnl.DailyNAV AS A " \
                "INNER JOIN PnLAppDb.dbo.Funds AS B ON A.FundId = B.FundID "

        if fund_name is not None and start_date_yyyy_mm_dd is not None:
            query += "WHERE  A.[Date] > '" + start_date_yyyy_mm_dd + "' AND B.FundName = '" + fund_name + "' "
        elif fund_name is not None:
            query += " WHERE B.FundName = '" + fund_name + "' "
        elif start_date_yyyy_mm_dd is not None:
            query += "WHERE  A.[Date] > '" + start_date_yyyy_mm_dd + "' "

        results = northpoint.optimized_execute(query)
        df = pd.DataFrame(results, columns=["Date", "NAV", "FundCode"])
        df.set_index(pd.DatetimeIndex(df['Date']), inplace=True)
        df.index.name = 'Date'
        df = df.sort_index()
        df['NAV'] = df['NAV'].astype(float)
        #idx = pd.date_range(df['Date'].min(), df['Date'].max())
        #df = df.reindex(idx, fill_value=np.nan)
        del df['Date']
        df.reset_index(level=0, inplace=True)
        df.rename(columns={"index": "Date"}, inplace=True)
        df.index.name = 'Date'
        df.ffill(inplace=True)
        df.set_index(pd.DatetimeIndex(df['Date']), inplace=True)
        df['Date'] = df['Date'].apply(lambda x: pd.to_datetime(x))
        # df.reset_index(inplace=True)
        df = df.reset_index(level=0, drop=True).reset_index()  # Added by Kshitij
        del df['index']
        return df[df['Date'] <= (datetime.datetime.today() - datetime.timedelta(days=1))]
    
    def get_spread_history_by_date(start_date_yyyy_mm_dd=None):
        if start_date_yyyy_mm_dd is None:
            start_date_yyyy_mm_dd = '20150101'
        elif (start_date_yyyy_mm_dd is not None) & (type(start_date_yyyy_mm_dd) != str):
            start_date_yyyy_mm_dd = start_date_yyyy_mm_dd.strftime('%Y%m%d')

        query = "SELECT DISTINCT " \
                "PCVH.TradeDate, " \
                "SPT_TRDGRP.SecName AS TradeGroup, " \
                "SPT_TRDGRP.SecName AS TradeGroup_TradarName, " \
                "PCVH.TradeGroupId, " \
                "PCVH.AllInSpread, " \
                "PCVH.DealValue, " \
                "100.0*(PCVH.AllInSpread/PCVH.DealValue) AS spread " \
                "FROM PnLAppDb.dbo.PositionCalculatedValuesHistory AS PCVH " \
                "INNER JOIN SecurityMaster.dbo.SecurityPivotTable AS SPT_TRDGRP ON PCVH.TradeGroupId = SPT_TRDGRP.ID " \
                "INNER JOIN SecurityMaster.dbo.SecurityPivotTable AS SPT_TARGET ON PCVH.SecurityID= SPT_TARGET.ID " \
                "WHERE SPT_TRDGRP.TargetTickers LIKE ('%'+SPT_TARGET.Ticker+'%') AND PCVH.TradeDate > '" \
                + start_date_yyyy_mm_dd + "' " \
                "AND PCVH.ConsolidatedMarketingGroup = 'Merger Arbitrage' AND PCVH.DealValue <> 0 " \
                "AND PCVH.AllInSpread IS NOT NULL " \
                "ORDER BY TradeDate "

        results = northpoint.optimized_execute(query)
        columns = ["Date", "TradeGroup", "TradeGroup_TradarName", "TradeGroupId", "AllInSpread", "DealValue", "Spread(%)"]
        df = pd.DataFrame(results, columns=columns)
        df["Date"] = df["Date"].apply(lambda x: pd.to_datetime(x))
        return df
