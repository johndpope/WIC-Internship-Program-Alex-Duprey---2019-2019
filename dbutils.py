''' This is a File for Common Database Operations that are not executed using Django ORM '''
#from django.db import connection, connections
import pandas as pd
import datetime


class Wic():

    @staticmethod
    def get_universe_from_funds():
        query = "CALL wic.GET_UNIVERSE_FROM_ALL_FUNDS()"
        universe_df = pd.read_sql_query(query, connection)
        universe_df.columns = ['DealName','Sleeve', 'Bucket', 'ClosingDate', 'Ticker', 'Price', 'DealDownside', 'DealUpside',
                               'PM_BASE_CASE', 'DealValue', 'CatalystRating', 'OriginationDate', 'Duration',
                               'Sector', 'Industry', 'RiskLimit']

        return universe_df

    @staticmethod
    def get_latest_positions_for_nav_impacts():
        ''' Routine to fetch latest positions for NAV Impacts '''
        query = "select Flat_file_as_of as `PositionsAsOf`,Fund as Fund, TradeGroup as TradeGroup, UnderlyingTicker as Underlying, Ticker, pm_base_case as PMBaseCase, FXCurrentLocalToBase as FXLocalView, (amount * Factor) as NetAmount, "\
                "outlier as Outlier, Strike as Strike, SecType, PutCall as PutCall, Price, BloombergID "\
                "from wic.daily_flat_file_db "\
                "where Flat_file_as_of = (select max(Flat_file_as_of) from wic.daily_flat_file_db) and Ticker not like '%CASH%' and SecType NOT IN ('B', 'FX', 'FXFWD', 'CVB') and SecType IS NOT NULL "\
                "and Fund in ('ARB', 'MACO', 'AED', 'CAM', 'LG', 'LEV') "

        latest_positions_df = pd.read_sql_query(query, connection)

        return latest_positions_df




    @staticmethod
    def get_statpro_df():
        ''' Gets the Entire StatPro Dataframe '''
        start_date = '2013-01-01'
        end_date = '2016-01-01'
        query = "SELECT Name as SecurityName, Ctp as Contribution, `From` as TradeDate, Classification3Name as TradeGroup, " \
                "CASE Fund_Name " \
                "WHEN 'Columbia AP Multi Manger Alternative Strategy' THEN 'CAM' " \
                "WHEN 'Litmon Gregory Master Alternative Strategy' THEN 'LG' " \
                "WHEN 'The Arbitrage Credit Opportunities Fund' THEN 'TACO' " \
                "WHEN 'The Arbitrage Event-Driven Fund' THEN 'AED' " \
                "WHEN 'The Arbitrage Fund' THEN 'ARB' " \
                "WHEN 'The Arbitrage Tactical Equity Fund' THEN 'TAQ' " \
                "END " \
                "as FundName"\
                " from stat_pro_test WHERE `FROM` BETWEEN '"+start_date+"' AND '"+\
                end_date +"'"

        stat_pro_df = pd.read_sql_query(query, connection)
        return stat_pro_df



    @staticmethod
    def get_fund_level_attribution(fund_name, start_date, end_date):
        ''' Assigns Group by to sum tradegroup contributions in a particular Fund '''
        start_date = datetime.datetime.strftime(datetime.datetime.strptime(start_date, "%m/%d/%Y"),
                                                "%Y-%m-%d")  # Database format is yyyy-mm-dd
        end_date = datetime.datetime.strftime(datetime.datetime.strptime(end_date, "%m/%d/%Y"), "%Y-%m-%d")

        fund_level_df = pd.read_sql_query("Select SUM(Ctp) as Ctp_Sum, `Classification3Name`, `From`, `To`, `Fund_Name` FROM stat_pro_test where `Fund_Name` = '"+fund_name+
                                          "' AND `To` BETWEEN '" + start_date + "' AND '" + end_date + "' GROUP BY Classification3Name", connection)

        #Cols = ['Ctp_Sum`, 'Classification3Name', 'From', 'To', 'Fund_Name']
        return fund_level_df

    @staticmethod
    def get_statpro_attribution(fund_name, start_date, end_date):
        ''' Returns the Attribution for specified Fund withing the given Range '''
        start_date = datetime.datetime.strftime(datetime.datetime.strptime(start_date,"%m/%d/%Y"), "%Y-%m-%d")  #Database format is yyyy-mm-dd
        end_date = datetime.datetime.strftime(datetime.datetime.strptime(end_date,"%m/%d/%Y"), "%Y-%m-%d")
        attribution = pd.read_sql_query("SELECT Ctp, `From`, `To`, `Classification3Name`, `Fund_Name` from stat_pro_test where `Fund_Name` = '"+fund_name+
                                        "' AND `To` BETWEEN '"+start_date+"' AND '"+end_date+"'",connection)

        # Retrieved Column values ['Ctp' 'From' 'To' 'Classification3Name' 'Fund_Name']
        return attribution


    @staticmethod
    def get_distinct_funds_from_statpro():
        '''Returns the Funds from Statpro'''
        # df = pd.read_csv("Columbia AP Multi Manager Alternative Strategy - W/Columbia AP Multi Manager Alternative Strategy - W2015.csv")
        # df['Fund_Name'] = "Columbia AP Multi Manger Alternative Strategy"
        #
        # df.to_sql(name='stat_pro_test',con=engine, if_exists='append', chunksize=100)

        df_funds = pd.read_sql_query("SELECT DISTINCT name from statpro_funds", connection)
        return df_funds
        # for fund_name in df_funds['Fund_Name']:
        #     print('Processing for ' + fund_name)
        #     df = pd.read_sql_query("Select * from stat_pro_test where Fund_Name ='" + fund_name + "'", engine)
        #     final_dataframe = pd.DataFrame(columns=['TradeGroup', 'Compounded Contribution', 'From', 'To'])
        #     print(final_dataframe.columns.values)
        #     #
        #     unique_tradegroups = df['Classification3Name'].unique()
        #     print('Calculating TradeGroup Level Compounded Contribution')
        #     for tradegroup in unique_tradegroups:
        #         filtered_dataframe = df[df.Classification3Name == tradegroup]['Ctp']
        #         min_date = df[df.Classification3Name == tradegroup]['From'].min()
        #         max_date = df[df.Classification3Name == tradegroup]['To'].max()
        #         first_value = float(filtered_dataframe.iloc[0])
        #         first_value = first_value / 100  # Initially Divide by 100
        #         compounded_ctp = first_value
        #         for item in filtered_dataframe[1:]:
        #             compounded_ctp = (1 + float(item) / 100) * (1 + compounded_ctp) - 1
        #         compounded_ctp *= 10000
        #         df2 = pd.DataFrame([[tradegroup, compounded_ctp, min_date, max_date]],
        #                            columns=['TradeGroup', 'Compounded Contribution', 'From', 'To'])
        #         final_dataframe = final_dataframe.append(df2)
        #
        #     final_dataframe.to_csv("Compounded_contribution_" + fund_name + ".csv")
        #     print('Calculating Fund Level Compounded Contribution')
        #
        #     first_value = final_dataframe['Compounded Contribution'].iloc[0]
        #     compounded_ctp = first_value / 10000
        #     min_date = final_dataframe['From'].min()
        #     max_date = final_dataframe['To'].max()
        #     for tradegroup_contribution in final_dataframe['Compounded Contribution'][1:]:  # Already Unique
        #         compounded_ctp = (1 + tradegroup_contribution / 10000) * (1 + compounded_ctp) - 1
        #
        #     fund_level_dataframe = pd.DataFrame([[fund_name, compounded_ctp * 10000, min_date, max_date]],
        #                                         columns=['FundName', 'Compounded Contribution', 'From', 'To'])
        #     fund_level_dataframe.to_csv(fund_name + ".csv")
        #     # For Fund level, compound Tradegroup level
        
    @staticmethod
    def optimized_execute(query, commit=False, retrieve_column_names=False, connection_timeout=250, extra_values=None):
        with closing(mysql.connector.connect(host=WIC_DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
                                             connection_timeout=connection_timeout)) as wic_cnx:
            with closing(wic_cnx.cursor()) as wic_cursor:
                wic_cnx.cmd_refresh(RefreshOption.TABLES | RefreshOption.LOG | RefreshOption.THREADS)  # refresh cache
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
            
    @staticmethod
    def get_ess_calibration_meta(alpha_ticker, peers_list, start_date_yyyy_mm_dd, end_date_yyyy_mm_dd):
        query = "SELECT " \
                "CID, " \
                "`Alpha Ticker`, " \
                "`Peers JSON`, " \
                "`Start Date`, " \
                "`End Date` " \
                "FROM " + DB_NAME + ".ess_idea_db_calibration_meta " \
                                    "WHERE `Alpha Ticker` ='" + alpha_ticker + "' AND `peers JSON`='" + json.dumps(
            sorted(
                peers_list)) + "' AND `Start Date`='" + start_date_yyyy_mm_dd + "' AND `End Date`='" + end_date_yyyy_mm_dd + "'"
        cols = ['CID', 'Alpha Ticker', 'Peers JSON', 'StartDate', 'EndDate']
        res = wic.optimized_execute(query)
        return pd.DataFrame(res, columns=cols)
    
    @staticmethod
    def get_multiple_df_from_ess_calibration_timeseries_table(cid):
        query = "SELECT " \
                "`CID`, " \
                "`Date`, " \
                "`Ticker`, " \
                "`PX`, " \
                "`P/E`, " \
                "`EV/EBITDA`, " \
                "`EV/SALES`, " \
                "`DVD YIELD`, " \
                "`FCF YIELD` " \
                "FROM " + DB_NAME + ".`ess_idea_db_calibration_timeseries` " \
                                    "WHERE CID=" + str(cid)

        cols = ['CID', 'Date', 'Ticker', 'PX', 'P/E', 'EV/EBITDA', 'EV/Sales', 'DVD yield', 'FCF yield']
        res = wic.optimized_execute(query)
        df = pd.DataFrame(res, columns=cols)
        df['Date'] = df['Date'].apply(lambda x: pd.to_datetime(x))
        return df

    @staticmethod
    def get_ess_idea_db_wic_adj_px():
        query = "SELECT A.DealKey,  A.Timestamp, A.`Alpha Upside`, A.`Alpha Downside` " \
                "FROM " + DB_NAME + ".`ess_idea_db_wic_adj_px` AS A " \
                                    "INNER JOIN ( " \
                                    "SELECT DealKey, max(B.Timestamp) as MaxDate " \
                                    "FROM " + DB_NAME + ".`ess_idea_db_wic_adj_px` AS B " \
                                                        "GROUP BY DealKey" \
                                                        ") AS C ON A.DealKey = C.DealKey and A.Timestamp = C.MaxDate "

        cols = ['DealKey', 'Timestamp', 'Up Price (WIC)', 'Down Price (WIC)']
        res = wic.optimized_execute(query)
        df = pd.DataFrame(res, columns=cols)
        df['Timestamp'] = df['Timestamp'].apply(lambda x: pd.to_datetime(x))
        return df
    
    @staticmethod
    def log(report, message):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            if message is None: message = 'N.A.'
            if report is None: report = 'N.A.'
            query = "INSERT INTO " + DB_NAME + ".log" + " VALUES (" + ",".join(
                ["'" + timestamp + "'", "'" + report + "'", "'" + message + "'"]) + ")"
            wic.optimized_execute(query, commit=True)
        except Exception as e:
            print(e)
            print(timestamp + ' logging failed ') 
        
        
class NorthPoint():
    ''' NorthPoint Class to access tables from NorthPoint Databases '''

    @staticmethod
    def test():
        statpro_df = Wic.get_statpro_df()
        pcvh_df = NorthPoint.get_position_calculated_values_history_for_statpro()

        # Merge the 2 Dataframes on TradeDate, SecName and TradeGroup
        statpro_df.to_csv('StatPro_df.csv')
        pcvh_df.to_csv('PCVH_df.csv')
        merged_df = pd.merge(statpro_df, pcvh_df, how='outer', on=['TradeDate', 'SecurityName', 'TradeGroup', 'FundName']).groupby(['TradeDate', 'SecurityName', 'TradeGroup','Contribution', 'PositionId', 'Price', 'SecurityType','FundName']).count().reset_index()
        merged_df.to_csv('PCVH_STATPRO_MERGED_DF.csv')


    @staticmethod
    def get_position_calculated_values_history_for_statpro(start_date_yyyy_mm_dd=None, end_date_yyyy_mm_dd=None):

        #Get the PVCH Dataframe for the Period 2013-2015
        query = "Select PCVH.TradeDate as TradeDate, PCVH.TradeGroup as TradeGroup, PCVH.PositionId, PCVH.Price, SPT.SecName as SecurityName, SPT.SecType as SecurityType, " \
                "CASE F.FundName " \
                "WHEN 'Columbia' THEN 'CAM' "\
                "WHEN 'Litman Gregory' THEN 'LG' "\
                "WHEN 'The Arbitrage Credit Opportunities Fund' THEN 'TACO' "\
                "WHEN 'The Arbitrage Event-Driven Fund' THEN 'AED' "\
                "WHEN 'The Arbitrage Fund' THEN 'ARB' "\
                "WHEN 'The Arbitrage Tactical Equity Fund' THEN 'TAQ' "\
                "END "\
                "as FundName from PnLAppDb.dbo.PositionCalculatedValuesHistory as PCVH \
                 INNER JOIN SecurityMaster.dbo.SecurityPivotTable as SPT \
                 ON \
                 PCVH.SecurityID = SPT.ID \
                 INNER JOIN dbo.Funds as F ON PCVH.Portfolio = F.FundID\
                 WHERE TradeDate BETWEEN '2013-01-01' AND '2016-01-01' ORDER BY TradeDate"

        pcvh_df = pd.read_sql_query(query, connections['NorthPoint-PnLAppDb'])

        return pcvh_df

# NorthPoint.test()