@staticmethod
    def get_tradegroups_attribution_to_fund_nav():
        today = datetime.datetime.today().strftime('%Y%m%d')
        #First, delete the information from the database if yesterday's data exists
        max_date_bps = dbutils.wic.get_max_date('tradegroup_attribution_to_fund_nav_bps')
        max_date_dol = dbutils.wic.get_max_date('tradegroup_attribution_to_fund_nav_dollar')
        if max_date_bps.strftime('%Y%m%d') == today:
            dbutils.wic.delete_table_by_certain_date(today, 'tradegroup_attribution_to_fund_nav_bps')
        if max_date_dol.strftime('%Y%m%d') == today:
            dbutils.wic.delete_table_by_certain_date(today, 'tradegroup_attribution_to_fund_nav_dollar')

        df = dbutils.wic.get_tradegroups_snapshot()

        df['EndDate'] = df['EndDate'].apply(lambda x: x if x is None else pd.to_datetime(x).strftime('%Y-%m-%d'))
        df['InceptionDate'] = df['InceptionDate'].apply(lambda x: x if x is None else pd.to_datetime(x).strftime('%Y-%m-%d'))
        #Do not want tradegroups closed before the start of the current year
        #df_reduced = df[df["EndDate"].dt.year >= today.year] #Takes out deals like cash that don't have date- wrong

        metrics2include = [('P&L(bps)', 'ITD'), ('P&L($)', 'ITD'),
                           ('P&L(bps)', 'YTD'), ('P&L($)', 'YTD'),
                           ('P&L(bps)', 'QTD'), ('P&L($)', 'QTD'),
                           ('P&L(bps)', 'MTD'), ('P&L($)', 'MTD'),
                           ('P&L(bps)', '30D'), ('P&L($)', '30D'),
                           ('P&L(bps)', '5D'), ('P&L($)', '5D'),
                           ('P&L(bps)', '1D'), ('P&L($)', '1D')]

        metric2display_name = {'P&L(bps)': '', 'P&L($)': ''}
        metric2unit = {'P&L(bps)': 'bps', 'P&L($)': '$'}

        # unjsonify metrics, and append columns
        metrics_df = pd.DataFrame([dfutils.json2row(json) for json in df['Metrics in NAV JSON']])
        metrics_df.index = df.index


        for (metric, period) in metrics2include:
            unit = metric2unit[metric]
            disp_name = metric2display_name[metric]
            display_colname = disp_name + ' ' + period + '(' + unit + ')'
            df[display_colname] = metrics_df[metric + '|' + period]

        del df['Metrics in NAV JSON']; del df['Metrics in NAV notes JSON']
        del df['Metrics in Bet JSON']; del df['Metrics in Bet notes JSON']
        del df['Analyst']

        sleeve2code = {'Merger Arbitrage': 'M&A', 'Equity Special Situations': 'ESS',
                       'Opportunistic' : 'OPP', 'Forwards':'FWD', 'Credit Opportunities':'CREDIT'}

        df['Sleeve'] = df['Sleeve'].apply(lambda x: sleeve2code[x] if x in sleeve2code else x)
        df = df[(~pd.isnull(df[' YTD($)']))]  # don't show null ytds. i.e. tradegroups closed before year started
        df['Date'] = today

        base_cols = ['Date', 'Fund', 'Sleeve', 'TradeGroup', 'LongShort', 'InceptionDate', 'EndDate', 'Status']
        bps_cols = [' ITD(bps)', ' YTD(bps)', ' QTD(bps)', ' MTD(bps)', ' 30D(bps)', ' 5D(bps)', ' 1D(bps)']
        dollar_cols = [' ITD($)', ' YTD($)', ' QTD($)', ' MTD($)', ' 30D($)', ' 5D($)', ' 1D($)']
        bps_df = df[base_cols+bps_cols].sort_values(by=' YTD(bps)')
        bps_df.rename(columns={' ITD(bps)': 'ITD_bps', ' YTD(bps)': 'YTD_bps', ' QTD(bps)': 'QTD_bps',
                               ' MTD(bps)': 'MTD_bps', ' 30D(bps)': '30D_bps', ' 5D(bps)': '5D_bps',
                               ' 1D(bps)': '1D_bps'}, inplace=True)
        dollar_df = df[base_cols+dollar_cols].sort_values(by=' YTD($)')
        dollar_df.rename(columns={' ITD($)': 'ITD_Dollar', ' YTD($)': 'YTD_Dollar', ' QTD($)': 'QTD_Dollar',
                                  ' MTD($)': 'MTD_Dollar', ' 30D($)': '30D_Dollar', ' 5D($)': '5D_Dollar',
                                  ' 1D($)': '1D_Dollar'}, inplace=True)

        #Import the new additions to the database
        con = dbutils.wic_aws_engine.connect()
        dollar_df.to_sql(con=con, schema='wic', name='tradegroup_attribution_to_fund_nav_dollar',
                              if_exists='append', chunksize=1000, index=False)
        time.sleep(5)

        bps_df.to_sql(con=con, schema='wic', name='tradegroup_attribution_to_fund_nav_bps',
                              if_exists='append', chunksize=1000, index=False)

        time.sleep(5)
        con.close()
        print('Inserted')
        return 'Inserted'

    @staticmethod
    def daily_pnl_df(PCVH):
        slicer = dfutils.df_slicer()
        today = datetime.datetime.today()
        yesterday = slicer.prev_n_business_days(1, today).strftime('%Y%m%d')

        #First, delete the information from the database if yesterday's data already exists
        max_date_pnl = dbutils.wic.get_max_date('tradegroup_overall_pnl')
        if max_date_pnl.strftime('%Y%m%d') == yesterday:
            dbutils.wic.delete_table_by_certain_date(yesterday, 'ticker_pnl_breakdown')
            dbutils.wic.delete_table_by_certain_date(yesterday, 'tradegroup_overall_pnl')

        fund_tg_combos = dbutils.tradar.get_active_tradegroups_from_tradar()

        limit_to_tradegroups = list(fund_tg_combos['TradeGroup'].unique())
        limit_to_funds = list(fund_tg_combos['Fund'].unique())

        #Get the start date for the queries (30 days back because this takes into account rolling volatility)
        start_date = datetime.datetime.strptime(max_date_pnl.strftime('%Y%m%d'), '%Y%m%d')
        start_date_yyyy_mm_dd = slicer.prev_n_business_days(30, start_date)

        PCVH = PCVH[PCVH.Date > start_date_yyyy_mm_dd]
        FUND_NAV_DF = dbutils.northpoint.get_NAV_df_by_date(start_date_yyyy_mm_dd=start_date_yyyy_mm_dd)
        TG_PNL_DF = dbutils.wic.get_tradegroups_total_pnl(start_date_yyyy_mm_dd=start_date_yyyy_mm_dd)
        SECURITIES_PNL_DF = dbutils.tradar.get_securities_pnl_by_tradegroup_and_fund_current(limit_to_tradegroups=limit_to_tradegroups,
                                                                              limit_to_funds=limit_to_funds,
                                                                              is_tg_names_in_tradar_format=True,
                                                                              start_date_yyyymmdd=start_date_yyyy_mm_dd,
                                                                              rollup_pnl=True)
        SECURITIES_PNL_DF['TradeGroup'] = SECURITIES_PNL_DF['TradeGroup'].apply(lambda x: x.upper())
        TG_PNL_DF['TradeGroup'] = TG_PNL_DF['TradeGroup'].apply(lambda x: x.upper())
        PCVH['TradeGroup'] = PCVH['TradeGroup'].apply(lambda x: x.upper())
        fund_tg_combos['TradeGroup'] = fund_tg_combos['TradeGroup'].apply(lambda x: x.upper())

        final_pnl_df = pd.DataFrame()
        final_ticker_pnl_df = pd.DataFrame()
        for i in range(0, len(fund_tg_combos['TradeGroup'])):
            bps_dollar_pnl_df = pd.DataFrame()
            bps_dollar_ticker_pnl_df = pd.DataFrame()
            tradar_tg_name = fund_tg_combos['TradeGroup'][i]
            fund = fund_tg_combos['Fund'][i]

            tg_pcvh = PCVH[(PCVH.TradeGroup == tradar_tg_name) & (PCVH.FundCode == fund)].copy()
            fund_nav_df = FUND_NAV_DF[FUND_NAV_DF.FundCode == fund].copy()
            securities_pnl_df = SECURITIES_PNL_DF[(SECURITIES_PNL_DF.TradeGroup == tradar_tg_name)
                                                  & (SECURITIES_PNL_DF.Fund == fund)].copy()
            tg_pnl_df = TG_PNL_DF[(TG_PNL_DF.TradeGroup == tradar_tg_name) & (TG_PNL_DF.Fund == fund)].copy()

            options_pnl_df = securities_pnl_df[securities_pnl_df['SecType'] ==
                                               'Option'][['Date', 'Total P&L']].groupby('Date').sum().reset_index().copy()
            options_pnl_df = options_pnl_df.iloc[np.trim_zeros(options_pnl_df['Total P&L']).index].copy()

            if len(options_pnl_df) >= 1:
                options_pnl_df = options_pnl_df.rename(columns={'Total P&L': 'Options_PnL_Dollar'})
                options_pnl_df['Fund'] = fund
                options_pnl_df['TradeGroup'] = tradar_tg_name
                options_pnl_df = pd.merge(options_pnl_df, fund_nav_df, how='inner', on=['Date'])
                options_pnl_df['Shifted Capital'] = options_pnl_df['NAV'].shift(1)
                if pd.isnull(options_pnl_df['Shifted Capital'].iloc[0]):
                    options_pnl_df.loc[0, 'Shifted Capital'] = options_pnl_df['NAV'].iloc[0]
                options_pnl_df['Shifted Capital'] = options_pnl_df['Shifted Capital'].apply(lambda x: np.nan if x == 0 else x)
                options_pnl_df['Shifted Forward-Filled Capital'] = options_pnl_df['Shifted Capital'].ffill()
                options_pnl_df['Options_PnL_bps'] = 1e4*(options_pnl_df["Options_PnL_Dollar"]/
                                                         options_pnl_df["Shifted Forward-Filled Capital"]).replace([np.inf,
                                                                                                                    -np.inf],
                                                                                                                   np.nan)
                if bps_dollar_pnl_df.empty:
                    bps_dollar_pnl_df = bps_dollar_pnl_df.append(options_pnl_df[['Date', 'Fund', 'TradeGroup',
                                                                             'Options_PnL_Dollar', 'Options_PnL_bps']])
                else:
                    bps_dollar_pnl_df = pd.merge(bps_dollar_pnl_df, options_pnl_df[['Date', 'Fund', 'TradeGroup',
                                                                             'Options_PnL_Dollar', 'Options_PnL_bps']],
                                                 how='outer', on=['Date', 'Fund', 'TradeGroup'])

            #P&L Breakdown by Ticker -- to be kept in a separate db
            tg_securities = securities_pnl_df[securities_pnl_df['SecType'] != 'Option']['Ticker'].unique()
            for ticker in tg_securities:
                if ticker == 'USD':
                    continue
                tkr_pnl_df = securities_pnl_df[securities_pnl_df['Ticker'] == ticker].copy()
                #tkr_pnl_df = tkr_pnl_df.loc[np.trim_zeros(tkr_pnl_df['Total P&L']).index].copy()
                if len(tkr_pnl_df) < 1:
                    continue
                tkr_pnl_df = tkr_pnl_df.rename(columns={'Total P&L': 'Ticker_PnL_Dollar'})
                tkr_pnl_df['Fund'] = fund
                tkr_pnl_df['TradeGroup'] = tradar_tg_name
                tkr_pnl_df = pd.merge(tkr_pnl_df, fund_nav_df, how='inner', on=['Date'])
                tkr_pnl_df['Shifted Capital'] = tkr_pnl_df['NAV'].shift(1)
                if pd.isnull(tkr_pnl_df['Shifted Capital'].iloc[0]):
                    tkr_pnl_df.loc[0, 'Shifted Capital'] = tkr_pnl_df['NAV'].iloc[0]
                tkr_pnl_df['Shifted Capital'] = tkr_pnl_df['Shifted Capital'].apply(lambda x: np.nan if x == 0 else x)
                tkr_pnl_df['Shifted Forward-Filled Capital'] = tkr_pnl_df['Shifted Capital'].ffill()
                tkr_pnl_df['Ticker_PnL_bps'] = 1e4*(tkr_pnl_df["Ticker_PnL_Dollar"]/
                                                         tkr_pnl_df["Shifted Forward-Filled Capital"]).replace([np.inf,
                                                                                                                -np.inf],
                                                                                                                np.nan)
                bps_dollar_ticker_pnl_df = bps_dollar_ticker_pnl_df.append(tkr_pnl_df[['Date', 'Fund', 'TradeGroup',
                                                                             'Ticker_PnL_Dollar', 'Ticker_PnL_bps', 'Ticker']])

            final_ticker_pnl_df = final_ticker_pnl_df.append(bps_dollar_ticker_pnl_df)

            #Daily PnL
            daily_pnl_df = tg_pnl_df[['Date', 'Fund', 'TradeGroup', 'Total P&L']]
            if len(daily_pnl_df) >= 1:
                daily_pnl_df = daily_pnl_df.rename(columns={'Total P&L': 'Daily_PnL_Dollar'})
                daily_pnl_df['Fund'] = fund
                daily_pnl_df['TradeGroup'] = tradar_tg_name
                daily_pnl_df = pd.merge(daily_pnl_df, fund_nav_df, how='inner', on=['Date'])
                daily_pnl_df['Shifted Capital'] = daily_pnl_df['NAV'].shift(1)
                if pd.isnull(daily_pnl_df['Shifted Capital'].iloc[0]):
                    daily_pnl_df.loc[0, 'Shifted Capital'] = daily_pnl_df['NAV'].iloc[0]
                daily_pnl_df['Shifted Capital'] = daily_pnl_df['Shifted Capital'].apply(lambda x: np.nan if x == 0 else x)
                daily_pnl_df['Shifted Forward-Filled Capital'] = daily_pnl_df['Shifted Capital'].ffill()
                daily_pnl_df['Daily_PnL_bps'] = 1e4*(daily_pnl_df["Daily_PnL_Dollar"]/
                                                         daily_pnl_df["Shifted Forward-Filled Capital"]).replace([np.inf, -np.inf],
                                                                                                                 np.nan)
                if bps_dollar_pnl_df.empty:
                    bps_dollar_pnl_df = bps_dollar_pnl_df.append(daily_pnl_df[['Date', 'Fund', 'TradeGroup',
                                                                             'Daily_PnL_Dollar', 'Daily_PnL_bps']])
                else:
                    bps_dollar_pnl_df = pd.merge(bps_dollar_pnl_df, daily_pnl_df[['Date', 'Fund', 'TradeGroup',
                                                                             'Daily_PnL_Dollar', 'Daily_PnL_bps']], how='outer',
                                                 on=['Date', 'Fund', 'TradeGroup'])

            rolling_vol_df = pd.DataFrame()
            if len(tg_pcvh) > 0:
                    tg_capital_df = dfutils.mktval_df(tg_pcvh)
                    roc_df = pd.merge(tg_pnl_df, tg_capital_df[['Date', 'Bet GrossMktVal']], how='inner', on=['Date'])
                    if len(roc_df) >= 1:
                        roc_df['Shifted Capital'] = roc_df['Bet GrossMktVal'].shift(1)
                        if pd.isnull(roc_df['Shifted Capital'].iloc[0]):
                            roc_df.loc[0, 'Shifted Capital'] = roc_df['Bet GrossMktVal'].iloc[0]
                        roc_df['Shifted Capital'] = roc_df['Shifted Capital'].apply(lambda x: np.nan if x == 0 else x)
                        roc_df['Shifted Forward-Filled Capital'] = roc_df['Shifted Capital'].ffill()
                        roc_df['PnL_Over_Cap_bps'] = 1e4*(roc_df["Total P&L"]/
                                                   roc_df["Shifted Forward-Filled Capital"]).replace([np.inf, -np.inf],
                                                                                                     np.nan)
                        roc_df['Rolling_30D_PnL_Vol'] = np.sqrt(252)*(roc_df['PnL_Over_Cap_bps']/100.0).rolling(window=30).std()
                        if len(roc_df['Rolling_30D_PnL_Vol'].dropna()) > 0:
                            rolling_vol_df[['Date', 'Fund', 'TradeGroup', 'Rolling_30D_PnL_Vol']] = roc_df[
                                ["Date", "Fund", "TradeGroup", "Rolling_30D_PnL_Vol"]].dropna()

                            if bps_dollar_pnl_df.empty:
                                bps_dollar_pnl_df = bps_dollar_pnl_df.append(rolling_vol_df[['Date', 'Fund', 'TradeGroup',
                                                                                             'Rolling_30D_PnL_Vol']])
                            else:
                                bps_dollar_pnl_df = bps_dollar_pnl_df.merge(rolling_vol_df[['Date', 'Fund',
                                                                                            'TradeGroup',
                                                                                            'Rolling_30D_PnL_Vol']],
                                                                            how='outer', on=['Date', 'Fund', 'TradeGroup'])

                        if bps_dollar_pnl_df.empty:
                            bps_dollar_pnl_df = bps_dollar_pnl_df.append(roc_df[['Date', 'Fund', 'TradeGroup',
                                                                             'PnL_Over_Cap_bps']])
                        else:
                            bps_dollar_pnl_df = bps_dollar_pnl_df.merge(roc_df[['Date', 'Fund', 'TradeGroup', 'PnL_Over_Cap_bps']],
                                                                        how='outer', on=['Date', 'Fund', 'TradeGroup'])

            final_pnl_df = final_pnl_df.append(bps_dollar_pnl_df)

        overall_pnl_additions = final_pnl_df[final_pnl_df.Date > start_date]
        ticker_pnl_additions = final_ticker_pnl_df[final_ticker_pnl_df.Date > start_date]

        con = dbutils.aws_engine.connect()
        ticker_pnl_additions.to_sql(con=con, schema='wic', name='ticker_pnl_breakdown',
                             if_exists='append', chunksize=1000, index=False)
        con.close()

        con = dbutils.aws_engine.connect()
        overall_pnl_additions.to_sql(con=con, schema='wic', name='tradegroup_overall_pnl',
                             if_exists='append', chunksize=1000, index=False)
        con.close()

        return 'P&L inserted...'

    # Following sections for drawdown calcs
    @staticmethod
    def calculate_max_drawdown_by_tradegroup(PCVH):
        start = time.time()
        today = datetime.datetime.today().strftime('%Y-%m-%d')
        PCVH['Date'] = PCVH['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))

        #Get only the active TradeGroups
        active_tgs = dbutils.wic.get_active_tradegroups()

        drawdown_df = pd.DataFrame(columns=['Date', 'Fund', 'TradeGroup', 'Last_Date', 'NAV_Max_bps', 'NAV_Min_bps',
                                            'NAV_Last_bps', 'NAV_MaxToMin_Drawdown_bps', 'NAV_MaxToLast_Drawdown_bps',
                                            'NAV_Max_Date', 'NAV_Min_Date', 'ROC_Max_bps', 'ROC_Min_bps',
                                            'ROC_Last_bps', 'ROC_MaxToMin_Drawdown_Pct', 'ROC_MaxToLast_Drawdown_Pct',
                                            'ROC_Max_Date', 'ROC_Min_Date', 'ROMC_Max_bps',
                                            'ROMC_Min_bps', 'ROMC_Last_bps', 'ROMC_MaxToMin_Drawdown_Pct',
                                            'ROMC_MaxToLast_Drawdown_Pct', 'ROMC_Max_Date', 'ROMC_Min_Date'])

        for i in range(0,len(active_tgs)):
            fund_code = active_tgs['Fund'][i]
            tg = active_tgs['TradeGroup'][i]

            tg_overall_pnl_df = dbutils.wic.get_tradegroup_overall_pnl_data(fund_code, tg)
            pnl_df = tg_overall_pnl_df.sort_values(by='Date').reset_index(drop=True)
            pnl_df['Date'] = pnl_df['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))
            pcvh = PCVH[(PCVH.FundCode == fund_code)&(PCVH.TradeGroup == tg)].copy()

            #Calculates the Cumulative Sum of the P&L data (NAV contribution)
            pnl_df['Cum_PnL_NAV_bps'] = pnl_df['Daily_PnL_bps'].cumsum()

            #Calculates the Cumulative Sum of the P&L data (ROC and ROMC)
            cap_df = pd.DataFrame()
            if len(pcvh) > 0:
                tg_capital_df = dfutils.mktval_df(pcvh)
                cap_df = pd.merge(pnl_df, tg_capital_df[['Date', 'Bet GrossMktVal']], how='inner', on=['Date'])
                if len(cap_df) > 1:
                    cap_df['Shifted_Capital_ROC'] = cap_df['Bet GrossMktVal'].shift(1)
                    if pd.isnull(cap_df['Shifted_Capital_ROC'].iloc[0]):
                        cap_df.loc[0, 'Shifted_Capital_ROC'] = cap_df['Bet GrossMktVal'].iloc[0]
                    cap_df['Shifted_Capital_ROC'] = cap_df['Shifted_Capital_ROC'].apply(lambda x: np.nan if x == 0 else x)
                    cap_df['Shifted_FF_Capital_ROC'] = cap_df['Shifted_Capital_ROC'].ffill()
                    cap_df['PnL_Over_Cap_bps'] = 1e4*(cap_df["Total P&L"]/
                                                    cap_df["Shifted_FF_Capital_ROC"]).replace([np.inf, -np.inf], np.nan)

                    mean_cap = cap_df['Shifted_FF_Capital_ROC'].fillna(0).astype(float).mean()
                    cap_df["Cum_PnL_ROC_bps"] = (1e4*((1.0+(cap_df["PnL_Over_Cap_bps"].astype(float)/1e4)).cumprod()-1))/100
                    cap_df['Cum_PnL_ROMC_bps'] = (1e4*(cap_df['Total P&L'].cumsum()/mean_cap))/100

            #Calulates the max-min drawdown and the max-curr drawdown for NAV, ROMC, and ROMC
            if ((pnl_df.empty) or('Daily_PnL_bps' not in pnl_df.columns) or (pnl_df['Daily_PnL_bps'].isnull().all())):
                last_NAV = None
                lastdate_NAV = None
                maxret_NAV = None
                maxdate_NAV = None
                minret_NAV = None
                mindate_NAV = None
                curr_drawdown_NAV = None
                max_drawdown_NAV = None
            else:

                last_valid_index_NAV = pnl_df.Cum_PnL_NAV_bps.last_valid_index()
                last_NAV = pnl_df.Cum_PnL_NAV_bps.iloc[last_valid_index_NAV]
                lastdate_NAV = pnl_df.loc[pnl_df.Cum_PnL_NAV_bps == last_NAV]['Date'].values[0]

                maxret_NAV = pnl_df.Cum_PnL_NAV_bps.max()
                maxdate_NAV = pnl_df.loc[pnl_df.Cum_PnL_NAV_bps == maxret_NAV]['Date'].values[0]

                if maxdate_NAV == lastdate_NAV:
                    minret_NAV = None
                    mindate_NAV = None
                    curr_drawdown_NAV = None
                    max_drawdown_NAV = None
                else:
                    minret_NAV = pnl_df[pnl_df.Date > maxdate_NAV].Cum_PnL_NAV_bps.min()
                    mindate_NAV = pnl_df.loc[pnl_df.Cum_PnL_NAV_bps == minret_NAV]['Date'].values[0]
                    curr_drawdown_NAV = (maxret_NAV-last_NAV)
                    max_drawdown_NAV = (maxret_NAV-minret_NAV)

            if ((cap_df.empty) or('PnL_Over_Cap_bps' not in cap_df.columns) or (cap_df['PnL_Over_Cap_bps'].isnull().all())):
                last_ROC = None
                lastdate_ROC = None
                maxret_ROC = None
                maxdate_ROC = None
                minret_ROC = None
                mindate_ROC = None
                curr_drawdown_ROC = None
                max_drawdown_ROC = None
                last_ROMC = None
                lastdate_ROMC = None
                maxret_ROMC = None
                maxdate_ROMC = None
                minret_ROMC = None
                mindate_ROMC = None
                curr_drawdown_ROMC = None
                max_drawdown_ROMC = None
            else:
                last_valid_index_ROC = cap_df.Cum_PnL_ROC_bps.last_valid_index()
                last_ROC = cap_df.Cum_PnL_ROC_bps.iloc[last_valid_index_ROC]
                lastdate_ROC = cap_df.loc[cap_df.Cum_PnL_ROC_bps == last_ROC]['Date'].values[0]
                maxret_ROC = cap_df.Cum_PnL_ROC_bps.max()
                maxdate_ROC = cap_df.loc[cap_df.Cum_PnL_ROC_bps == maxret_ROC]['Date'].values[0]

                if maxdate_ROC == lastdate_ROC:
                    minret_ROC = None
                    mindate_ROC = None
                    curr_drawdown_ROC = None
                    max_drawdown_ROC = None
                else:
                    minret_ROC = cap_df[cap_df.Date > maxdate_ROC].Cum_PnL_ROC_bps.min()
                    mindate_ROC = cap_df.loc[cap_df.Cum_PnL_ROC_bps == minret_ROC]['Date'].values[0]
                    curr_drawdown_ROC = (((1+(maxret_ROC/100))-(1+(last_ROC/100)))/(1+(maxret_ROC/100)))*100
                    max_drawdown_ROC = (((1+(maxret_ROC/100))-(1+(minret_ROC/100)))/(1+(maxret_ROC/100)))*100

                last_valid_index_ROMC = cap_df.Cum_PnL_ROMC_bps.last_valid_index()
                last_ROMC = cap_df.Cum_PnL_ROMC_bps.iloc[last_valid_index_ROMC]
                lastdate_ROMC = cap_df.loc[cap_df.Cum_PnL_ROMC_bps == last_ROMC]['Date'].values[0]
                maxret_ROMC = cap_df.Cum_PnL_ROMC_bps.max()
                maxdate_ROMC = cap_df.loc[cap_df.Cum_PnL_ROMC_bps == maxret_ROMC]['Date'].values[0]

                if maxdate_ROMC == lastdate_ROMC:
                    minret_ROMC = None
                    mindate_ROMC = None
                    curr_drawdown_ROMC = None
                    max_drawdown_ROMC = None
                else:
                    minret_ROMC = cap_df[cap_df.Date > maxdate_ROMC].Cum_PnL_ROMC_bps.min()
                    mindate_ROMC = cap_df.loc[cap_df.Cum_PnL_ROMC_bps == minret_ROMC]['Date'].values[0]
                    curr_drawdown_ROMC = (((1+(maxret_ROMC/100))-(1+(last_ROMC/100)))/(1+(maxret_ROMC/100))*100)
                    max_drawdown_ROMC = (((1+(maxret_ROMC/100))-(1+(minret_ROMC/100)))/(1+(maxret_ROMC/100))*100)

            #Returns the results and the corresponding dates as a pandas series for simplicity right now
            drawdown_df.loc[i, 'Date'] = today
            drawdown_df.loc[i, 'Fund'] = fund_code
            drawdown_df.loc[i, 'TradeGroup'] = tg
            drawdown_df.loc[i, 'Last_Date'] = None if lastdate_NAV is None else pd.to_datetime(str(lastdate_NAV)).strftime('%Y-%m-%d')
            drawdown_df.loc[i, 'NAV_Max_bps'] = maxret_NAV
            drawdown_df.loc[i, 'NAV_Last_bps'] = last_NAV
            drawdown_df.loc[i, 'NAV_Min_bps'] = minret_NAV
            drawdown_df.loc[i, 'NAV_MaxToMin_Drawdown_bps'] = max_drawdown_NAV
            drawdown_df.loc[i, 'NAV_MaxToLast_Drawdown_bps'] = curr_drawdown_NAV
            drawdown_df.loc[i, 'NAV_Max_Date'] = None if maxdate_NAV is None else pd.to_datetime(str(maxdate_NAV)).strftime('%Y-%m-%d')
            drawdown_df.loc[i, 'NAV_Min_Date'] = None if mindate_NAV is None else pd.to_datetime(str(mindate_NAV)).strftime('%Y-%m-%d')
            drawdown_df.loc[i, 'ROC_Max_bps'] = maxret_ROC
            drawdown_df.loc[i, 'ROC_Last_bps'] = last_ROC
            drawdown_df.loc[i, 'ROC_Min_bps'] = minret_ROC
            drawdown_df.loc[i, 'ROC_MaxToMin_Drawdown_Pct'] = max_drawdown_ROC
            drawdown_df.loc[i, 'ROC_MaxToLast_Drawdown_Pct'] = curr_drawdown_ROC
            drawdown_df.loc[i, 'ROC_Max_Date'] = None if maxdate_ROC is None else pd.to_datetime(str(maxdate_ROC)).strftime('%Y-%m-%d')
            drawdown_df.loc[i, 'ROC_Min_Date'] = None if mindate_ROC is None else pd.to_datetime(str(mindate_ROC)).strftime('%Y-%m-%d')
            #drawdown_df.loc[i, 'ROC_Last_Date'] = None if lastdate_ROC is None else pd.to_datetime(str(lastdate_ROC)).strftime('%Y-%m-%d')
            drawdown_df.loc[i, 'ROMC_Max_bps'] = maxret_ROMC
            drawdown_df.loc[i, 'ROMC_Last_bps'] = last_ROMC
            drawdown_df.loc[i, 'ROMC_Min_bps'] = minret_ROMC
            drawdown_df.loc[i, 'ROMC_MaxToMin_Drawdown_Pct'] = max_drawdown_ROMC
            drawdown_df.loc[i, 'ROMC_MaxToLast_Drawdown_Pct'] = curr_drawdown_ROMC
            drawdown_df.loc[i, 'ROMC_Max_Date'] = None if maxdate_ROMC is None else pd.to_datetime(str(maxdate_ROMC)).strftime('%Y-%m-%d')
            drawdown_df.loc[i, 'ROMC_Min_Date'] = None if mindate_ROMC is None else pd.to_datetime(str(mindate_ROMC)).strftime('%Y-%m-%d')
            #drawdown_df.loc[i, 'ROMC_Last_Date'] = None if lastdate_ROMC is None else pd.to_datetime(str(lastdate_ROMC)).strftime('%Y-%m-%d')

        con = dbutils.wic_aws_engine.connect()
        drawdown_df.to_sql(con=con, schema='wic', name='risk_tradegroup_drawdown', if_exists='append', chunksize=1000, index=False)
        time.sleep(4)
        con.close()
        dbutils.wic.log('DAILY CALC', 'Done Calculating TradeGroup drawdown')
        print('Done calculating Tradegroup drawdown')


    @staticmethod
    def calculate_bucket_drawdown():
        today = datetime.datetime.today().strftime('%Y-%m-%d')

        #Collects all the data to perform the calcs
        bucket_pnl = dbutils.wic.get_buckets_pnl_cache() #Get the Daily PnL for the buckets of each fund
        fund_nav_df = dbutils.northpoint.get_NAV_df_by_date() #Fund NAV
        bucket_bet_capital = dbutils.wic.get_buckets_capital_cache() #Get the GrossMktVal for the buckets of each fund

        #Takes the daily tradegroup dollar P&L and converts it to bps and then calculates the cumulative sum -- NAV
        funds = ['ARB', 'AED', 'CAM', 'TACO', 'TAQ', 'LEV', 'LG', 'MACO', 'MALT', 'WED']

        bucket_drawdown_df = pd.DataFrame()
        for fund_code in funds:
            buckets = ['Merger Arbitrage', 'Equity Special Situations', 'Credit Opportunities',
                       'Opportunistic', 'Forwards','Break']
            fund_nav = fund_nav_df[fund_nav_df.FundCode == fund_code].copy()
            for bckt in buckets:
                bucket_pnl_df = bucket_pnl[(bucket_pnl['Fund'] == fund_code) & (bucket_pnl['Bucket'] == bckt)][['Date', 'Total P&L']].sort_values(by='Date').copy()
                if len(bucket_pnl_df) == 0:
                    continue
                bucket_capital_df = bucket_bet_capital[(bucket_bet_capital['Fund'] == fund_code) & (bucket_bet_capital['Bucket'] == bckt)][['Date', 'GrossMktVal']].sort_values(by='Date').copy()
                pnl_df = pd.merge(bucket_pnl_df, fund_nav[['Date', 'NAV']], how='inner', on=['Date'])
                pnl_df['Shifted_Capital_NAV'] = pnl_df['NAV'].shift(1)
                if pd.isnull(pnl_df['Shifted_Capital_NAV'].iloc[0]):
                    pnl_df.loc[0, 'Shifted_Capital_NAV'] = pnl_df['NAV'].iloc[0]
                pnl_df['Shifted_Capital_NAV'] = pnl_df['Shifted_Capital_NAV'].apply(lambda x: np.nan if x == 0 else x)
                pnl_df['Shifted_FF_Capital_NAV'] = pnl_df['Shifted_Capital_NAV'].ffill()
                pnl_df['Daily_PnL_bps'] = 1e4*(pnl_df["Total P&L"]/pnl_df["Shifted_FF_Capital_NAV"]).replace([np.inf, -np.inf], np.nan)
                pnl_df['Cum_PnL_NAV_bps'] = pnl_df['Daily_PnL_bps'].cumsum()

                ## bug fix: only show chart when p&l after we started saving down alpha/hedge (used for capital calculation)
                bucket_pnl_usd_df = bucket_pnl_df[bucket_pnl_df['Date']>=bucket_capital_df[~pd.isnull(bucket_capital_df['GrossMktVal'])]['Date'].min()].copy()

                #Takes the daily bucket dollar P&L and calculates ROC and ROMC (bps)
                cap_df = pd.DataFrame()
                cap_df = pd.merge(bucket_pnl_usd_df, bucket_capital_df, how='inner', on=['Date'])
                if len(cap_df) > 1:
                    cap_df['Shifted_Capital_ROC'] = cap_df['GrossMktVal'].shift(1)
                    if pd.isnull(cap_df['Shifted_Capital_ROC'].iloc[0]):
                        cap_df.loc[0, 'Shifted_Capital_ROC'] = cap_df['GrossMktVal'].iloc[0]
                    cap_df['Shifted_Capital_ROC'] = cap_df['Shifted_Capital_ROC'].apply(lambda x: np.nan if x == 0 else x)
                    cap_df['Shifted_FF_Capital_ROC'] = cap_df['Shifted_Capital_ROC'].ffill()
                    cap_df['PnL_Over_Cap_bps'] = 1e4*(cap_df["Total P&L"]/
                                                    cap_df["Shifted_FF_Capital_ROC"]).replace([np.inf, -np.inf], np.nan)

                    mean_cap = cap_df['Shifted_FF_Capital_ROC'].fillna(0).astype(float).mean()
                    cap_df["Cum_PnL_ROC_bps"] = (1e4*((1.0+(cap_df["PnL_Over_Cap_bps"].astype(float)/1e4)).cumprod()-1))/100
                    cap_df['Cum_PnL_ROMC_bps'] = (1e4*(cap_df['Total P&L'].cumsum()/mean_cap))/100


                #combined_df = pd.merge(pnl_df, bucket_df, how='outer', on=['Date'])

                #Calulates the max-min drawdown and the max-curr drawdown for NAV, ROMC, and ROMC
                last_valid_index_NAV = pnl_df.Cum_PnL_NAV_bps.last_valid_index()
                last_NAV = pnl_df.Cum_PnL_NAV_bps.iloc[last_valid_index_NAV]
                lastdate_NAV = pnl_df.loc[pnl_df.Cum_PnL_NAV_bps == last_NAV]['Date'].values[0]

                maxret_NAV = pnl_df.Cum_PnL_NAV_bps.max()
                maxdate_NAV = pnl_df.loc[pnl_df.Cum_PnL_NAV_bps == maxret_NAV]['Date'].values[0]

                if maxdate_NAV == lastdate_NAV:
                    minret_NAV = None
                    mindate_NAV = None
                    curr_drawdown_NAV = None
                    max_drawdown_NAV = None
                else:
                    minret_NAV = pnl_df[pnl_df.Date > maxdate_NAV].Cum_PnL_NAV_bps.min()
                    mindate_NAV = pnl_df.loc[pnl_df.Cum_PnL_NAV_bps == minret_NAV]['Date'].values[0]
                    curr_drawdown_NAV = (maxret_NAV-last_NAV)
                    max_drawdown_NAV = (maxret_NAV-minret_NAV)

                if ('PnL_Over_Cap_bps' not in cap_df.columns):#|(combined_df.PnL_Over_Cap_bps.isnull().values.all()):
                    last_ROC = None
                    lastdate_ROC = None
                    maxret_ROC = None
                    maxdate_ROC = None
                    minret_ROC = None
                    mindate_ROC = None
                    curr_drawdown_ROC = None
                    max_drawdown_ROC = None
                    last_ROMC = None
                    lastdate_ROMC = None
                    maxret_ROMC = None
                    maxdate_ROMC = None
                    minret_ROMC = None
                    mindate_ROMC = None
                    curr_drawdown_ROMC = None
                    max_drawdown_ROMC = None
                else:
                    last_valid_index_ROC = cap_df.Cum_PnL_ROC_bps.last_valid_index()
                    last_ROC = cap_df.Cum_PnL_ROC_bps.iloc[last_valid_index_ROC]
                    lastdate_ROC = cap_df.loc[cap_df.Cum_PnL_ROC_bps == last_ROC]['Date'].values[0]
                    maxret_ROC = cap_df.Cum_PnL_ROC_bps.max()
                    maxdate_ROC = cap_df.loc[cap_df.Cum_PnL_ROC_bps == maxret_ROC]['Date'].values[0]

                    if maxdate_ROC == lastdate_ROC:
                        minret_ROC = None
                        mindate_ROC = None
                        curr_drawdown_ROC = None
                        max_drawdown_ROC = None
                    else:
                        minret_ROC = cap_df[cap_df.Date > maxdate_ROC].Cum_PnL_ROC_bps.min()
                        mindate_ROC = cap_df.loc[cap_df.Cum_PnL_ROC_bps == minret_ROC]['Date'].values[0]
                        curr_drawdown_ROC = (((1+(maxret_ROC/100))-(1+(last_ROC/100)))/(1+(maxret_ROC/100)))*100
                        max_drawdown_ROC = (((1+(maxret_ROC/100))-(1+(minret_ROC/100)))/(1+(maxret_ROC/100)))*100


                    last_valid_index_ROMC = cap_df.Cum_PnL_ROMC_bps.last_valid_index()
                    last_ROMC = cap_df.Cum_PnL_ROMC_bps.iloc[last_valid_index_ROMC]
                    lastdate_ROMC = cap_df.loc[cap_df.Cum_PnL_ROMC_bps == last_ROMC]['Date'].values[0]
                    maxret_ROMC = cap_df.Cum_PnL_ROMC_bps.max()
                    maxdate_ROMC = cap_df.loc[cap_df.Cum_PnL_ROMC_bps == maxret_ROMC]['Date'].values[0]

                    if maxdate_ROMC == lastdate_ROMC:
                        minret_ROMC = None
                        mindate_ROMC = None
                        curr_drawdown_ROMC = None
                        max_drawdown_ROMC = None
                    else:
                        minret_ROMC = cap_df[cap_df.Date > maxdate_ROMC].Cum_PnL_ROMC_bps.min()
                        mindate_ROMC = cap_df.loc[cap_df.Cum_PnL_ROMC_bps == minret_ROMC]['Date'].values[0]
                        curr_drawdown_ROMC = (((1+(maxret_ROMC/100))-(1+(last_ROMC/100)))/(1+(maxret_ROMC/100))*100)
                        max_drawdown_ROMC = (((1+(maxret_ROMC/100))-(1+(minret_ROMC/100)))/(1+(maxret_ROMC/100))*100)


                #Returns the results and the corresponding dates as a pandas series for simplicity right now
                drawdown_df = pd.Series()
                drawdown_df['Date'] = today
                drawdown_df['Fund'] = fund_code
                drawdown_df['Bucket'] = bckt
                drawdown_df['Last_Date'] = None if lastdate_NAV is None else pd.to_datetime(str(lastdate_NAV)).strftime('%Y-%m-%d')
                drawdown_df['NAV_Max_bps'] = maxret_NAV
                drawdown_df['NAV_Last_bps'] = last_NAV
                drawdown_df['NAV_Min_bps'] = minret_NAV
                drawdown_df['NAV_MaxToMin_Drawdown_bps'] = max_drawdown_NAV
                drawdown_df['NAV_MaxToLast_Drawdown_bps'] = curr_drawdown_NAV
                drawdown_df['NAV_Max_Date'] = None if maxdate_NAV is None else pd.to_datetime(str(maxdate_NAV)).strftime('%Y-%m-%d')
                drawdown_df['NAV_Min_Date'] = None if mindate_NAV is None else pd.to_datetime(str(mindate_NAV)).strftime('%Y-%m-%d')
                #drawdown_df['NAV_Last_Date'] = None if lastdate_NAV is None else pd.to_datetime(str(lastdate_NAV)).strftime('%Y-%m-%d')
                drawdown_df['ROC_Max_bps'] = maxret_ROC
                drawdown_df['ROC_Last_bps'] = last_ROC
                drawdown_df['ROC_Min_bps'] = minret_ROC
                drawdown_df['ROC_MaxToMin_Drawdown_Pct'] = max_drawdown_ROC
                drawdown_df['ROC_MaxToLast_Drawdown_Pct'] = curr_drawdown_ROC
                drawdown_df['ROC_Max_Date'] = None if maxdate_ROC is None else pd.to_datetime(str(maxdate_ROC)).strftime('%Y-%m-%d')
                drawdown_df['ROC_Min_Date'] = None if mindate_ROC is None else pd.to_datetime(str(mindate_ROC)).strftime('%Y-%m-%d')
                #drawdown_df['ROC_Last_Date'] = None if lastdate_ROC is None else pd.to_datetime(str(lastdate_ROC)).strftime('%Y-%m-%d')
                drawdown_df['ROMC_Max_bps'] = maxret_ROMC
                drawdown_df['ROMC_Last_bps'] = last_ROMC
                drawdown_df['ROMC_Min_bps'] = minret_ROMC
                drawdown_df['ROMC_MaxToMin_Drawdown_Pct'] = max_drawdown_ROMC
                drawdown_df['ROMC_MaxToLast_Drawdown_Pct'] = curr_drawdown_ROMC
                drawdown_df['ROMC_Max_Date'] = None if maxdate_ROMC is None else pd.to_datetime(str(maxdate_ROMC)).strftime('%Y-%m-%d')
                drawdown_df['ROMC_Min_Date'] = None if mindate_ROMC is None else pd.to_datetime(str(mindate_ROMC)).strftime('%Y-%m-%d')
                #drawdown_df['ROMC_Last_Date'] = None if lastdate_ROMC is None else pd.to_datetime(str(lastdate_ROMC)).strftime('%Y-%m-%d')

                bucket_drawdown_df = bucket_drawdown_df.append(drawdown_df, ignore_index=True)

        con = dbutils.wic_aws_engine.connect()
        bucket_drawdown_df.to_sql(con=con, schema='wic', name='risk_bucket_drawdown', if_exists='append', chunksize=1000, index=False)
        time.sleep(4)
        con.close()
        dbutils.wic.log('DAILY CALC', 'Done Calculating Bucket drawdown')
        print('Done calculating Bucket drawdown')



    @staticmethod
    def calculate_sleeve_drawdown():
        today = datetime.datetime.today().strftime('%Y-%m-%d')

        #Collects all the data to perform the calcs
        sleeve_pnl = dbutils.wic.get_sleeves_pnl_cache() #Get the Daily PnL for the sleeves of each fund
        fund_nav_df = dbutils.northpoint.get_NAV_df_by_date() #Fund NAV
        sleeve_bet_capital = dbutils.wic.get_sleeves_capital_cache() #Get the GrossMktVal for the sleeves of each fund

        #Takes the daily sleeve dollar P&L and converts it to bps and then calculates the cumulative sum -- NAV
        funds = ['ARB', 'AED', 'CAM', 'TACO', 'TAQ', 'LEV', 'LG', 'MACO', 'MALT', 'WED']

        sleeve_drawdown_df = pd.DataFrame()
        for fund_code in funds:
            sleeves = ['Merger Arbitrage', 'Equity Special Situations', 'Credit Opportunities',
                       'Opportunistic', 'Forwards','Break']
            fund_nav = fund_nav_df[fund_nav_df.FundCode == fund_code].copy()
            for slv in sleeves:
                sleeve_pnl_df = sleeve_pnl[(sleeve_pnl['Fund'] == fund_code) & (sleeve_pnl['Sleeve'] == slv)][['Date', 'Total P&L']].sort_values(by='Date').copy()
                if len(sleeve_pnl_df) == 0:
                    continue
                sleeve_capital_df = sleeve_bet_capital[(sleeve_bet_capital['Fund'] == fund_code) & (sleeve_bet_capital['Sleeve'] == slv)][['Date', 'GrossMktVal']].sort_values(by='Date').copy()
                pnl_df = pd.merge(sleeve_pnl_df, fund_nav[['Date', 'NAV']], how='inner', on=['Date'])
                pnl_df['Shifted_Capital_NAV'] = pnl_df['NAV'].shift(1)
                if pd.isnull(pnl_df['Shifted_Capital_NAV'].iloc[0]):
                    pnl_df.loc[0, 'Shifted_Capital_NAV'] = pnl_df['NAV'].iloc[0]
                pnl_df['Shifted_Capital_NAV'] = pnl_df['Shifted_Capital_NAV'].apply(lambda x: np.nan if x == 0 else x)
                pnl_df['Shifted_FF_Capital_NAV'] = pnl_df['Shifted_Capital_NAV'].ffill()
                pnl_df['Daily_PnL_bps'] = 1e4*(pnl_df["Total P&L"]/pnl_df["Shifted_FF_Capital_NAV"]).replace([np.inf, -np.inf], np.nan)
                pnl_df['Cum_PnL_NAV_bps'] = pnl_df['Daily_PnL_bps'].cumsum()

                ## bug fix: only show chart when p&l after we started saving down alpha/hedge (used for capital calculation)
                sleeve_pnl_usd_df = sleeve_pnl_df[sleeve_pnl_df['Date']>=sleeve_capital_df[~pd.isnull(sleeve_capital_df['GrossMktVal'])]['Date'].min()].copy()

                #Takes the daily sleeve dollar P&L and calculates ROC and ROMC (bps)
                cap_df = pd.DataFrame()
                cap_df = pd.merge(sleeve_pnl_usd_df, sleeve_capital_df, how='inner', on=['Date'])
                if len(cap_df) > 1:
                    cap_df['Shifted_Capital_ROC'] = cap_df['GrossMktVal'].shift(1)
                    if pd.isnull(cap_df['Shifted_Capital_ROC'].iloc[0]):
                        cap_df.loc[0, 'Shifted_Capital_ROC'] = cap_df['GrossMktVal'].iloc[0]
                    cap_df['Shifted_Capital_ROC'] = cap_df['Shifted_Capital_ROC'].apply(lambda x: np.nan if x == 0 else x)
                    cap_df['Shifted_FF_Capital_ROC'] = cap_df['Shifted_Capital_ROC'].ffill()
                    cap_df['PnL_Over_Cap_bps'] = 1e4*(cap_df["Total P&L"]/
                                                    cap_df["Shifted_FF_Capital_ROC"]).replace([np.inf, -np.inf], np.nan)

                    mean_cap = cap_df['Shifted_FF_Capital_ROC'].fillna(0).astype(float).mean()
                    cap_df["Cum_PnL_ROC_bps"] = (1e4*((1.0+(cap_df["PnL_Over_Cap_bps"].astype(float)/1e4)).cumprod()-1))/100
                    cap_df['Cum_PnL_ROMC_bps'] = (1e4*(cap_df['Total P&L'].cumsum()/mean_cap))/100


                #combined_df = pd.merge(pnl_df, sleeve_df, how='outer', on=['Date'])

                #Calulates the max-min drawdown and the max-curr drawdown for NAV, ROMC, and ROMC
                last_valid_index_NAV = pnl_df.Cum_PnL_NAV_bps.last_valid_index()
                last_NAV = pnl_df.Cum_PnL_NAV_bps.iloc[last_valid_index_NAV]
                lastdate_NAV = pnl_df.loc[pnl_df.Cum_PnL_NAV_bps == last_NAV]['Date'].values[0]

                maxret_NAV = pnl_df.Cum_PnL_NAV_bps.max()
                maxdate_NAV = pnl_df.loc[pnl_df.Cum_PnL_NAV_bps == maxret_NAV]['Date'].values[0]

                if maxdate_NAV == lastdate_NAV:
                    minret_NAV = None
                    mindate_NAV = None
                    curr_drawdown_NAV = None
                    max_drawdown_NAV = None
                else:
                    minret_NAV = pnl_df[pnl_df.Date > maxdate_NAV].Cum_PnL_NAV_bps.min()
                    mindate_NAV = pnl_df.loc[pnl_df.Cum_PnL_NAV_bps == minret_NAV]['Date'].values[0]
                    curr_drawdown_NAV = (maxret_NAV-last_NAV)
                    max_drawdown_NAV = (maxret_NAV-minret_NAV)

                if ('PnL_Over_Cap_bps' not in cap_df.columns):#|(combined_df.PnL_Over_Cap_bps.isnull().values.all()):
                    last_ROC = None
                    lastdate_ROC = None
                    maxret_ROC = None
                    maxdate_ROC = None
                    minret_ROC = None
                    mindate_ROC = None
                    curr_drawdown_ROC = None
                    max_drawdown_ROC = None
                    last_ROMC = None
                    lastdate_ROMC = None
                    maxret_ROMC = None
                    maxdate_ROMC = None
                    minret_ROMC = None
                    mindate_ROMC = None
                    curr_drawdown_ROMC = None
                    max_drawdown_ROMC = None
                else:
                    last_valid_index_ROC = cap_df.Cum_PnL_ROC_bps.last_valid_index()
                    last_ROC = cap_df.Cum_PnL_ROC_bps.iloc[last_valid_index_ROC]
                    lastdate_ROC = cap_df.loc[cap_df.Cum_PnL_ROC_bps == last_ROC]['Date'].values[0]
                    maxret_ROC = cap_df.Cum_PnL_ROC_bps.max()
                    maxdate_ROC = cap_df.loc[cap_df.Cum_PnL_ROC_bps == maxret_ROC]['Date'].values[0]

                    if maxdate_ROC == lastdate_ROC:
                        minret_ROC = None
                        mindate_ROC = None
                        curr_drawdown_ROC = None
                        max_drawdown_ROC = None
                    else:
                        minret_ROC = cap_df[cap_df.Date > maxdate_ROC].Cum_PnL_ROC_bps.min()
                        mindate_ROC = cap_df.loc[cap_df.Cum_PnL_ROC_bps == minret_ROC]['Date'].values[0]
                        curr_drawdown_ROC = (((1+(maxret_ROC/100))-(1+(last_ROC/100)))/(1+(maxret_ROC/100)))*100
                        max_drawdown_ROC = (((1+(maxret_ROC/100))-(1+(minret_ROC/100)))/(1+(maxret_ROC/100)))*100


                    last_valid_index_ROMC = cap_df.Cum_PnL_ROMC_bps.last_valid_index()
                    last_ROMC = cap_df.Cum_PnL_ROMC_bps.iloc[last_valid_index_ROMC]
                    lastdate_ROMC = cap_df.loc[cap_df.Cum_PnL_ROMC_bps == last_ROMC]['Date'].values[0]
                    maxret_ROMC = cap_df.Cum_PnL_ROMC_bps.max()
                    maxdate_ROMC = cap_df.loc[cap_df.Cum_PnL_ROMC_bps == maxret_ROMC]['Date'].values[0]

                    if maxdate_ROMC == lastdate_ROMC:
                        minret_ROMC = None
                        mindate_ROMC = None
                        curr_drawdown_ROMC = None
                        max_drawdown_ROMC = None
                    else:
                        minret_ROMC = cap_df[cap_df.Date > maxdate_ROMC].Cum_PnL_ROMC_bps.min()
                        mindate_ROMC = cap_df.loc[cap_df.Cum_PnL_ROMC_bps == minret_ROMC]['Date'].values[0]
                        curr_drawdown_ROMC = (((1+(maxret_ROMC/100))-(1+(last_ROMC/100)))/(1+(maxret_ROMC/100))*100)
                        max_drawdown_ROMC = (((1+(maxret_ROMC/100))-(1+(minret_ROMC/100)))/(1+(maxret_ROMC/100))*100)


                #Returns the results and the corresponding dates as a pandas series for simplicity right now
                drawdown_df = pd.Series()
                drawdown_df['Date'] = today
                drawdown_df['Fund'] = fund_code
                drawdown_df['Sleeve'] = slv
                drawdown_df['Last_Date'] = None if lastdate_NAV is None else pd.to_datetime(str(lastdate_NAV)).strftime('%Y-%m-%d')
                drawdown_df['NAV_Max_bps'] = maxret_NAV
                drawdown_df['NAV_Last_bps'] = last_NAV
                drawdown_df['NAV_Min_bps'] = minret_NAV
                drawdown_df['NAV_MaxToMin_Drawdown_bps'] = max_drawdown_NAV
                drawdown_df['NAV_MaxToLast_Drawdown_bps'] = curr_drawdown_NAV
                drawdown_df['NAV_Max_Date'] = None if maxdate_NAV is None else pd.to_datetime(str(maxdate_NAV)).strftime('%Y-%m-%d')
                drawdown_df['NAV_Min_Date'] = None if mindate_NAV is None else pd.to_datetime(str(mindate_NAV)).strftime('%Y-%m-%d')
                #drawdown_df['NAV_Last_Date'] = None if lastdate_NAV is None else pd.to_datetime(str(lastdate_NAV)).strftime('%Y-%m-%d')
                drawdown_df['ROC_Max_bps'] = maxret_ROC
                drawdown_df['ROC_Last_bps'] = last_ROC
                drawdown_df['ROC_Min_bps'] = minret_ROC
                drawdown_df['ROC_MaxToMin_Drawdown_Pct'] = max_drawdown_ROC
                drawdown_df['ROC_MaxToLast_Drawdown_Pct'] = curr_drawdown_ROC
                drawdown_df['ROC_Max_Date'] = None if maxdate_ROC is None else pd.to_datetime(str(maxdate_ROC)).strftime('%Y-%m-%d')
                drawdown_df['ROC_Min_Date'] = None if mindate_ROC is None else pd.to_datetime(str(mindate_ROC)).strftime('%Y-%m-%d')
                #drawdown_df['ROC_Last_Date'] = None if lastdate_ROC is None else pd.to_datetime(str(lastdate_ROC)).strftime('%Y-%m-%d')
                drawdown_df['ROMC_Max_bps'] = maxret_ROMC
                drawdown_df['ROMC_Last_bps'] = last_ROMC
                drawdown_df['ROMC_Min_bps'] = minret_ROMC
                drawdown_df['ROMC_MaxToMin_Drawdown_Pct'] = max_drawdown_ROMC
                drawdown_df['ROMC_MaxToLast_Drawdown_Pct'] = curr_drawdown_ROMC
                drawdown_df['ROMC_Max_Date'] = None if maxdate_ROMC is None else pd.to_datetime(str(maxdate_ROMC)).strftime('%Y-%m-%d')
                drawdown_df['ROMC_Min_Date'] = None if mindate_ROMC is None else pd.to_datetime(str(mindate_ROMC)).strftime('%Y-%m-%d')
                #drawdown_df['ROMC_Last_Date'] = None if lastdate_ROMC is None else pd.to_datetime(str(lastdate_ROMC)).strftime('%Y-%m-%d')

                sleeve_drawdown_df = sleeve_drawdown_df.append(drawdown_df, ignore_index=True)

        con = dbutils.wic_aws_engine.connect()
        sleeve_drawdown_df.to_sql(con=con, schema='wic', name='risk_sleeve_drawdown', if_exists='append', chunksize=1000, index=False)
        time.sleep(4)
        con.close()
        dbutils.wic.log('DAILY CALC', 'Done Calculating Sleeve drawdown')
        print('Done calculating Sleeve drawdown')