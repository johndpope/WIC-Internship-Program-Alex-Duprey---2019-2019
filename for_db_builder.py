#The functions below goes in dfutils
def df2row(pivot_col, df):
    dfcols = [c for c in df.columns if c != pivot_col]
    cols = [colname + '|' + period for (colname, period) in itertools.product(dfcols, df[pivot_col])]
    cols2vals = {c:None for c in cols}

    for idx in df.index:
        row = df.loc[idx]
        pivot = row[pivot_col]
        for cln in dfcols:
            key = cln + '|' + pivot
            cols2vals[key] = row[cln]
    res = pd.Series()
    for cln in cols:
        res[cln] = cols2vals[cln]
    return res

def json2row(json):
    df = pd.read_json(json)
    return dfutils.df2row('Period', df)


def mktval_df(pcvh_df):
    float_cols = ['Alpha Exposure', 'AlphaHedge Exposure', 'Hedge Exposure',
                  'Alpha NetMktVal', 'AlphaHedge NetMktVal', 'Hedge NetMktVal',
                  'Alpha GrossMktVal', 'AlphaHedge GrossMktVal', 'Hedge GrossMktVal',
                  'Alpha GrossExp', 'AlphaHedge GrossExp', 'Hedge GrossExp']
    calc_cols = ['Bet Exposure', 'Bet NetMktVal','Bet GrossMktVal', 'Total NetExposure',
                 'Total NetMktVal', 'Total GrossMktVal']

    if len(pcvh_df) == 0: return pd.DataFrame(columns=["Date"]+float_cols+calc_cols)

    alpha_pcvh = pcvh_df[pcvh_df['AlphaHedge'] == 'Alpha']
    alphahedge_pcvh = pcvh_df[pcvh_df['AlphaHedge'] == 'Alpha Hedge']
    hedge_pcvh = pcvh_df[pcvh_df['AlphaHedge'] == 'Hedge']

    alpha_net_exp = alpha_pcvh[['Date', 'Exposure_Net']].groupby(
        'Date').sum().reset_index().rename(columns={'index':'Date', 'Exposure_Net':'Alpha Exposure'})
    alpha_net_mv = alpha_pcvh[['Date', 'NetMktVal']].groupby(
        'Date').sum().reset_index().rename(columns={'index':'Date', 'NetMktVal':'Alpha NetMktVal'})
    alpha_gross_mv = alpha_pcvh[['Date', 'NetMktVal']].groupby(
        'Date').agg(lambda x: sum(abs(x))).reset_index().rename(columns={'index':'Date',
                                                                         'NetMktVal':'Alpha GrossMktVal'})
    alpha_gross_exp = alpha_pcvh[['Date', 'Exposure_Net']].groupby(
        'Date').agg(lambda x: sum(abs(x))).reset_index().rename(columns={'index':'Date',
                                                                         'Exposure_Net':'Alpha GrossExp'})

    alphahedge_net_exp = alphahedge_pcvh[['Date','Exposure_Net']].groupby(
        'Date').sum().reset_index().rename(columns={'index':'Date', 'Exposure_Net':'AlphaHedge Exposure'})
    alphahedge_net_mv = alphahedge_pcvh[['Date','NetMktVal']].groupby(
        'Date').sum().reset_index().rename(columns={'index':'Date', 'NetMktVal':'AlphaHedge NetMktVal'})
    alphahedge_gross_mv = alphahedge_pcvh[['Date','NetMktVal']].groupby(
        'Date').agg(lambda x: sum(abs(x))).reset_index().rename(columns={'index':'Date',
                                                                         'NetMktVal':'AlphaHedge GrossMktVal'})
    alphahedge_gross_exp = alphahedge_pcvh[['Date','Exposure_Net']].groupby(
        'Date').agg(lambda x: sum(abs(x))).reset_index().rename(columns={'index':'Date',
                                                                         'Exposure_Net':'AlphaHedge GrossExp'})

    hedge_net_exp = hedge_pcvh[['Date','Exposure_Net']].groupby(
        'Date').sum().reset_index().rename(columns={'index':'Date','Exposure_Net':'Hedge Exposure'})
    hedge_net_mv = hedge_pcvh[['Date','NetMktVal']].groupby(
        'Date').sum().reset_index().rename(columns={'index':'Date', 'NetMktVal':'Hedge NetMktVal'})
    hedge_gross_mv = hedge_pcvh[['Date','NetMktVal']].groupby(
        'Date').agg(lambda x: sum(abs(x))).reset_index().rename(columns={'index':'Date',
                                                                         'NetMktVal':'Hedge GrossMktVal'})
    hedge_gross_exp = hedge_pcvh[['Date','Exposure_Net']].groupby(
        'Date').agg(lambda x: sum(abs(x))).reset_index().rename(columns={'index':'Date',
                                                                         'Exposure_Net':'Hedge GrossExp'})

    mktval_df = pd.merge(alpha_net_exp, alphahedge_net_exp, how='outer', on=['Date']).fillna(0)
    mktval_df = pd.merge(mktval_df, hedge_net_exp, how='outer', on=['Date']).fillna(0)
    mktval_df = pd.merge(mktval_df, alpha_net_mv, how='outer', on=['Date']).fillna(0)
    mktval_df = pd.merge(mktval_df, alphahedge_net_mv, how='outer', on=['Date']).fillna(0)
    mktval_df = pd.merge(mktval_df, hedge_net_mv, how='outer', on=['Date']).fillna(0)
    mktval_df = pd.merge(mktval_df, alpha_gross_mv, how='outer', on=['Date']).fillna(0)
    mktval_df = pd.merge(mktval_df, alphahedge_gross_mv, how='outer', on=['Date']).fillna(0)
    mktval_df = pd.merge(mktval_df, hedge_gross_mv, how='outer', on=['Date']).fillna(0)
    mktval_df = pd.merge(mktval_df, alpha_gross_exp, how='outer', on=['Date']).fillna(0)
    mktval_df = pd.merge(mktval_df, alphahedge_gross_exp, how='outer', on=['Date']).fillna(0)
    mktval_df = pd.merge(mktval_df, hedge_gross_exp, how='outer', on=['Date']).fillna(0)

    mktval_df[float_cols] = mktval_df[float_cols].astype(float)
    mktval_df['Bet Exposure'] = mktval_df['Alpha Exposure']+mktval_df['AlphaHedge Exposure']
    mktval_df['Bet NetMktVal'] = mktval_df['Alpha NetMktVal']+mktval_df['AlphaHedge NetMktVal']
    mktval_df['Bet GrossMktVal'] = mktval_df['Alpha GrossMktVal']+mktval_df['AlphaHedge GrossMktVal']
    mktval_df['Total NetExposure'] = mktval_df['Alpha Exposure']+mktval_df['AlphaHedge Exposure']+mktval_df['Hedge Exposure']
    mktval_df['Total GrossExposure'] = mktval_df['Alpha GrossExp']+mktval_df['AlphaHedge GrossExp']+mktval_df['Hedge GrossExp']
    mktval_df['Total NetMktVal'] = mktval_df['Alpha NetMktVal']+mktval_df['AlphaHedge NetMktVal']+mktval_df['Hedge NetMktVal']
    mktval_df['Total GrossMktVal'] = mktval_df['Alpha GrossMktVal']+mktval_df['AlphaHedge GrossMktVal']+mktval_df['Hedge GrossMktVal']


    return mktval_df.sort_values(by='Date')


#The following functions are for db_builder
def get_exposure_spreads_nav_df(PCVH):
    start = time.time()
    slicer = dfutils.df_slicer()
    today = datetime.datetime.today()
    yesterday = slicer.prev_n_business_days(1, today).strftime('%Y%m%d')
    
    #First, delete the information from the database if yesterday's data exists
    max_date = dbutils.wic.get_max_date("tradegroup_exposure_nav_info")
    if max_date.strftime('%Y%m%d') == yesterday:
        dbutils.wic.delete_table_by_certain_date(yesterday, 'tradegroup_exposure_nav_info')

    fund_tg_combos = dbutils.wic.get_current_tradegroup_and_fund()
    
    last_date_exposure_db = datetime.datetime.strptime(max_date.strftime('%Y%m%d'), '%Y%m%d')
    FUND_NAV_DF = dbutils.northpoint.get_NAV_df_by_date(start_date_yyyy_mm_dd=last_date_exposure_db)
    SPREADS_HISTORY_DF = dbutils.northpoint.get_spread_history_by_date(start_date_yyyy_mm_dd=last_date_exposure_db)
    PCVH = PCVH[PCVH.Date > last_date_exposure_db]
    PCVH['TradeGroup'] = PCVH['TradeGroup'].apply(lambda x: x.upper())
    SPREADS_HISTORY_DF['TradeGroup'] = SPREADS_HISTORY_DF['TradeGroup'].apply(lambda x: x.upper())
    overall_df = pd.DataFrame()
    
    for i in range(0, len(fund_tg_combos['TradeGroup'])):
        overall_exp_df = pd.DataFrame()
        tradar_tg_name = fund_tg_combos['TradeGroup'][i]
        fund = fund_tg_combos['Fund'][i]

        tg_pcvh = PCVH[(PCVH.TradeGroup == tradar_tg_name) & (PCVH.FundCode == fund)].copy()
        fund_nav_df = FUND_NAV_DF[FUND_NAV_DF.FundCode == fund].copy()
        spreads_history_df = SPREADS_HISTORY_DF[SPREADS_HISTORY_DF.TradeGroup_TradarName == tradar_tg_name].copy()
        
        # Capital AS (%) OF NAV TIME SERIES #
        cap_df = pd.DataFrame()
        capital_df = tg_pcvh[tg_pcvh['AlphaHedge']
                             .isin(['Alpha',
                                    'Alpha Hedge'])][['Date',
                                                      'NetMktVal']].groupby('Date').agg(lambda x: sum(abs(x))).reset_index()
        if len(capital_df) > 0:
            capital_df = pd.merge(capital_df, fund_nav_df, how='inner', on=['Date'])
            capital_df['CapitalAsPctOfNAV'] = 100.0*(capital_df['NetMktVal'].astype(float)
                                                 /capital_df['NAV'].astype(float))
    
            cap_df['Date'] = capital_df['Date']
            cap_df['Capital_Percent_of_NAV'] = capital_df['CapitalAsPctOfNAV']
            cap_df['Fund'] = fund
            cap_df['TradeGroup'] = tradar_tg_name
            
            if overall_exp_df.empty:
                overall_exp_df = overall_exp_df.append(cap_df, sort=True)
            else:
                overall_exp_df = overall_exp_df.merge(cap_df, how='outer', on=['Date', 'Fund', 'TradeGroup'])
        
        # NETEXP (% OF NAV) #
        net_exp_df = pd.DataFrame()
        tot_net_exp_df = tg_pcvh[['Date', 'Exposure_Net']].groupby('Date').sum().reset_index()
        tot_net_exp_df = tot_net_exp_df.rename(columns={'Exposure_Net':'Total Exposure'}).sort_values(by='Date')
        tot_net_exp_df = pd.merge(tot_net_exp_df, fund_nav_df, how='inner', on=['Date'])
        tot_net_exp_df['NetExpAsPctOfNAV'] = 100.0*(tot_net_exp_df['Total Exposure'].astype(float)
                                                    /tot_net_exp_df['NAV'].astype(float))
        if len(tot_net_exp_df) > 0:
            net_exp_df['Date'] = tot_net_exp_df['Date']
            net_exp_df['NetExp_Percent_of_NAV'] = tot_net_exp_df['NetExpAsPctOfNAV']
            net_exp_df['Fund'] = fund
            net_exp_df['TradeGroup'] = tradar_tg_name
            
            if overall_exp_df.empty:
                overall_exp_df = overall_exp_df.append(net_exp_df, sort=True)
            else:
                overall_exp_df = overall_exp_df.merge(net_exp_df, how='outer', on=['Date', 'Fund', 'TradeGroup'])

        # GROSSEXP (% OF NAV) #
        gross_exp_df = pd.DataFrame()
        tot_gross_exp_df = tg_pcvh[['Date', 'Exposure_Net']].groupby('Date').agg(lambda x: sum(abs(x))).reset_index()
        tot_gross_exp_df = tot_gross_exp_df.rename(columns={'Exposure_Net':
                                                            'Total Gross Exposure'}).sort_values(by='Date')
        tot_gross_exp_df = pd.merge(tot_gross_exp_df, fund_nav_df, how='inner', on=['Date'])
        tot_gross_exp_df['GrossExpAsPctOfNAV'] = 100.0*(tot_gross_exp_df['Total Gross Exposure'].astype(float)
                                                        /tot_gross_exp_df['NAV'].astype(float))
        if len(tot_gross_exp_df) > 0:
            gross_exp_df['Date'] = tot_gross_exp_df['Date']
            gross_exp_df['GrossExp_Percent_of_NAV'] = tot_gross_exp_df['GrossExpAsPctOfNAV']
            gross_exp_df['Fund'] = fund
            gross_exp_df['TradeGroup'] = tradar_tg_name
            
            if overall_exp_df.empty:
                overall_exp_df = overall_exp_df.append(gross_exp_df, sort=True)
            else:
                overall_exp_df = overall_exp_df.merge(gross_exp_df, how='outer', on=['Date', 'Fund', 'TradeGroup'])
           
        hedge_exp_df = pd.DataFrame()
        if len(tg_pcvh) > 0:
            alpha_net_exp = tg_pcvh[tg_pcvh['AlphaHedge'] ==
                                    'Alpha'][['Date', 'Exposure_Net']].groupby('Date').sum().reset_index()
            alphahedge_net_exp = tg_pcvh[tg_pcvh['AlphaHedge'] ==
                                         'Alpha Hedge'][['Date', 'Exposure_Net']].groupby('Date').sum().reset_index()
            hedge_net_exp = tg_pcvh[tg_pcvh['AlphaHedge'] ==
                                    'Hedge'][['Date', 'Exposure_Net']].groupby('Date').sum().reset_index()

            if len(alpha_net_exp) > 0:
                alpha_net_exp_df = pd.DataFrame()
                alpha_net_exp = pd.merge(alpha_net_exp, fund_nav_df, how='inner', on=['Date'])
                alpha_net_exp['Exposure_Net'] = 100.0*(alpha_net_exp['Exposure_Net'].astype(float)
                                                       /alpha_net_exp['NAV'].astype(float))
                alpha_net_exp_df['Date'] = alpha_net_exp['Date']
                alpha_net_exp_df['Alpha_Exposure'] = alpha_net_exp['Exposure_Net']
                alpha_net_exp_df['Fund'] = fund
                alpha_net_exp_df['TradeGroup'] = tradar_tg_name
                
                if overall_exp_df.empty:
                    overall_exp_df = overall_exp_df.append(alpha_net_exp_df, sort=True)
                else:
                    overall_exp_df = overall_exp_df.merge(alpha_net_exp_df, how='outer', on=['Date', 'Fund', 'TradeGroup'])
            
            if len(alphahedge_net_exp) > 0:
                alphahedge_net_exp_df = pd.DataFrame()
                alphahedge_net_exp = pd.merge(alphahedge_net_exp, fund_nav_df, how='inner', on=['Date'])
                alphahedge_net_exp['Exposure_Net'] = 100.0*(alphahedge_net_exp['Exposure_Net'].astype(float)
                                                            /alphahedge_net_exp['NAV'].astype(float))
                alphahedge_net_exp_df['Date'] = alphahedge_net_exp['Date']
                alphahedge_net_exp_df['AlphaHedge_Exposure'] = alphahedge_net_exp['Exposure_Net']
                alphahedge_net_exp_df['Fund'] = fund
                alphahedge_net_exp_df['TradeGroup'] = tradar_tg_name
            
                if overall_exp_df.empty:
                    overall_exp_df = overall_exp_df.append(alphahedge_net_exp_df, sort=True)
                else:
                    overall_exp_df = overall_exp_df.merge(alphahedge_net_exp_df, how='outer', on=['Date', 'Fund', 'TradeGroup'])

            if len(hedge_net_exp) > 0:
                hedge_net_exp_df = pd.DataFrame()
                hedge_net_exp = pd.merge(hedge_net_exp, fund_nav_df, how='inner', on=['Date'])
                hedge_net_exp['Exposure_Net'] = 100.0*(hedge_net_exp['Exposure_Net'].astype(float)
                                                       /hedge_net_exp['NAV'].astype(float))
                hedge_net_exp_df['Date'] = hedge_net_exp['Date']
                hedge_net_exp_df['Hedge_Exposure'] = hedge_net_exp['Exposure_Net']
                hedge_net_exp_df['Fund'] = fund
                hedge_net_exp_df['TradeGroup'] = tradar_tg_name
                
                if overall_exp_df.empty:
                    overall_exp_df = overall_exp_df.append(hedge_net_exp_df, sprt=True)
                else:
                    overall_exp_df = overall_exp_df.merge(hedge_net_exp_df, how='outer', on=['Date', 'Fund', 'TradeGroup'])
        
        #SPREAD AS PERCENT#
        spread = pd.DataFrame()
        if tradar_tg_name in spreads_history_df['TradeGroup_TradarName'].unique():
            spreads_ts_df = spreads_history_df[spreads_history_df['TradeGroup_TradarName'] ==
                                               tradar_tg_name][['Date', 'Spread(%)']].sort_values(by='Date')
            spreads_ts_df = spreads_ts_df.rename(columns={'Spread(%)':'SpreadAsPct'}).sort_values(by='Date')
            spreads_ts_df['SpreadAsPct'] = spreads_ts_df['SpreadAsPct'].astype(float)
            
            spread['Date'] = spreads_ts_df['Date']
            spread['Spread_as_Percent'] = spreads_ts_df['SpreadAsPct']
            spread['Fund'] = fund
            spread['TradeGroup'] = tradar_tg_name
            
            if overall_exp_df.empty:
                overall_exp_df = overall_exp_df.append(spread, sort=True)
            else:
                overall_exp_df = overall_exp_df.merge(spread, how='outer', on=['Date', 'Fund', 'TradeGroup'])
                
        overall_df = overall_df.append(overall_exp_df, sort=True)  
    
        #Import the new additions to the database
        con = engine.connect()
        overall_df.to_sql(con=con, schema='wic', name='tradegroup_exposure_nav_info',
                          if_exists='append', chunksize=1000, index=False)
        con.close()
    
    end = time.time()
    print(end - start)
    return overall_df

def get_tradegroup_performance_over_own_capital():
    slicer = dfutils.df_slicer()
    today = datetime.datetime.today().strftime('%Y%m%d')
    
    #First, delete the information from the database if yesterday's data exists
    max_date_vol_df = dbutils.wic.get_max_date("volatility_distribution_timeseries")
    max_date_cap_df = dbutils.wic.get_max-date("tradegroup_performance_over_own_capital")
    if max_date_vol_df.strftime('%Y%m%d') == today:
        dbutils.wic.delete_table_by_certain_date(today, 'volatility_distribution_timeseries')
    if max_date_cap_df.strftime('%Y%m%d') == today:
        dbutils.wic.delete_table_by_certain_date(today, 'volatility_distribution_timeseries')

    try:
        df = dbutils.wic.get_tradegroups_snapshot()

        df['EndDate'] = df['EndDate'].apply(lambda x: x if x is None else pd.to_datetime(x).strftime('%Y-%m-%d'))
        df['InceptionDate'] = df['InceptionDate'].apply(lambda x: x if x is None else pd.to_datetime(x).strftime('%Y-%m-%d'))

        # region formatting configuration
        metrics2include = [('P&L(bps)', 'ITD'), ('P&L(bps)', 'YTD'), ('ROMC(bps)', 'YTD'), ('P&L(bps)', 'MTD'),
                                   ('ROMC(bps)', 'MTD'), ('P&L(bps)', '5D'), ('P&L(bps)', '1D'),
                                   ('P&L($)', 'ITD'), ('P&L($)', 'YTD'), ('P&L($)', 'MTD'), ('P&L($)', '5D'), ('P&L($)', '1D'),
                                   ('ANN. VOL', '30D'), ('CAPITAL($)', '1D')]
        metric2display_name = {'P&L(bps)': '', 'P&L($)': '', 'ANN. VOL': 'VOL',
                                       'ROMC(bps)':'ROMC', 'CAPITAL($)':'CAPITAL'}
        metric2unit = {'P&L(bps)': 'bps', 'P&L($)': '$', 'ANN. VOL': '%',
                               'ROMC(bps)': 'bps', 'CAPITAL($)':'%'} # will transform capital from $ to % later
        # endregion

        fund2vol_chart = []#, fund2vol_breakdown_by_tg = {}, {}
        funds = ["ARB", "AED", "TACO", "WED", "CAM", "LG", "TAQ", "LEV", "MACO", "MALT"]
        overall_df = pd.DataFrame()
        nav_df = get_NAV_df_by_date()
        vol_cohort_chart = {}
        for fund_code in funds:
            f_df = df[df['Fund'] == fund_code].copy()
            f_nav = nav_df[nav_df.FundCode == fund_code]
            f_curr_nav = f_nav['NAV'].iloc[-1]

            metrics_df = pd.DataFrame([dfutils.json2row(j) for j in f_df['Metrics in Bet JSON']])
            metrics_df.index = f_df.index

            for (metric, period) in metrics2include:
                unit = metric2unit[metric]
                disp_name = metric2display_name[metric]
                display_colname = disp_name + ' ' + period + '(' + unit + ')'
                f_df[display_colname] = metrics_df[metric + '|' + period]

            f_df['CAPITAL 1D(%)'] = [np.nan if status == 'CLOSED' else 1e2*(cap_usd/f_curr_nav)
                                        for (cap_usd,status) in zip(f_df['CAPITAL 1D(%)'],f_df['Status'])]

            del f_df['Metrics in NAV JSON']; del f_df['Metrics in NAV notes JSON']
            del f_df['Metrics in Bet JSON']; del f_df['Metrics in Bet notes JSON']
            del f_df['Analyst']

            sleeve2code = {'Merger Arbitrage': 'M&A', 'Equity Special Situations': 'ESS',
                                   'Opportunistic' : 'OPP', 'Forwards':'FWD', 'Credit Opportunities':'CREDIT'}

            f_df['Sleeve'] = f_df['Sleeve'].apply(lambda x: sleeve2code[x] if x in sleeve2code else x)
            f_df = f_df[(~pd.isnull(f_df[' YTD($)']))]  # don't show null ytds
            f_df = f_df.sort_values(by=' YTD($)')
            f_df['Date'] = today.strftime('%Y-%m-%d')

            f_df.rename(columns={' ITD(bps)': 'ITD_bps', ' YTD(bps)': 'YTD_bps', ' MTD(bps)': 'MTD_bps',
                                         ' 5D(bps)': '5D_bps', ' 1D(bps)': '1D_bps', ' ITD($)': 'ITD_Dollar', ' YTD($)': 'YTD_Dollar',
                                         ' MTD($)': 'MTD_Dollar', ' 5D($)': '5D_Dollar', ' 1D($)': '1D_Dollar',
                                         'ROMC YTD(bps)': 'ROMC_YTD_bps', 'ROMC MTD(bps)': 'ROMC_MTD_bps',
                                         'CAPITAL 1D(%)': 'Cap_1D_Pct'}, inplace=True)

            overall_df = overall_df.append(f_df)

                # region VOLATILITY CHARTS
            vol_cohorts = ["0%-5%", "5%-10%", "10%-15%", "15%-20%", "20%-25%", "25%-30%", "30%-35%",
                                   "35%-40%", "40%-45%", "45%-50%", "50%+"]
            vol_cohorts2cnt = {k: 0 for k in vol_cohorts}
            vol_cohorts2tg_vol_pairs = {v: [] for v in vol_cohorts}
            vol_cohort = []
            for (tg, vol) in zip(f_df['TradeGroup'], f_df['VOL 30D(%)']):
                if 0 < vol <= 5:
                    vol_cohorts2cnt["0%-5%"] += 1
                    vol_cohorts2tg_vol_pairs["0%-5%"].append({'bucket': tg, 'value': vol})
                if 5 < vol <= 10:
                    vol_cohorts2cnt["5%-10%"] += 1
                    vol_cohorts2tg_vol_pairs["5%-10%"].append({'bucket': tg, 'value': vol})
                if 10 < vol <= 15:
                    vol_cohorts2cnt["10%-15%"] += 1
                    vol_cohorts2tg_vol_pairs["10%-15%"].append({'bucket': tg, 'value': vol})
                if 15 < vol <= 20:
                    vol_cohorts2cnt["15%-20%"] += 1
                    vol_cohorts2tg_vol_pairs["15%-20%"].append({'bucket': tg, 'value': vol})
                if 20 < vol <= 25:
                    vol_cohorts2cnt["20%-25%"] += 1
                    vol_cohorts2tg_vol_pairs["20%-25%"].append({'bucket': tg, 'value': vol})
                if 25 < vol <= 30:
                    vol_cohorts2cnt["25%-30%"] += 1
                    vol_cohorts2tg_vol_pairs["25%-30%"].append({'bucket': tg, 'value': vol})
                if 30 < vol <= 35:
                    vol_cohorts2cnt["30%-35%"] += 1
                    vol_cohorts2tg_vol_pairs["30%-35%"].append({'bucket': tg, 'value': vol})
                if 35 < vol <= 40:
                    vol_cohorts2cnt["35%-40%"] += 1
                    vol_cohorts2tg_vol_pairs["35%-40%"].append({'bucket': tg, 'value': vol})
                if 40 < vol <= 45:
                    vol_cohorts2cnt["40%-45%"] += 1
                    vol_cohorts2tg_vol_pairs["40%-45%"].append({'bucket': tg, 'value': vol})
                if 45 < vol <= 50:
                    vol_cohorts2cnt["45%-50%"] += 1
                    vol_cohorts2tg_vol_pairs["45%-50%"].append({'bucket': tg, 'value': vol})
                if vol > 50:
                    vol_cohorts2cnt["50%+"] += 1
                    vol_cohorts2tg_vol_pairs["50%+"].append({'bucket': tg, 'value': vol})

            for k in vol_cohorts:
                vol_cohort.append({'bucket': k, 'value': vol_cohorts2cnt[k],
                                        'subdata': vol_cohorts2tg_vol_pairs[k]})

            vol_cohort_chart[fund_code] = vol_cohort
            #endregion
            
        vol_df = pd.DataFrame(columns=['Date','vol_distribution_charts'])
        vol_df.loc[0] = [today.strftime('%Y-%m-%d'), json.dumps(vol_cohort_chart)]
        cols = list(overall_df.columns.values)
        cols.pop(cols.index('Date')) #Want to reorganize so the Date timestamp comes first
        overall_df = overall_df[['Date'] + cols].rename(columns={'VOL 30D(%)': 'VOL_30D_Pct'})
        
        #Import the new additions to the tradegroup_performance_over_own_capital database
        con = engine.connect()
        overall_df.to_sql(con=con, schema='wic', name='tradegroup_performance_over_own_capital',
                          if_exists='append', chunksize=1000, index=False)
        con.close()
        #Import the new additions to the volatility_distribution_timeseries database
        con = engine.connect()
        vol_df.to_sql(con=con, schema='wic', name=' volatility_distribution_timeseries',
                          if_exists='append', chunksize=1000, index=False)
        con.close()

    except Exception as e:
        print e

    return overall_df, vol_df

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
        
    del df['Metrics in NAV JSON']; del df['Metrics in NAV notes JSON'];
    del df['Metrics in Bet JSON']; del df['Metrics in Bet notes JSON'];
    del df['Analyst']

    sleeve2code = {'Merger Arbitrage': 'M&A', 'Equity Special Situations': 'ESS',
                   'Opportunistic' : 'OPP', 'Forwards':'FWD', 'Credit Opportunities':'CREDIT'}

    df['Sleeve'] = df['Sleeve'].apply(lambda x: sleeve2code[x] if x in sleeve2code else x)
    df = df[(~pd.isnull(df[' YTD($)']))]  # don't show null ytds. i.e. tradegroups closed before year started
    df['Date'] = today.strftime('%Y-%m-%d')

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

    return bps_df, dollar_df

def daily_pnl_df(PCVH):
    start = time.time()
    slicer = dfutils.df_slicer()
    today = datetime.datetime.today()
    yesterday = slicer.prev_n_business_days(1, today).strftime('%Y%m%d')
    
    #First, delete the information from the database if yesterday's data exists
    max_date_pnl = dbutils.wic.get_max_date('ticker_pnl_breakdown')
    if max_date_pnl.strftime('%Y%m%d') == yesterday:
        dbutils.wic.delete_table_by_certain_date(yesterday, 'ticker_pnl_breakdown')
        dbutils.wic.delete_table_by_certain_date(yesterday, 'ticker_pnl_breakdown')    
    
    fund_tg_combos = dbutils.wic.get_current_tradegroup_and_fund()
    limit_to_tradegroups = list(fund_tg_combos['TradeGroup'].unique())
    limit_to_funds = list(fund_tg_combos['Fund'].unique())
    
    start_date = datetime.datetime.strptime(max_date_pnl.strftime('%Y%m%d'), '%Y%m%d')
    start_date_yyyy_mm_dd = slicer.prev_n_business_days(30, start_date)
    end_date_yyyy_mm_dd = slicer.prev_n_business_days(1, today)
    
    PCVH = PCVH[PCVH.Date > start_date_yyyy_mm_dd]
    FUND_NAV_DF = dbutils.northpoint.get_NAV_df_by_date(start_date_yyyy_mm_dd=start_date_yyyy_mm_dd)
    TG_PNL_DF = dbutils.wic.get_tradegroups_total_pnl(start_date_yyyy_mm_dd=start_date_yyyy_mm_dd)
    SECURITIES_PNL_DF = dbutils.tradar.get_securities_pnl_by_tradegroup_and_fund_current(limit_to_tradegroups=limit_to_tradegroups,
                                                              limit_to_funds=limit_to_funds,
                                                              is_tg_names_in_tradar_format=True, rollup_pnl=True)
    
    SECURITIES_PNL_DF['TradeGroup'] = SECURITIES_PNL_DF['TradeGroup'].apply(lambda x: x.upper())
    TG_PNL_DF['TradeGroup'] = TG_PNL_DF['TradeGroup'].apply(lambda x: x.upper())
    PCVH['TradeGroup'] = PCVH['TradeGroup'].apply(lambda x: x.upper())
    
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

        if len(options_pnl_df) > 1:
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
                                                                         'Options_PnL_Dollar', 'Options_PnL_bps']], sort=True)
            else:
                bps_dollar_pnl_df = pd.merge(bps_dollar_pnl_df, options_pnl_df[['Date', 'Fund', 'TradeGroup',
                                                                         'Options_PnL_Dollar', 'Options_PnL_bps']],
                                             how='outer', on=['Date', 'Fund', 'TradeGroup'])

        #P&L Breakdown by Ticker -- to be kept in a separate db
        tg_securities = securities_pnl_df[securities_pnl_df['SecType'] != 'Option']['Ticker'].unique()
        for ticker in tg_securities:
            tkr_pnl_df = securities_pnl_df[securities_pnl_df['Ticker'] == ticker].copy()
            tkr_pnl_df = tkr_pnl_df.loc[np.trim_zeros(tkr_pnl_df['Total P&L']).index].copy()
            if len(tkr_pnl_df) <= 1:
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

        final_ticker_pnl_df = final_ticker_pnl_df.append(bps_dollar_ticker_pnl_df, sort=True)

        #Daily PnL  
        daily_pnl_df = tg_pnl_df[['Date', 'Fund', 'TradeGroup', 'Total P&L']]
        if len(daily_pnl_df) > 1:
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
                                                                         'Daily_PnL_Dollar', 'Daily_PnL_bps']], sort=True)
            else:
                bps_dollar_pnl_df = pd.merge(bps_dollar_pnl_df, daily_pnl_df[['Date', 'Fund', 'TradeGroup',
                                                                         'Daily_PnL_Dollar', 'Daily_PnL_bps']], how='outer',
                                             on=['Date', 'Fund', 'TradeGroup'])

        cum_pnl = pd.DataFrame()
        rolling_vol_df = pd.DataFrame()
        if len(tg_pcvh) > 1:
                tg_capital_df = dfutils.mktval_df(tg_pcvh)
                roc_df = pd.merge(tg_pnl_df, tg_capital_df[['Date', 'Bet GrossMktVal']], how='inner', on=['Date'])
                roc_df['Shifted Capital'] = roc_df['Bet GrossMktVal'].shift(1)
                if pd.isnull(roc_df['Shifted Capital'].iloc[0]):
                    roc_df.loc[0, 'Shifted Capital'] = roc_df['Bet GrossMktVal'].iloc[0]
                roc_df['Shifted Capital'] = roc_df['Shifted Capital'].apply(lambda x: np.nan if x == 0 else x)
                roc_df['Shifted Forward-Filled Capital'] = roc_df['Shifted Capital'].ffill()
                roc_df['PnL_Over_Cap_bps'] = 1e4*(roc_df["Total P&L"]/
                                           roc_df["Shifted Forward-Filled Capital"]).replace([np.inf, -np.inf],
                                                                                             np.nan) 

                #roc_df['Cumulative P&L (%)'] = roc_df['Cumulative P&L bps (ffiled)'].apply(lambda x: x/100.0)
                roc_df['Rolling_30D_PnL_Vol'] = math.sqrt(252)*(roc_df['PnL_Over_Cap_bps']/100.0).rolling(window=30).std()
                if len(roc_df['Rolling_30D_PnL_Vol'].dropna()) > 0:
                    rolling_vol_df[['Date', 'Fund', 'TradeGroup', 'Rolling_30D_PnL_Vol']] = roc_df[
                        ["Date", "Fund", "TradeGroup", "Rolling_30D_PnL_Vol"]].dropna()

                    if bps_dollar_pnl_df.empty:
                        bps_dollar_pnl_df = bps_dollar_pnl_df.append(rolling_vol_df[['Date', 'Fund', 'TradeGroup',
                                                                                     'Rolling_30D_PnL_Vol']], sort=True)
                    else:
                        bps_dollar_pnl_df = bps_dollar_pnl_df.merge(rolling_vol_df[['Date', 'Fund',
                                                                                    'TradeGroup',
                                                                                    'Rolling_30D_PnL_Vol']],
                                                                    how='outer', on=['Date', 'Fund', 'TradeGroup'])

                if bps_dollar_pnl_df.empty:
                    bps_dollar_pnl_df = bps_dollar_pnl_df.append(roc_df[['Date', 'Fund', 'TradeGroup',
                                                                     'PnL_Over_Cap_bps']], sort=True)
                else:
                    bps_dollar_pnl_df = bps_dollar_pnl_df.merge(roc_df[['Date', 'Fund', 'TradeGroup', 'PnL_Over_Cap_bps']],
                                                                how='outer', on=['Date', 'Fund', 'TradeGroup'])
        
        final_pnl_df = final_pnl_df.append(bps_dollar_pnl_df, sort=True)
        
        overall_pnl_additions = final_pnl_df[final_pnl_df.Date > start_date]
        ticker_pnl_additons = final_ticker_pnl_df[final_ticker_pnl_df.Date > start_date]
        
        con = engine.connect()
        ticker_pnl_additions.to_sql(con=con, schema='wic', name='ticker_pnl_breakdown',
                             if_exists='append', chunksize=1000, index=False)
        con.close()
        
        con = engine.connect()
        overall_pnl_additions.to_sql(con=con, schema='wic', name='tradegroup_overall_pnl',
                             if_exists='append', chunksize=1000, index=False)
        con.close()
        
    end = time.time()
    print(end-start)
    return overall_pnl, ticker_pnl      
