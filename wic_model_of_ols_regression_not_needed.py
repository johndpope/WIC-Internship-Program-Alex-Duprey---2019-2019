def calibration_data(alpha_ticker, OLS_results, metrics):
    peer_tickers = OLS_results['overlap_peers']
    len_peer = len(peer_tickers)
    weight = [(1/len_peer)]*len_peer
    peer2weight = dict(zip(peer_tickers, weight))
    
    alpha_historical_mult = OLS_results['alpha_historical_mult_df']
    peer2historical_mult = OLS_results['peer2historical_mult_df']

    metric2rel = {}
    metrics2 = []
    for metric in metrics:
        if len(alpha_historical_mult[~pd.isnull(alpha_historical_mult[metric])])>0:
            alpha_over_peers_df = pd.DataFrame()
            alpha_over_peers_df['Date'] = alpha_historical_mult['Date']
            alpha_over_peer_df_list = []
            tot_adj_weight = 0
            peers_included = []
            for (peer,weight) in peer2weight.items():
                peer_mult = peer2historical_mult[peer]
                alpha_over_peer_df = compare_multiples(alpha_historical_mult,peer_mult,metric)
                if len(alpha_over_peer_df) > 0:
                    tot_adj_weight += weight
                    peers_included.append(peer)
                    alpha_over_peers_df = pd.merge(alpha_over_peers_df,alpha_over_peer_df,how='left',on='Date').rename(columns={'Multiple Ratio':'vs. '+peer})
                    alpha_over_peer_df_list.append(alpha_over_peer_df[['Date','Multiple Ratio']].rename(columns={'Multiple Ratio': 'vs. ' + peer}))

            peer2adj_weight = {p:(peer2weight[p]/tot_adj_weight) for p in peers_included}
            for p in peer2adj_weight:
                alpha_over_peers_df['vs. '+p+'(weighted)'] = peer2adj_weight[p]*alpha_over_peers_df['vs. '+p]

            alpha_over_peers_df['vs. all peers'] = alpha_over_peers_df[['vs. '+p+'(weighted)' for p in peers_included]].sum(axis=1)

            mu=alpha_over_peers_df['vs. all peers'].mean()
            sigma=alpha_over_peers_df['vs. all peers'].std()

            metrics2.append(metric)
            
            metric2rel[metric] = {
                'Mu':mu,
                'Sigma':sigma,
                'Alpha vs. all peers, dataframe': alpha_over_peers_df,
                'Alpha vs. each peer, list':alpha_over_peer_df_list,
                'Peers adjusted weight': peer2adj_weight
            }
        else:
            continue

    return {
        'metrics': metrics2,
        'peer_tickers':peer_tickers,
        'metric2rel':metric2rel,
        'alpha_historical_mult_df': alpha_historical_mult,
        'peer2historical_mult_df': peer2historical_mult
    }

def metric2implied_px(alpha_ticker,peer_tickers, dt, metrics, api_host, metric2stat_rel, fperiod='1BF'):
    slicer = dfutils.df_slicer()

    start_date_yyyymmdd = slicer.prev_n_business_days(100,dt).strftime('%Y%m%d')
    peer2mult_df = {p:multiples_df(p,start_date_yyyymmdd, dt.strftime('%Y%m%d'), api_host, fperiod, multiples_to_query=metrics) for p in peer_tickers}
    alpha_mult_underlying_df = multiple_underlying_df(alpha_ticker,start_date_yyyymmdd,dt.strftime('%Y%m%d'),api_host,fperiod)

    metric2data={m:{} for m in metrics}
    for metric in metrics:
        #alpha_mult = alpha_mult_df[metric].iloc[-1]
        #alpha_px = alpha_mult_df['PX'].iloc[-1]
        alpha_balance_sheet_df = alpha_mult_underlying_df[alpha_mult_underlying_df['Date']==dt.strftime('%Y-%m-%d')]
        peer2mult = {p:peer2mult_df[p][metric].iloc[-1] for p in peer_tickers}

        stat_rel = metric2stat_rel[metric]
        mu = stat_rel['Mu']
        sigma = stat_rel['Sigma']
        peer2adj_weight = stat_rel['Peers adjusted weight']
        #peers_multiple = sum([peer2adj_weight[p]*peer2mult[p] for p in peer_tickers])
        peers_multiple = sum([(peer2adj_weight[p]*peer2mult[p] if p in peer2adj_weight else 0.0)  for p in peer_tickers])

        implied_multiple_high = (mu+2*sigma)*peers_multiple
        implied_multiple_mean = mu*peers_multiple
        implied_multiple_low = (mu-2*sigma)*peers_multiple

        if metric in ['FCF Yield','DVD Yield']: # flip
            tmp = implied_multiple_high
            implied_multiple_high = implied_multiple_low
            implied_multiple_low = tmp

        metric2data[metric]['Alpha implied multiple (high)'] = implied_multiple_high
        metric2data[metric]['Alpha implied multiple (mean)'] = implied_multiple_mean
        metric2data[metric]['Alpha implied multiple (low)'] = implied_multiple_low
        metric2data[metric]['Alpha Balance Sheet DataFrame'] = alpha_balance_sheet_df
        #metric2data[metric]['Alpha observed multiple'] = alpha_mult ### takes extra api call, use only if needed
        metric2data[metric]['Peers multiple'] = peers_multiple
        metric2data[metric]['Peer2Multiple'] = peer2mult
        metric2data[metric]['Alpha Unaffected PX (-2sigma)'] = compute_implied_price_from_multiple(metric,implied_multiple_low,alpha_balance_sheet_df)
        metric2data[metric]['Alpha Unaffected PX (avg)'] = compute_implied_price_from_multiple(metric,implied_multiple_mean,alpha_balance_sheet_df)
        metric2data[metric]['Alpha Unaffected PX (+2sigma)'] = compute_implied_price_from_multiple(metric,implied_multiple_high,alpha_balance_sheet_df)
        #metric2data[metric]['Alpha PX'] = alpha_px

    return metric2data

def premium_analysis_df(alpha_ticker, tgt_dt, analyst_upside, calib_results, api_host):
    peers = calib_results['peer_tickers']
    last_price_target_dt = datetime.datetime.strptime(tgt_dt, '%Y-%m-%d')
    as_of_dt = datetime.datetime.today()
    metrics = calib_results['metrics']
    metric2stat_rel = calib_results['metric2rel']
    
    metric2implied_now = metric2implied_px(alpha_ticker,peers,as_of_dt,metrics,api_host,metric2stat_rel,fperiod='1BF')
    metric2implied_at_price_tgt_date = metric2implied_px(alpha_ticker,peers,last_price_target_dt,metrics,api_host,metric2stat_rel,fperiod='1BF')

    df = pd.DataFrame()
    df['Metric'] = metrics
    df['Alpha to Peer historical ratio (mean)'] = df['Metric'].apply(lambda m: metric2stat_rel[m]['Mu'])
    df['Alpha to Peer historical ratio (std)'] = df['Metric'].apply(lambda m: metric2stat_rel[m]['Sigma'])

    df['Peers Composite Multiple @ Price Target Date'] = df['Metric'].apply(lambda m: metric2implied_at_price_tgt_date[m]['Peers multiple'])
    df['Peer2Multiple @ Price Target Date'] = df['Metric'].apply(lambda m: metric2implied_at_price_tgt_date[m]['Peer2Multiple'])
    df['Alpha Implied Multiple (mean) @ Price Target Date'] = df['Metric'].apply(lambda m: metric2implied_at_price_tgt_date[m]['Alpha implied multiple (mean)'])
    df['Alpha Balance Sheet @ Price Target Date'] = df['Metric'].apply(lambda m: metric2implied_at_price_tgt_date[m]['Alpha Balance Sheet DataFrame'])
    df['Alpha Unaffected PX @ Price Target Date'] = df['Metric'].apply(lambda m: metric2implied_at_price_tgt_date[m]['Alpha Unaffected PX (avg)'])
    df['Premium'] = None
    df['Alpha Upside (analyst)'] = analyst_upside
    df['Premium'] = df['Alpha Upside (analyst)'] - df['Alpha Unaffected PX @ Price Target Date']


    df['Peers Composite Multiple @ Now'] = df['Metric'].apply(lambda m: metric2implied_now[m]['Peers multiple'])
    df['Peer2Multiple @ Now'] = df['Metric'].apply(lambda m: metric2implied_now[m]['Peer2Multiple'])
    df['Alpha Implied Multiple (mean) @ Now'] = df['Metric'].apply(lambda m: metric2implied_now[m]['Alpha implied multiple (mean)'])
    df['Alpha Balance Sheet @ Now'] = df['Metric'].apply(lambda m: metric2implied_now[m]['Alpha Balance Sheet DataFrame'])
    df['Alpha Unaffected PX (low) @ Now'] = df['Metric'].apply(lambda m: metric2implied_now[m]['Alpha Unaffected PX (-2sigma)'])
    df['Alpha Unaffected PX (mean) @ Now'] = df['Metric'].apply(lambda m: metric2implied_now[m]['Alpha Unaffected PX (avg)'])
    df['Alpha Unaffected PX (high) @ Now'] = df['Metric'].apply(lambda m: metric2implied_now[m]['Alpha Unaffected PX (+2sigma)'])


    df['Alpha Downside (Adj)'] = df['Alpha Unaffected PX (low) @ Now']
    df['Alpha Upside (Adj)'] = df['Alpha Unaffected PX (mean) @ Now'] + df['Premium']
    #df['Alpha Downside (Adj,weighted)'] = df['Alpha Downside (Adj)'].astype(float)
    #df['Alpha Upside (Adj,weighted)'] = df['Alpha Upside (Adj)'].astype(float)

    return df