__author__ = 'aduprey'
from rpy2.robjects.packages import importr
from rpy2.robjects import pandas2ri
pandas2ri.activate()
base = importr('base')
stats = importr("stats")
utils = importr('utils')

import bbgclient
import dfutils
import datetime
import pandas as pd
import json
import numpy as np
import statsmodels.formula.api as sm



def multiple_underlying_df(ticker, end_date_yyyymmdd, api_host, fperiod="1BF"):
    '''
    Retrieves the balance sheet information from Bloomberg
    '''
    
    slicer = dfutils.df_slicer()

    if type(end_date_yyyymmdd) == str:
        end_date_yyyymmdd = datetime.datetime.strptime(end_date_yyyymmdd, '%Y%m%d')

    def last_elem_or_null(ts):
        if ts is None: return None
        if len(ts) == 0: return None
        return ts.iloc[-1]

    px = last_elem_or_null(bbgclient.bbgclient.get_timeseries(ticker, 'PX_LAST',
                                                              slicer.prev_n_business_days(100, end_date_yyyymmdd).
                                                              strftime('%Y%m%d'), end_date_yyyymmdd.strftime('%Y%m%d'),
                                                              api_host=api_host))

    mkt_cap = last_elem_or_null(bbgclient.bbgclient.get_timeseries(ticker, 'CUR_MKT_CAP',
                                                                   slicer.prev_n_business_days(100, end_date_yyyymmdd).
                                                                   strftime('%Y%m%d'), end_date_yyyymmdd.
                                                                   strftime('%Y%m%d'), api_host=api_host))

    ev_component = last_elem_or_null(bbgclient.bbgclient.get_timeseries(ticker, 'CUR_EV_COMPONENT',
                                                                        slicer.prev_n_business_days(100,
                                                                                                    end_date_yyyymmdd)
                                                                        .strftime('%Y%m%d'), end_date_yyyymmdd.
                                                                        strftime('%Y%m%d'), api_host=api_host))

    eqy_sh_out = last_elem_or_null(bbgclient.bbgclient.get_timeseries(ticker, 'EQY_SH_OUT',
                                                                      slicer.prev_n_business_days(100,
                                                                                                  end_date_yyyymmdd).
                                                                      strftime('%Y%m%d'),
                                                                      end_date_yyyymmdd.strftime('%Y%m%d'),
                                                                      api_host=api_host))
    best_ebitda = last_elem_or_null(bbgclient.bbgclient.get_timeseries(ticker, 'BEST_EBITDA',
                                                                       slicer.prev_n_business_days(100,
                                                                                                   end_date_yyyymmdd)
                                                                       .strftime('%Y%m%d'),
                                                                       end_date_yyyymmdd.strftime('%Y%m%d'),
                                                                       {'BEST_FPERIOD_OVERRIDE': fperiod}, api_host))
    best_sales = last_elem_or_null(bbgclient.bbgclient.get_timeseries(ticker, 'BEST_SALES',
                                                                      slicer.prev_n_business_days(100,
                                                                                                  end_date_yyyymmdd).
                                                                      strftime('%Y%m%d'),
                                                                      end_date_yyyymmdd.strftime('%Y%m%d'),
                                                                      {'BEST_FPERIOD_OVERRIDE': fperiod}, api_host))

    best_eps = last_elem_or_null(bbgclient.bbgclient.get_timeseries(ticker, 'BEST_EPS',
                                                                    slicer.prev_n_business_days(100, end_date_yyyymmdd)
                                                                    .strftime('%Y%m%d'), end_date_yyyymmdd.
                                                                    strftime('%Y%m%d'), {'BEST_FPERIOD_OVERRIDE':
                                                                                         fperiod}, api_host))

    div_ind_yield = last_elem_or_null(bbgclient.bbgclient.get_timeseries(ticker, 'DIVIDEND_INDICATED_YIELD',
                                                                         slicer.prev_n_business_days(100,
                                                                                                     end_date_yyyymmdd)
                                                                         .strftime('%Y%m%d'),
                                                                         end_date_yyyymmdd.strftime('%Y%m%d'),
                                                                         api_host=api_host))

    best_opp = last_elem_or_null(bbgclient.bbgclient.get_timeseries(ticker, 'BEST_OPP',
                                                                    slicer.prev_n_business_days(100, end_date_yyyymmdd)
                                                                    .strftime('%Y%m%d'), end_date_yyyymmdd.
                                                                    strftime('%Y%m%d'), api_host=api_host))

    best_ni = last_elem_or_null(bbgclient.bbgclient.get_timeseries(ticker, 'BEST_NET_INCOME',
                                                                   slicer.prev_n_business_days(100,
                                                                                               end_date_yyyymmdd)
                                                                   .strftime('%Y%m%d'), end_date_yyyymmdd.
                                                                   strftime('%Y%m%d'), {'BEST_FPERIOD_OVERRIDE':
                                                                                        fperiod},
                                                                   api_host=api_host))
    best_capex = last_elem_or_null(bbgclient.bbgclient.get_timeseries(ticker, 'BEST_CAPEX',
                                                                      slicer.prev_n_business_days(100,
                                                                                                  end_date_yyyymmdd).
                                                                      strftime('%Y%m%d'),
                                                                      end_date_yyyymmdd.strftime('%Y%m%d'),
                                                                      api_host=api_host))

    cols = ['Date', 'PX', 'CUR_MKT_CAP', 'EQY_SH_OUT', 'BEST_EBITDA', 'BEST_SALES',
            'BEST_EPS', 'DIVIDEND_INDICATED_YIELD', 'BEST_OPP', 'BEST_NET_INCOME', 'BEST_CAPEX', 'CUR_EV_COMPONENT']
    datum = [(pd.to_datetime(end_date_yyyymmdd), px, mkt_cap, eqy_sh_out, best_ebitda, best_sales, best_eps,
              div_ind_yield, best_opp, best_ni, best_capex, ev_component)]
    df = pd.DataFrame(columns=cols, data=datum)

    return df


def multiples_df(ticker, start_date_yyyymmdd, unaffected_date_yyyymmdd, api_host, fperiod, multiples_to_query='ALL'):
    '''
    Function to get the multiples values for a particular stock from Bloomberg
    '''
    
    if multiples_to_query == 'ALL':
        multiples_to_query = ['EV/EBITDA','EV/Sales','P/EPS','DVD yield','FCF yield']
    pe = pd.Series()
    ev_to_ebitda = pd.Series()
    ev_to_sales = pd.Series()
    dvd_yield = pd.Series()
    px = bbgclient.bbgclient.get_timeseries(ticker,'PX_LAST', start_date_yyyymmdd, unaffected_date_yyyymmdd, 
                                            api_host=api_host)
    if 'EV/EBITDA' in multiples_to_query:
        ev_to_ebitda = bbgclient.bbgclient.get_timeseries(ticker, 'BEST_CUR_EV_TO_EBITDA', start_date_yyyymmdd,
                                                          unaffected_date_yyyymmdd, {'BEST_FPERIOD_OVERRIDE': fperiod},
                                                          api_host)
    if 'EV/Sales' in multiples_to_query:
        ev_to_sales = bbgclient.bbgclient.get_timeseries(ticker, 'BEST_CURRENT_EV_BEST_SALES', start_date_yyyymmdd,
                                                         unaffected_date_yyyymmdd, {'BEST_FPERIOD_OVERRIDE': fperiod},
                                                         api_host)
    if 'P/EPS' in multiples_to_query:
        pe = bbgclient.bbgclient.get_timeseries(ticker, 'BEST_PE_RATIO', start_date_yyyymmdd,
                                                unaffected_date_yyyymmdd, {'BEST_FPERIOD_OVERRIDE': fperiod}, api_host)
    if 'DVD yield' in multiples_to_query:
        dvd_yield = bbgclient.bbgclient.get_timeseries(ticker, 'DIVIDEND_INDICATED_YIELD', start_date_yyyymmdd,
                                                       unaffected_date_yyyymmdd, api_host=api_host)

    df = px.reset_index().rename(columns={'index':'Date', 0:'PX'})
    if 'FCF yield' in multiples_to_query:
        #### FCF-related mnemonics
        ebitda = bbgclient.bbgclient.get_timeseries(ticker, 'BEST_EBITDA', start_date_yyyymmdd, unaffected_date_yyyymmdd,
                                                    {'BEST_FPERIOD_OVERRIDE': fperiod},api_host).reset_index().rename(
            columns={'index':'Date', 0:'EBITDA'})
        opp = bbgclient.bbgclient.get_timeseries(ticker, 'BEST_OPP', start_date_yyyymmdd, unaffected_date_yyyymmdd,
                                                 {'BEST_FPERIOD_OVERRIDE': fperiod},api_host).reset_index().rename(
            columns={'index':'Date', 0:'OPP'})
        capex = bbgclient.bbgclient.get_timeseries(ticker, 'BEST_CAPEX', start_date_yyyymmdd, unaffected_date_yyyymmdd,
                                                   {'BEST_FPERIOD_OVERRIDE': fperiod},api_host).reset_index().rename(
            columns={'index':'Date', 0:'CAPEX'})
        eqy_sh_out = bbgclient.bbgclient.get_timeseries(ticker, 'EQY_SH_OUT', start_date_yyyymmdd, unaffected_date_yyyymmdd,
                                                        api_host=api_host).reset_index().rename(
            columns={'index':'Date',0:'EQY_SH_OUT'})
        ni = bbgclient.bbgclient.get_timeseries(ticker, 'BEST_NET_INCOME', start_date_yyyymmdd, unaffected_date_yyyymmdd,
                                                {'BEST_FPERIOD_OVERRIDE': fperiod},api_host).reset_index().rename(
            columns={'index':'Date', 0:'NI'})
        ####
        fcf = pd.merge(px.reset_index().rename(columns={'index':'Date',0:'PX'}),ebitda, how='left',
                       on=['Date']).ffill().bfill()
        fcf = pd.merge(fcf, opp, how='left', on=['Date']).ffill().bfill()
        fcf = pd.merge(fcf, capex, how='left', on=['Date']).ffill().bfill()
        fcf = pd.merge(fcf, eqy_sh_out, how='left', on=['Date']).ffill().bfill()
        fcf = pd.merge(fcf, ni, how='left', on=['Date']).ffill().bfill()
        fcf['FCF'] = (fcf['NI'] + fcf['EBITDA'] - fcf['OPP'] + fcf['CAPEX'])/fcf['EQY_SH_OUT']
        fcf['FCF yield'] = fcf['FCF']/fcf['PX']
        df = pd.merge(df, fcf[['Date','FCF yield']], how='left', on='Date').ffill().bfill()

    df = pd.merge(df,pe.reset_index().rename(columns={'index':'Date', 0:'P/EPS'}), how='left', on=['Date']).ffill().bfill()
    df = pd.merge(df, ev_to_ebitda.reset_index().rename(columns={'index':'Date', 0:'EV/EBITDA'}), how='left',
                  on='Date').ffill().bfill()
    df = pd.merge(df, ev_to_sales.reset_index().rename(columns={'index':'Date', 0:'EV/Sales'}), how='left',
                  on='Date').ffill().bfill()
    df = pd.merge(df, dvd_yield.reset_index().rename(columns={'index':'Date', 0:'DVD yield'}), how='left',
                  on='Date').ffill().bfill()

    # Date     PX     P/EPS  EV/EBITDA  EV/Sales  DVD yield  FCF yield
    return df


def compute_implied_price_from_multiple(metric_name, multiple, mult_underlying_df):
    '''
    Caclulates the unaffected price from alpha multiple and balance sheet information
    '''
    
    try:
        if metric_name == 'EV/EBITDA':
            ebitda = float(mult_underlying_df['BEST_EBITDA'].iloc[0])
            eqy_sh_out = float(mult_underlying_df['EQY_SH_OUT'].iloc[0])
            ev_component = float(mult_underlying_df['CUR_EV_COMPONENT'].iloc[0])
            return ((multiple*ebitda)-ev_component)/eqy_sh_out

        if metric_name == 'EV/Sales':
            sales = float(mult_underlying_df['BEST_SALES'].iloc[0])
            eqy_sh_out = float(mult_underlying_df['EQY_SH_OUT'].iloc[0])
            ev_component = float(mult_underlying_df['CUR_EV_COMPONENT'].iloc[0])
            return ((multiple*sales)-ev_component)/eqy_sh_out

        if metric_name == 'P/EPS':
            eps = float(mult_underlying_df['BEST_EPS'].iloc[0])
            return eps*multiple

        if metric_name == 'DVD yield':
            curr_dvd_yield = float(mult_underlying_df['DIVIDEND_INDICATED_YIELD'].iloc[0])
            curr_px = float(mult_underlying_df['PX'].iloc[0])
            curr_dvd = curr_dvd_yield*curr_px
            implied_px = curr_dvd/multiple
            return implied_px

        if metric_name == 'FCF yield':
            ni = float(mult_underlying_df['BEST_NET_INCOME'].iloc[0])
            ebitda = float(mult_underlying_df['BEST_EBITDA'].iloc[0])
            opp = float(mult_underlying_df['BEST_OPP'].iloc[0])
            capex = float(mult_underlying_df['BEST_CAPEX'].iloc[0])
            eqy_sh_out = float(mult_underlying_df['EQY_SH_OUT'].iloc[0])
            fcf = (ni + ebitda - opp + capex)/eqy_sh_out
            implied_px = fcf/multiple
            return implied_px

    except Exception as e:
        print('failed calculating implied price from multiple: ' + str(metric_name) + ' ' + str(e.args))
        return None


def compute_multiple_from_price(metric_name, price, mult_underlying_df):
    '''
    Calculates the alpha multiple from price using the unaffected price and balance sheet information.
    This is the reverse calculation of the function above.
    '''
    try:
        if metric_name == 'EV/EBITDA':
            ebitda = float(mult_underlying_df['BEST_EBITDA'].iloc[0])
            eqy_sh_out = float(mult_underlying_df['EQY_SH_OUT'].iloc[0])
            ev_component = float(mult_underlying_df['CUR_EV_COMPONENT'].iloc[0])
            return ((price * eqy_sh_out) + ev_component) / ebitda

        if metric_name == 'EV/Sales':
            sales = float(mult_underlying_df['BEST_SALES'].iloc[0])
            eqy_sh_out = float(mult_underlying_df['EQY_SH_OUT'].iloc[0])
            ev_component = float(mult_underlying_df['CUR_EV_COMPONENT'].iloc[0])
            return ((price * eqy_sh_out) + ev_component) / sales

        if metric_name == 'P/EPS':
            eps = float(mult_underlying_df['BEST_EPS'].iloc[0])
            return price / eps

        if metric_name == 'DVD yield':
            curr_dvd_yield = float(mult_underlying_df['DIVIDEND_INDICATED_YIELD'].iloc[0])
            curr_px = float(mult_underlying_df['PX'].iloc[0])
            curr_dvd = curr_dvd_yield * curr_px
            multiple = curr_dvd / price
            return multiple

        if metric_name == 'FCF yield':
            ni = float(mult_underlying_df['BEST_NET_INCOME'].iloc[0])
            ebitda = float(mult_underlying_df['BEST_EBITDA'].iloc[0])
            opp = float(mult_underlying_df['BEST_OPP'].iloc[0])
            capex = float(mult_underlying_df['BEST_CAPEX'].iloc[0])
            eqy_sh_out = float(mult_underlying_df['EQY_SH_OUT'].iloc[0])
            fcf = (ni + ebitda - opp + capex) / eqy_sh_out
            multiple = fcf / price
            return multiple

    except Exception as e:
        print('failed calculating implied multiple from price: ' + str(metric_name) + ' ' + str(e.args))
        return None

    
def bloomberg_peers(alpha):
    '''
    Grabs the potential peers from the Bloomberg Peer Correlation page
    '''
        
    potential_peers = []
    peer = bbgclient.bbgclient.get_secid2field([alpha], "tickers", ['BLOOMBERG_PEERS'], req_type="refdata")
    for i in range(0, len(peer[alpha]["BLOOMBERG_PEERS"])):
        potential_peers.append(peer[alpha]["BLOOMBERG_PEERS"][i]["Peer Ticker"] + " EQUITY")
    
    return(potential_peers)


def run_regression_optimal_peers(alpha_ticker, unaffect_dt, lookback_period=120, fperiod="1BF"):
    '''
    Function to run the regression for optimal peers. This function is to run each time a deal is made
    and the values to be stored. If the unaffected date changes then the regression coefficient will need
    to be updated if they were previously saved.
    '''
    
    metrics = ['P/EPS', "EV/EBITDA", "EV/Sales", 'DVD yield', 'FCF yield']
    api_host = bbgclient.bbgclient.get_next_available_host()
    slicer = dfutils.df_slicer()
    unaff_dt = datetime.datetime.strptime(unaffect_dt, '%Y-%m-%d')
    
    peer_ticker_list = bloomberg_peers(alpha_ticker)
    
    alpha_historical_mult_df = multiples_df(alpha_ticker, slicer.prev_n_business_days(lookback_period,unaff_dt).strftime('%Y%m%d'), unaff_dt.strftime('%Y%m%d'), api_host, fperiod) # ['CID','Date','Ticker','PX','P/EPS','EV/EBITDA','EV/Sales','DVD yield','FCF yield']
    peer2historical_mult_df = {p:multiples_df(p,slicer.prev_n_business_days(lookback_period,unaff_dt).strftime('%Y%m%d'), unaff_dt.strftime('%Y%m%d'), api_host, fperiod, multiples_to_query=metrics) for p in peer_ticker_list}
    ticker2short_ticker = {p:p.split(' ')[0] for p in peer_ticker_list+[alpha_ticker]}
    
    #peer2ptd_multiple = {p:multiples_df(p,slicer.prev_n_business_days(100,price_tgt_dt).strftime('%Y%m%d'),price_tgt_dt.strftime('%Y%m%d'),api_host,fperiod='1BF',multiples_to_query=metrics) for p in peer_ticker_list}
    #peer2now_multiple = {p:multiples_df(p,slicer.prev_n_business_days(100,as_of_dt).strftime('%Y%m%d'),as_of_dt.strftime('%Y%m%d'),api_host,fperiod='1BF',multiples_to_query=metrics) for p in peer_ticker_list}
    
    #Runs the regression on all metrics and find the optimal peer group per metric which is then uses to calculate the downside
    rows = {}
    metric2peer2coeff = {m:{} for m in metrics}
    peers_d = {}
    for metric in metrics:
        if len(alpha_historical_mult_df[~pd.isnull(alpha_historical_mult_df[metric])])>0:
            m_df = alpha_historical_mult_df[['Date',metric]].rename(columns={metric:ticker2short_ticker[alpha_ticker]})
            for p in peer2historical_mult_df:
                m_df = pd.merge(m_df,peer2historical_mult_df[p][['Date',metric]],how='left',on='Date').rename(columns={metric:ticker2short_ticker[p]})
            peer_tickers = [p for p in peer_ticker_list if len(m_df[~pd.isnull(m_df[p.split(' ')[0]])])>0] # remove peers with all nulls
            #peer_tickers = [p for p in peer_tickers if ~peer2ptd_multiple[p][metric].isnull().all() and ~peer2now_multiple[p][metric].isnull().all()]
            m_ols_df = m_df[[alpha_ticker.split(' ')[0]]+[t.split(' ')[0] for t in peer_tickers]]
            formula = alpha_ticker.split(' ')[0] + '~.' #+ " + ".join([t.split(' ')[0] for t in peer_ticker_list])
            r_df = pandas2ri.py2ri(m_ols_df)
            r_df = stats.na_omit(r_df)
            model = stats.lm(formula, data = r_df)
            optimal_model = stats.step(model, direction = "backward", trace = False)
            ### MODEL RESULTS SUCH AS FIT, PARAMETERS, ETC.
            summary_results = base.summary(optimal_model)
            summary_results2 = pandas2ri.ri2py(summary_results)
            ###
            optimal_peers = base.attr(optimal_model.rx2('terms'), "term.labels")
            optimal_peers2 = []
            for i in optimal_peers:
                for k in peer_tickers:
                    if k.startswith(i + ' '):
                        optimal_peers2.append(k)
                    elif i.startswith('X') and k.startswith(i[1:] + ' '):
                        optimal_peers2.append(k)
                    elif '.' in i:
                        i = i.replace('.', '/')
                        if k.startswith(i + ' '):
                            optimal_peers2.append(k)
                    #Add the "__ EQUITY" phrase back into the optimal peer list for Bloomberg search
            peers_d[metric] = optimal_peers2
            optimal_model_coeff = optimal_model.rx2('coefficients')
            optimal_model_coeff_df = pd.DataFrame(columns=['Peer','Coefficient'],data=[(optimal_peers2[count],optimal_model_coeff[count + 1]) for count in range(0,len(optimal_model_coeff.names)-1)])
            optimal_model_coeff_df = optimal_model_coeff_df.append({'Peer': 'Intercept', 'Coefficient': optimal_model_coeff[0]}, ignore_index = True)
            peer2coeff = {p:optimal_model_coeff.rx2(p)[0] for p in optimal_model_coeff.names}
            del peer2coeff["(Intercept)"]
            count = 0
            for k in optimal_peers:
                peer2coeff[optimal_peers2[count]] = peer2coeff.pop(k)
                count = count + 1
            peer2coeff["(Intercept)"] = optimal_model_coeff[0]
            metric2peer2coeff[metric] = peer2coeff
            #rows.append([metric, summary_results2, optimal_model_coeff_df])
            #rows[metric] = optimal_model_coeff_df
        else:
            continue
            
    try:
        peer_set = {k:set(v) for k, v in peers_d.items()}
        overlap_peers = set.intersection(*peer_set.values()) #Select overlap group from the optimal regression peers above
    
        #Regression of the overlap group which is then used to calculate downside based on all metrics
        if overlap_peers != set():
            rows2 = {}
            metric2peer2coeff2 = {m:{} for m in metrics}
            for metric in metrics:
                if len(alpha_historical_mult_df[~pd.isnull(alpha_historical_mult_df[metric])])>0:
                    m_df2 = alpha_historical_mult_df[['Date',metric]].rename(columns={metric:ticker2short_ticker[alpha_ticker]})
                    for p in overlap_peers:
                        m_df2 = pd.merge(m_df2,peer2historical_mult_df[p][['Date',metric]],how='left',on='Date').rename(columns={metric:ticker2short_ticker[p]})
                    overlap_peers = [p for p in overlap_peers if len(m_df[~pd.isnull(m_df[p.split(' ')[0]])])>0] # remove peers with all nulls
                    m_ols_df2= m_df2[[alpha_ticker.split(' ')[0]]+[t.split(' ')[0] for t in overlap_peers]]
                    #regress a vs. p1,p2,...,pn
                    formula = alpha_ticker.split(' ')[0] + ' ~. ' #+ " + ".join([t.split(' ')[0] for t in overlap_peers])
                    r_df2 = pandas2ri.py2ri(m_ols_df2)
                    r_df2 = stats.na_omit(r_df2)
                    model2 = stats.lm(formula, data = r_df2)
                    #ols_result = sm.ols(formula=formula, data=m_ols_df).fit()
                    summary = base.summary(model2)
                    summary = pandas2ri.ri2py(summary)
                    model_coeff = model2.rx2('coefficients')
                    model_terms = base.attr(model2.rx2('terms'), "term.labels")
                    overlap_peers2 = []
                    for i in model_terms:
                        for k in overlap_peers:
                            if k.startswith(i + ' '):
                                overlap_peers2.append(k)
                            elif i.startswith('X') and k.startswith(i[1:] + ' '):
                                overlap_peers2.append(k)
                            elif '.' in i:
                                i = i.replace('.', '/')
                                if k.startswith(i + ' '):
                                    overlap_peers2.append(k)
                            #Add the "__ EQUITY" phrase back into the optimal peer list for Bloomberg search
                    peer2coeff2 = {}
                    for p in range(0,len(overlap_peers)):
                        peer2coeff2[overlap_peers2[p]] = model_coeff[p+1]
                    peer2coeff2['Intercept'] = model_coeff[0]
                    metric2peer2coeff2[metric] = peer2coeff2
                    #rows2.append([metric,summary]+[peer2coeff2[p] for p in overlap_peers2]+[peer2coeff2['Intercept']])
                else:
                    continue
                    
            return(metric2peer2coeff, metric2peer2coeff2)
    except:
        print("An exception occurred. There may not be an overlap group.")
            
    return(metric2peer2coeff)


def premium_analysis_df_OLS2_quick_all_changes(dataframes, regression_results, alpha_ticker, analyst_upside, analyst_downside, analyst_pt_wic, tgt_dt,  api_host, adjustments_df_bear=None, adjustments_df_bull=None, adjustments_df_pt=None, bear_flag=None, bull_flag=None, pt_flag=None, fperiod = "1BF"):
    '''
    Calculates downside, upside, and PT WIC using bear, bull, and PT WIC balancesheets. Outputs the information in a JSON 
    string of dataframes and dictionaries for convenience. Contain the Optimal Peer Group calculations as well as the 
    Overlap Peer Group calculations.
    '''
    
    metrics = ['P/EPS', "EV/EBITDA", "EV/Sales", 'DVD yield', 'FCF yield']
    slicer = dfutils.df_slicer()
    price_tgt_dt = datetime.datetime.strptime(tgt_dt, '%Y-%m-%d')
    as_of_dt = datetime.datetime.today()
    
    if len(regression_results) == 2:
        optimal_results = regression_results[0]
        overlap_results = regression_results[1]
    else:
        optimal_results = regression_results
        
    peers = []
    for metric in metrics:
        for x in optimal_results[metric]:
            if x not in peers:
                peers.append(x)
    peers.remove('(Intercept)')
    
    peer2ptd_multiple = dataframes['peer2ptd_multiple']
    peer2now_multiple = dataframes['peer2now_multiple']
    alpha_balance_sheet_df_ptd = multiple_underlying_df(alpha_ticker, price_tgt_dt.strftime('%Y%m%d'), api_host, fperiod='1BF')
    #alpha_balance_sheet_df_now = multiple_underlying_df(alpha_ticker,slicer.prev_n_business_days(100,as_of_dt).strftime('%Y%m%d'),as_of_dt.strftime('%Y%m%d'),api_host,fperiod='1BF')
    
    col_metrics = []
    peers_d = {}
    for metric in metrics:
        if metric in optimal_results.keys():
            peers_d[metric] = list(optimal_results[metric].keys())
            col_metrics.append(metric)
    for metric in metrics:
        del peers_d[metric][-1]    
    
    df = pd.DataFrame(columns = ['Metric'], data = col_metrics)
    
    df['Alpha Upside (analyst)'] = analyst_upside
    df['Alpha Downside (analyst)'] = analyst_downside
    df['Alpha PT WIC (analyst)'] = analyst_pt_wic
    
    df['Peers Multiples DataFrame @ Price Target Date'] = df['Metric'].apply(lambda m: pd.DataFrame(columns=['Peer','Multiple'], data=[(p,peer2ptd_multiple[p][m].fillna(0).iloc[-1]) for p in peers_d[m]]))
    df['Peers Multiples @ Price Target Date'] = df['Metric'].apply(lambda m: {p: peer2ptd_multiple[p][m].fillna(0).iloc[-1] for p in peers_d[m]})
    df['Alpha Implied Multiple @ Price Target Date'] = df['Metric'].apply(lambda m: sum([optimal_results[m][p]*peer2ptd_multiple[p][m].fillna(0).iloc[-1] for p in peers_d[m]]) + optimal_results[m]['(Intercept)'])
    
    if adjustments_df_bear is None:
        alpha_balance_sheet_df_bear = alpha_balance_sheet_df_ptd
        df['Alpha Balance Sheet DataFrame (Bear Case)'] =  [alpha_balance_sheet_df_bear]*len(df)
        df['Alpha Bear Multiple @ Price Target Date'] = [compute_multiple_from_price(m, analyst_downside, alpha_balance_sheet_df_bear) for (m, analyst_downside) in zip(df['Metric'], df['Alpha Downside (analyst)'])]
    else:
        if bear_flag is None:
            adjustments = ast.literal_eval(adjustments_df_bear)[0]
            adjustments_df = pd.DataFrame.from_dict(adjustments, orient='index')
            adjustments_df = adjustments_df.T
            #adjustments_df = adjustments_df.drop(columns='Date')
            cols = adjustments_df.columns
            adjustments_df[cols] = adjustments_df[cols].apply(pd.to_numeric)
            alpha_balance_sheet_df_bear = alpha_balance_sheet_df_ptd.add(adjustments_df, axis='columns')
            df['Alpha Balance Sheet DataFrame (Bear Case)'] = [alpha_balance_sheet_df_bear] * len(df)
            df['Alpha Bear Multiple @ Price Target Date'] = [
                compute_multiple_from_price(m, analyst_downside, alpha_balance_sheet_df_bear) for (m, analyst_downside) in
                zip(df['Metric'], df['Alpha Downside (analyst)'])]
        else:
            adjustments = ast.literal_eval(adjustments_df_bear)[0]
            adjustments_df = pd.DataFrame.from_dict(adjustments, orient='index')
            adjustments_df = adjustments_df.T
            #adjustments_df = adjustments_df.drop(columns='Date')
            cols = adjustments_df.columns
            adjustments_df[cols] = adjustments_df[cols].apply(pd.to_numeric)
            alpha_balance_sheet_df_bear = adjustments_df
            df['Alpha Balance Sheet DataFrame (Bear Case)'] = [alpha_balance_sheet_df_bear] * len(df)
            df['Alpha Bear Multiple @ Price Target Date'] = [
                compute_multiple_from_price(m, analyst_downside, alpha_balance_sheet_df_bear) for (m, analyst_downside) in
                zip(df['Metric'], df['Alpha Downside (analyst)'])]      
            

    if adjustments_df_bull is None:
        alpha_balance_sheet_df_bull = alpha_balance_sheet_df_ptd
        df['Alpha Balance Sheet DataFrame (Bull Case)'] = [alpha_balance_sheet_df_bull] * len(df)
        df['Alpha Bull Multiple @ Price Target Date'] = [
            compute_multiple_from_price(m, analyst_upside, alpha_balance_sheet_df_bull) for (m, analyst_upside) in
            zip(df['Metric'], df['Alpha Upside (analyst)'])]            
    else:
        if bull_flag is None:
            adjustments = ast.literal_eval(adjustments_df_bull)[0]
            adjustments_df = pd.DataFrame.from_dict(adjustments, orient='index')
            adjustments_df = adjustments_df.T
            #adjustments_df = adjustments_df.drop(columns='Date')
            cols = adjustments_df.columns
            adjustments_df[cols] = adjustments_df[cols].apply(pd.to_numeric)
            alpha_balance_sheet_df_bull = alpha_balance_sheet_df_ptd.add(adjustments_df, axis='columns')
            df['Alpha Balance Sheet DataFrame (Bull Case)'] = [alpha_balance_sheet_df_bull] * len(df)
            df['Alpha Bull Multiple @ Price Target Date'] = [
                compute_multiple_from_price(m, analyst_upside, alpha_balance_sheet_df_bull) for (m, analyst_upside) in
                zip(df['Metric'], df['Alpha Upside (analyst)'])]        
        else:
            adjustments = ast.literal_eval(adjustments_df_bear)[0]
            adjustments_df = pd.DataFrame.from_dict(adjustments, orient='index')
            adjustments_df = adjustments_df.T
            #adjustments_df = adjustments_df.drop(columns='Date')
            cols = adjustments_df.columns
            adjustments_df[cols] = adjustments_df[cols].apply(pd.to_numeric)
            alpha_balance_sheet_df_bull = adjustments_df
            df['Alpha Balance Sheet DataFrame (Bull Case)'] = [alpha_balance_sheet_df_bull] * len(df)
            df['Alpha Bull Multiple @ Price Target Date'] = [
                compute_multiple_from_price(m, analyst_upside, alpha_balance_sheet_df_bull) for (m, analyst_upside) in
                zip(df['Metric'], df['Alpha Upside (analyst)'])]   
            
    if adjustments_df_pt is None:
        alpha_balance_sheet_df_pt = alpha_balance_sheet_df_ptd
        df['Alpha Balance Sheet DataFrame (PT WIC Case)'] = [alpha_balance_sheet_df_pt] * len(df)
        df['Alpha PT WIC Multiple @ Price Target Date'] = [
            compute_multiple_from_price(m, analyst_pt_wic, alpha_balance_sheet_df_ptd) for (m, analyst_pt_wic) in
            zip(df['Metric'], df['Alpha PT WIC (analyst)'])]
    else:
        if pt_flag is None:
            adjustments = ast.literal_eval(adjustments_df_pt)[0]
            adjustments_df = pd.DataFrame.from_dict(adjustments, orient='index')
            adjustments_df = adjustments_df.T
            #adjustments_df = adjustments_df.drop(columns='Date')
            cols = adjustments_df.columns
            adjustments_df[cols] = adjustments_df[cols].apply(pd.to_numeric)
            alpha_balance_sheet_df_pt = alpha_balance_sheet_df_ptd.add(adjustments_df, axis='columns')
            df['Alpha Balance Sheet DataFrame (PT WIC Case)'] = [alpha_balance_sheet_df_pt] * len(df)
            df['Alpha PT WIC Multiple @ Price Target Date'] = [
                compute_multiple_from_price(m, analyst_pt_wic, alpha_balance_sheet_df_pt) for (m, analyst_pt_wic) in
                zip(df['Metric'], df['Alpha PT WIC (analyst)'])]
        else:
            adjustments = ast.literal_eval(adjustments_df_pt)[0]
            adjustments_df = pd.DataFrame.from_dict(adjustments, orient='index')
            adjustments_df = adjustments_df.T
            #adjustments_df = adjustments_df.drop(columns='Date')
            cols = adjustments_df.columns
            adjustments_df[cols] = adjustments_df[cols].apply(pd.to_numeric)
            alpha_balance_sheet_df_pt = adjustments_df
            df['Alpha Balance Sheet DataFrame (PT WIC Case)'] = [alpha_balance_sheet_df_pt] * len(df)
            df['Alpha PT WIC Multiple @ Price Target Date'] = [
                compute_multiple_from_price(m, analyst_pt_wic, alpha_balance_sheet_df_pt) for (m, analyst_pt_wic) in
                zip(df['Metric'], df['Alpha PT WIC (analyst)'])]   
     
    df['Premium Bear (%)'] = (((df['Alpha Bear Multiple @ Price Target Date'] / df[
        'Alpha Implied Multiple @ Price Target Date']) * 100.0) - 100.0).astype(float)
    df['Premium PT WIC (%)'] = (((df['Alpha PT WIC Multiple @ Price Target Date'] / df[
        'Alpha Implied Multiple @ Price Target Date']) * 100.0) - 100.0).astype(float)
    df['Premium Bull (%)'] = (((df['Alpha Bull Multiple @ Price Target Date'] / df[
        'Alpha Implied Multiple @ Price Target Date']) * 100.0) - 100.0).astype(float)
            
    df['Peers Multiples DataFrame @ Now'] = df['Metric'].apply(lambda m: pd.DataFrame(columns=['Peer','Multiple'],
                                                                                      data=[(p,peer2now_multiple[p][
                                                                                          m].fillna(0).iloc[-1]) for p
                                                                                            in peers_d[m]]))
    df['Peers Multiples @ Now'] = df['Metric'].apply(
        lambda m: {p: peer2now_multiple[p][m].fillna(0).iloc[-1] for p in peers_d[m]})
    
    df['Alpha Implied Multiple @ Now'] = df['Metric'].apply(lambda m: sum(
        [optimal_results[m][p]*peer2now_multiple[p][m].fillna(0).iloc[-1] for p in peers_d[m]])
                                                            +optimal_results[m]['(Intercept)'])
    
    df['Alpha Bear Multiple @ Now'] = (df['Alpha Implied Multiple @ Now']*(1 + (df['Premium Bear (%)'] / 100.0))).astype(float)
    df['Alpha PT WIC Multiple @ Now'] = (df['Alpha Implied Multiple @ Now']*(1 + (df['Premium PT WIC (%)'] / 100.0))).astype(float)
    df['Alpha Bull Multiple @ Now'] = (df['Alpha Implied Multiple @ Now']*(1 + (df['Premium Bull (%)'] / 100.0))).astype(float)
        
    df['Alpha Downside'] = [compute_implied_price_from_multiple(m, mult, alpha_balance_sheet_df_bear) for (m, mult) in
                            zip(df['Metric'], df['Alpha Bear Multiple @ Now'])]
    df['Alpha PT WIC'] = [compute_implied_price_from_multiple(m, mult, alpha_balance_sheet_df_pt) for (m, mult) in
                            zip(df['Metric'], df['Alpha PT WIC Multiple @ Now'])]
    df['Alpha Upside'] = [compute_implied_price_from_multiple(m, mult, alpha_balance_sheet_df_bull) for (m, mult) in
                            zip(df['Metric'], df['Alpha Bull Multiple @ Now'])]
    
    #Start the overlap peers analysis
    if len(regression_results) == 2:
        for metric in metrics:
            if metric in overlap_results.keys():
                overlap_peers = list(overlap_results[metric].keys())
                break
    
        overlap_peers.remove('Intercept')

        df2 = pd.DataFrame(columns = ['Metric'], data = col_metrics)

        df2['Alpha Upside (analyst)'] = analyst_upside
        df2['Alpha Downside (analyst)'] = analyst_downside
        df2['Alpha PT WIC (analyst)'] = analyst_pt_wic

        df2['Peers Multiples DataFrame @ Price Target Date'] = df2['Metric'].apply(lambda m: pd.DataFrame(columns=['Peer','Multiple'], data=[(p,peer2ptd_multiple[p][m].fillna(0).iloc[-1]) for p in overlap_peers]))
        df2['Peers Multiples @ Price Target Date'] = df2['Metric'].apply(lambda m: {p: peer2ptd_multiple[p][m].fillna(0).iloc[-1] for p in overlap_peers})
        df2['Alpha Implied Multiple @ Price Target Date'] = df2['Metric'].apply(lambda m: sum([overlap_results[m][p]*peer2ptd_multiple[p][m].fillna(0).iloc[-1] for p in overlap_peers]) + overlap_results[m]['Intercept'])

        if adjustments_df_bear is None:
            alpha_balance_sheet_df_bear = alpha_balance_sheet_df_ptd
            df2['Alpha Balance Sheet DataFrame (Bear Case)'] =  [alpha_balance_sheet_df_bear]*len(df)
            df2['Alpha Bear Multiple @ Price Target Date'] = [compute_multiple_from_price(m, analyst_downside, alpha_balance_sheet_df_bear) for (m, analyst_downside) in zip(df2['Metric'], df2['Alpha Downside (analyst)'])]
        else:
            if bear_flag is None:
                adjustments = ast.literal_eval(adjustments_df_bear)[0]
                adjustments_df = pd.DataFrame.from_dict(adjustments, orient='index')
                adjustments_df = adjustments_df.T
                #adjustments_df = adjustments_df.drop(columns='Date')
                cols = adjustments_df.columns
                adjustments_df[cols] = adjustments_df[cols].apply(pd.to_numeric)
                alpha_balance_sheet_df_bear = alpha_balance_sheet_df_ptd.add(adjustments_df, axis='columns')
                df2['Alpha Balance Sheet DataFrame (Bear Case)'] = [alpha_balance_sheet_df_bear] * len(df)
                df2['Alpha Bear Multiple @ Price Target Date'] = [
                    compute_multiple_from_price(m, analyst_downside, alpha_balance_sheet_df_bear) for (m, analyst_downside) in
                    zip(df2['Metric'], df2['Alpha Downside (analyst)'])]
            else:
                adjustments = ast.literal_eval(adjustments_df_bear)[0]
                adjustments_df = pd.DataFrame.from_dict(adjustments, orient='index')
                adjustments_df = adjustments_df.T
                #adjustments_df = adjustments_df.drop(columns='Date')
                cols = adjustments_df.columns
                adjustments_df[cols] = adjustments_df[cols].apply(pd.to_numeric)
                alpha_balance_sheet_df_bear = adjustments_df
                df2['Alpha Balance Sheet DataFrame (Bear Case)'] = [alpha_balance_sheet_df_bear] * len(df)
                df2['Alpha Bear Multiple @ Price Target Date'] = [
                    compute_multiple_from_price(m, analyst_downside, alpha_balance_sheet_df_bear) for (m, analyst_downside) in
                    zip(df2['Metric'], df2['Alpha Downside (analyst)'])]      


        if adjustments_df_bull is None:
            alpha_balance_sheet_df_bull = alpha_balance_sheet_df_ptd
            df2['Alpha Balance Sheet DataFrame (Bull Case)'] = [alpha_balance_sheet_df_bull] * len(df)
            df2['Alpha Bull Multiple @ Price Target Date'] = [
                compute_multiple_from_price(m, analyst_upside, alpha_balance_sheet_df_bull) for (m, analyst_upside) in
                zip(df2['Metric'], df2['Alpha Upside (analyst)'])]            
        else:
            if bull_flag is None:
                adjustments = ast.literal_eval(adjustments_df_bull)[0]
                adjustments_df = pd.DataFrame.from_dict(adjustments, orient='index')
                adjustments_df = adjustments_df.T
                #adjustments_df = adjustments_df.drop(columns='Date')
                cols = adjustments_df.columns
                adjustments_df[cols] = adjustments_df[cols].apply(pd.to_numeric)
                alpha_balance_sheet_df_bull = alpha_balance_sheet_df_ptd.add(adjustments_df, axis='columns')
                df2['Alpha Balance Sheet DataFrame (Bull Case)'] = [alpha_balance_sheet_df_bull] * len(df)
                df2['Alpha Bull Multiple @ Price Target Date'] = [
                    compute_multiple_from_price(m, analyst_upside, alpha_balance_sheet_df_bull) for (m, analyst_upside) in
                    zip(df2['Metric'], df2['Alpha Upside (analyst)'])]        
            else:
                adjustments = ast.literal_eval(adjustments_df_bear)[0]
                adjustments_df = pd.DataFrame.from_dict(adjustments, orient='index')
                adjustments_df = adjustments_df.T
                #adjustments_df = adjustments_df.drop(columns='Date')
                cols = adjustments_df.columns
                adjustments_df[cols] = adjustments_df[cols].apply(pd.to_numeric)
                alpha_balance_sheet_df_bull = adjustments_df
                df2['Alpha Balance Sheet DataFrame (Bull Case)'] = [alpha_balance_sheet_df_bull] * len(df)
                df2['Alpha Bull Multiple @ Price Target Date'] = [
                    compute_multiple_from_price(m, analyst_upside, alpha_balance_sheet_df_bull) for (m, analyst_upside) in
                    zip(df2['Metric'], df2['Alpha Upside (analyst)'])]   

        if adjustments_df_pt is None:
            alpha_balance_sheet_df_pt = alpha_balance_sheet_df_ptd
            df2['Alpha Balance Sheet DataFrame (PT WIC Case)'] = [alpha_balance_sheet_df_pt] * len(df)
            df2['Alpha PT WIC Multiple @ Price Target Date'] = [
                compute_multiple_from_price(m, analyst_pt_wic, alpha_balance_sheet_df_ptd) for (m, analyst_pt_wic) in
                zip(df2['Metric'], df2['Alpha PT WIC (analyst)'])]
        else:
            if pt_flag is None:
                adjustments = ast.literal_eval(adjustments_df_pt)[0]
                adjustments_df = pd.DataFrame.from_dict(adjustments, orient='index')
                adjustments_df = adjustments_df.T
                #adjustments_df = adjustments_df.drop(columns='Date')
                cols = adjustments_df.columns
                adjustments_df[cols] = adjustments_df[cols].apply(pd.to_numeric)
                alpha_balance_sheet_df_pt = alpha_balance_sheet_df_ptd.add(adjustments_df, axis='columns')
                df2['Alpha Balance Sheet DataFrame (PT WIC Case)'] = [alpha_balance_sheet_df_pt] * len(df)
                df2['Alpha PT WIC Multiple @ Price Target Date'] = [
                    compute_multiple_from_price(m, analyst_pt_wic, alpha_balance_sheet_df_pt) for (m, analyst_pt_wic) in
                    zip(df2['Metric'], df2['Alpha PT WIC (analyst)'])]
            else:
                adjustments = ast.literal_eval(adjustments_df_pt)[0]
                adjustments_df = pd.DataFrame.from_dict(adjustments, orient='index')
                adjustments_df = adjustments_df.T
                #adjustments_df = adjustments_df.drop(columns='Date')
                cols = adjustments_df.columns
                adjustments_df[cols] = adjustments_df[cols].apply(pd.to_numeric)
                alpha_balance_sheet_df_pt = adjustments_df
                df2['Alpha Balance Sheet DataFrame (PT WIC Case)'] = [alpha_balance_sheet_df_pt] * len(df)
                df2['Alpha PT WIC Multiple @ Price Target Date'] = [
                    compute_multiple_from_price(m, analyst_pt_wic, alpha_balance_sheet_df_pt) for (m, analyst_pt_wic) in
                    zip(df2['Metric'], df2['Alpha PT WIC (analyst)'])]   

        df2['Premium Bear (%)'] = (((df2['Alpha Bear Multiple @ Price Target Date'] / df2[
            'Alpha Implied Multiple @ Price Target Date']) * 100.0) - 100.0).astype(float)
        df2['Premium PT WIC (%)'] = (((df2['Alpha PT WIC Multiple @ Price Target Date'] / df2[
            'Alpha Implied Multiple @ Price Target Date']) * 100.0) - 100.0).astype(float)
        df2['Premium Bull (%)'] = (((df2['Alpha Bull Multiple @ Price Target Date'] / df2[
            'Alpha Implied Multiple @ Price Target Date']) * 100.0) - 100.0).astype(float)

        df2['Peers Multiples DataFrame @ Now'] = df2['Metric'].apply(lambda m: pd.DataFrame(columns=['Peer','Multiple'],
                                                                                          data=[(p,peer2now_multiple[p][
                                                                                              m].fillna(0).iloc[-1]) for p
                                                                                                in peers_d[m]]))
        df2['Peers Multiples @ Now'] = df2['Metric'].apply(
            lambda m: {p: peer2now_multiple[p][m].fillna(0).iloc[-1] for p in overlap_peers})

        df2['Alpha Implied Multiple @ Now'] = df2['Metric'].apply(lambda m: sum(
            [overlap_results[m][p]*peer2now_multiple[p][m].fillna(0).iloc[-1] for p in overlap_peers])
                                                                +overlap_results[m]['Intercept'])

        df2['Alpha Bear Multiple @ Now'] = (df2['Alpha Implied Multiple @ Now']*(1 + (df2['Premium Bear (%)'] / 100.0))).astype(float)
        df2['Alpha PT WIC Multiple @ Now'] = (df2['Alpha Implied Multiple @ Now']*(1 + (df2['Premium PT WIC (%)'] / 100.0))).astype(float)
        df2['Alpha Bull Multiple @ Now'] = (df2['Alpha Implied Multiple @ Now']*(1 + (df2['Premium Bull (%)'] / 100.0))).astype(float)

        df2['Alpha Downside'] = [compute_implied_price_from_multiple(m, mult, alpha_balance_sheet_df_bear) for (m, mult) in
                                zip(df2['Metric'], df2['Alpha Bear Multiple @ Now'])]
        df2['Alpha PT WIC'] = [compute_implied_price_from_multiple(m, mult, alpha_balance_sheet_df_pt) for (m, mult) in
                                zip(df2['Metric'], df2['Alpha PT WIC Multiple @ Now'])]
        df2['Alpha Upside'] = [compute_implied_price_from_multiple(m, mult, alpha_balance_sheet_df_bull) for (m, mult) in
                                zip(df2['Metric'], df2['Alpha Bull Multiple @ Now'])]

        #optimal_peer_results = {df['Metric'][i]: {'Alpha Downside': df['Alpha Downside'][i], 'Alpha PT WIC': df['Alpha PT WIC'][i], 'Alpha Upside': df['Alpha Upside'][i]} for i in range(0,len(df))}
                
        #overlap_peer_results = {df2['Metric'][i]: {'Alpha Downside': df2['Alpha Downside'][i], 'Alpha PT WIC': df2['Alpha PT WIC'][i], 'Alpha Upside': df2['Alpha Upside'][i]} for i in range(0,len(df2))}
         
        #results are displayed in dataframe format and dictionary format for whichever is easiest to use     
        return { 'optimal_peer_results_dict': df.set_index('Metric').to_dict(),
                'optimal_peer_results_df': df,
                'overlap_peer_results_dict': df2.set_index('Metric').to_dict(),
                'overlap_peer_results_df': df2,
                }
        
    #optimal_peer_results = {df['Metric'][i]: {'Alpha Downside': df['Alpha Downside'][i], 'Alpha PT WIC': df['Alpha PT WIC'][i], 'Alpha Upside': df['Alpha Upside'][i]} for i in range(0,len(df))}
        
    return {
        'optimal_peer_results_dict': df.set_index('Metric').to_dict(),
        'optimal_peer_results_df': df
    }
