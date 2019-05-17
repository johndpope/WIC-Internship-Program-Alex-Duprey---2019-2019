__author__ = 'pgrimberg'
import json
import pandas as pd
class Trade:
    def __init__(self, fund_name, qty, prc, trade_id, tradegroup, secid):
        self.fund_name = fund_name
        self.qty = qty
        self.prc = prc
        self.trade_id = trade_id
        self.tradegroup = tradegroup
        self.secid = secid



class ESS_IDEA:

    def __init__(self, df):
        self.df = df
        self.db_id2value = json.loads(df['JSON'].iloc[0])

    def alpha_ticker(self):
        return self.df['Alpha Ticker'].iloc[0]

    def peer_tickers(self):
        return [self.db_id2value[k] for k in self.db_id2value if 'peer_ticker_' in k]

    def price_target_date_dt(self):
        if 'price_target_date' not in self.db_id2value: return None
        price_tgt_dt = self.db_id2value['price_target_date']
        if pd.isnull(price_tgt_dt): return None
        return pd.to_datetime(price_tgt_dt)

    def unaffected_date_dt(self):
        if 'unaffected_date' not in self.db_id2value: return None
        unaffected_dt = self.db_id2value['unaffected_date']
        if pd.isnull(unaffected_dt): return None
        return pd.to_datetime(unaffected_dt)

    def metric2weight(self):
        serial_numbers = [k.split('_')[-1] for k in self.db_id2value.keys() if k.startswith('val_metric_name_')]
        metric2weight = {self.db_id2value['val_metric_name_'+sn] : float(self.db_id2value['val_metric_weight_'+sn]) for sn in serial_numbers}
        if 'P/EPS' in metric2weight: metric2weight['P/E'] = metric2weight['P/EPS']  # patch, adjustment to name - change name of metric in ess form
        return metric2weight

    def peer2weight(self):
        serial_numbers = [k.split('_')[-1] for k in self.db_id2value.keys() if k.startswith('peer_ticker_')]
        p2w = {}
        for sn in serial_numbers:
            p = self.db_id2value['peer_ticker_'+sn]
            try:
                w = float(self.db_id2value['peer_weight_'+sn])
                p2w[p] = w
            except:
                p2w[p] = 0
        return p2w

    def analyst_upside(self):
        if 'alpha_up_price_target' not in self.db_id2value: return None
        return  float(self.db_id2value['alpha_up_price_target'])

    def is_complete(self):
        if 'is_complete_checkbox' not in self.db_id2value: return None
        return self.db_id2value['is_complete_checkbox']

    def allocated_to(self):
        if 'allocated_to' not in self.db_id2value: return None
        return self.db_id2value['allocated_to']




class HOLIDAY:
    @staticmethod
    def get_holidays():
        return \
            ["2016-01-01","2016-01-18","2016-02-15","2016-03-25","2016-05-30",
             "2016-07-04","2016-09-05","2016-11-24","2016-12-25",
             "2017-01-02","2017-01-16","2017-02-20","2017-04-14","2017-05-29",'2017-07-04',
             "2017-09-04","2017-11-23","2017-12-25",
             "2018-01-01","2018-01-13","2018-02-19","2018-03-30","2018-05-28",'2018-07-04',
             "2018-09-03","2018-11-22","2018-12-25"
             ]