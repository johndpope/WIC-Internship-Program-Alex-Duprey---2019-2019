# -*- coding: utf-8 -*-
"""
Created on Tue Apr 16 16:55:01 2019

@author: nmiraj
"""
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
df=pd.read_csv('C:/Users/nmiraj/Documents/Python code/drawdown_example.csv')
df.Ret_index=(1+df.Daily_return).cumprod()
last=df.Ret_index.iloc[-1]
maxret=df.Ret_index.max()
maxloc=df.Ret_index.idxmax()
minret=df.Ret_index[maxloc:].min()
curr_drawdown=(maxret-last)/maxret
max_drawdown =(maxret-minret)/maxret
maxdate=df.loc[df.Ret_index==maxret]['Date'].values[0]
maxdrawdate=df.loc[df.Ret_index==minret]['Date'].values[0]
print(curr_drawdown, max_drawdown, maxret, maxdate, maxloc, maxdrawdate)
plt.figure(figsize=(12,10))
plt.xticks(np.arange(0,100,5),rotation=70)
plt.plot(df.Date,df.Ret_index)
plt.plot([maxloc],[maxret],'o',color="Red",markersize=10)