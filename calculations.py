"""
core/calculations.py
====================
Pure pandas / numpy momentum metric functions.
Zero dependency on Streamlit — identical to the originals in momn_streamlit_app_v10.py.
"""

import datetime as _dt
import numpy as np
import pandas as pd


def getMedianVolume(data):
    return data.median().round(0)

def getDailyReturns(data):
    return data.ffill().pct_change()

def getMaskDailyChange(data):
    dr = getDailyReturns(data)
    m1 = dr.eq(np.inf)
    m2 = dr.eq(-np.inf)
    return (dr.mask(m1, data[~m1].max(), axis=1)
              .mask(m2, data[~m2].min(), axis=1)
              .bfill(axis=1))

def getStdev(data):
    return np.std(getMaskDailyChange(data) * 100)

def getAbsReturns(data):
    return round((data.iloc[-1] / data.iloc[0] - 1) * 100, 2)

def getVolatility(data):
    return round(data.std(ddof=0) * np.sqrt(252) * 100, 2)

def getMonthlyPrices(data):
    grps = data.groupby([data.index.year, data.index.month])
    monthly = pd.DataFrame()
    for k in grps:
        monthly = pd.concat([monthly, k[1].tail(1)])
    return monthly

def getMonthlyReturns(data):
    return data.pct_change()

def getSharpe(data):
    return round(np.sqrt(252) * data.mean() / data.std(), 2)

def getSortino(data):
    return np.sqrt(252) * data.mean() / data[data < 0].std()

def getMaxDrawdown(data):
    cummRet = (data + 1).cumprod()
    peak = cummRet.expanding(min_periods=1).max()
    return ((cummRet / peak) - 1).min()

def getCalmar(data):
    return data.mean() * 252 / abs(getMaxDrawdown(data))

def getNMonthRoC(data, N):
    return round((data.iloc[-1] / data.iloc[-1 - N] - 1) * 100, 2)

def getFIP(data):
    retPos = np.sum(data.pct_change()[1:] > 0)
    retNeg = np.sum(data.pct_change()[1:] < 0)
    return retPos - retNeg

def getSharpeRoC(roc, volatility):
    return round(roc / volatility, 2)

def getBeta(dfNifty, data12M):
    dailyReturns = getDailyReturns(pd.concat([dfNifty, data12M], axis=1))[1:]
    var = np.var(dailyReturns['Nifty'])
    cov = dailyReturns.cov()
    return [round(cov.loc[k, 'Nifty'] / var, 2) for k in cov.columns[1:]]

def calculate_z_score(data):
    mean, std = data.mean(), data.std()
    return ((data - mean) / std).round(2)


def build_dfStats(close, high, volume, dates, ranking_method):
    # ── Guard: future date ────────────────────────────────────
    today = _dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    if dates['endDate'] > today:
        raise ValueError(
            f"Lookback Date '{dates['endDate'].strftime('%d-%m-%Y')}' future date hai!\n"
            f"Aaj ki date select karo: {today.strftime('%d-%m-%Y')}"
        )

    # ── Guard: empty data ─────────────────────────────────────
    if close.empty:
        raise ValueError("Koi data download nahi hua. Internet check karo aur retry karo.")

    symbol = list(close.columns)

    data20Y   = close.loc[:dates['endDate']].copy()
    volume20Y = volume.loc[:dates['endDate']].copy()
    high20Y   = high.loc[:dates['endDate']].copy()

    data12M   = data20Y.loc[dates['date12M']:].copy()
    data9M    = data20Y.loc[dates['date9M']:].copy()
    data6M    = data20Y.loc[dates['date6M']:].copy()
    data3M    = data20Y.loc[dates['date3M']:].copy()
    data1M    = data20Y.loc[dates['date1M']:].copy()
    volume12M = volume20Y.loc[dates['date12M']:].copy()

    # ── Guard: sliced empty ───────────────────────────────────
    for name, df in [('12M', data12M), ('9M', data9M), ('6M', data6M),
                     ('3M', data3M), ('1M', data1M)]:
        if df.empty or len(df) < 2:
            raise ValueError(
                f"data{name} empty hai!\n"
                f"Future date ya market holiday select hua hai.\n"
                f"Aaj ki ya recent past trading date select karo."
            )

    dfStats = pd.DataFrame(index=symbol)
    dfStats['Close']   = round(data12M.iloc[-1], 2)
    data12M_Temp = data12M.fillna(0)
    dfStats['dma200d'] = round(data12M_Temp.rolling(window=200).mean().iloc[-1], 2)

    dfStats['roc12M'] = getAbsReturns(data12M)
    dfStats['roc9M']  = getAbsReturns(data9M)
    dfStats['roc6M']  = getAbsReturns(data6M)
    dfStats['roc3M']  = getAbsReturns(data3M)
    dfStats['roc1M']  = getAbsReturns(data1M)

    dfStats['vol12M'] = getVolatility(getDailyReturns(data12M))
    dfStats['vol9M']  = getVolatility(getDailyReturns(data9M))
    dfStats['vol6M']  = getVolatility(getDailyReturns(data6M))
    dfStats['vol3M']  = getVolatility(getDailyReturns(data3M))

    dfStats['sharpe12M'] = getSharpeRoC(dfStats['roc12M'], dfStats['vol12M'])
    dfStats['sharpe9M']  = getSharpeRoC(dfStats['roc9M'],  dfStats['vol9M'])
    dfStats['sharpe6M']  = getSharpeRoC(dfStats['roc6M'],  dfStats['vol6M'])
    dfStats['sharpe3M']  = getSharpeRoC(dfStats['roc3M'],  dfStats['vol3M'])

    dfStats['z_score12M'] = calculate_z_score(dfStats['sharpe12M'])
    dfStats['z_score9M']  = calculate_z_score(dfStats['sharpe9M'])
    dfStats['z_score6M']  = calculate_z_score(dfStats['sharpe6M'])
    dfStats['z_score3M']  = calculate_z_score(dfStats['sharpe3M'])

    for col in ['sharpe12M','sharpe9M','sharpe6M','sharpe3M',
                'z_score12M','z_score9M','z_score6M','z_score3M']:
        dfStats[col] = dfStats[col].replace([np.inf, -np.inf], np.nan).fillna(0)

    if ranking_method == "avgSharpe12_6_3":
        dfStats['avgSharpe12_6_3'] = dfStats[["sharpe12M","sharpe6M","sharpe3M"]].mean(axis=1).round(2)
    elif ranking_method == "avg_All":
        dfStats['avg_All'] = dfStats[["sharpe12M","sharpe9M","sharpe6M","sharpe3M"]].mean(axis=1).round(2)
    elif ranking_method == "avgSharpe9_6_3":
        dfStats['avgSharpe9_6_3'] = dfStats[["sharpe9M","sharpe6M","sharpe3M"]].mean(axis=1).round(2)
    elif ranking_method == "avgZScore12_6_3":
        dfStats['avgZScore12_6_3'] = dfStats[['z_score12M','z_score6M','z_score3M']].mean(axis=1).round(2)
    elif ranking_method == "avgZScore12_9_6_3":
        dfStats['avgZScore12_9_6_3'] = dfStats[['z_score12M','z_score9M','z_score6M','z_score3M']].mean(axis=1).round(2)

    dfStats['volm_cr']  = (getMedianVolume(volume12M) / 1e7).round(2)
    dfStats['ATH']      = round(high20Y.max(), 2)
    dfStats['AWAY_ATH'] = round((dfStats['Close'] / dfStats['ATH'] - 1) * 100, 2)

    dataDaily_pct = round(getDailyReturns(data12M) * 100, 2)
    dfStats['circuit'] = (
        (dataDaily_pct ==  4.99).sum() + (dataDaily_pct ==  5.00).sum() +
        (dataDaily_pct ==  9.99).sum() + (dataDaily_pct == 10.00).sum() +
        (dataDaily_pct == 19.99).sum() + (dataDaily_pct == 20.00).sum() +
        (dataDaily_pct == -4.99).sum() + (dataDaily_pct == -5.00).sum() +
        (dataDaily_pct == -9.99).sum() + (dataDaily_pct == -10.00).sum() +
        (dataDaily_pct == -19.99).sum() + (dataDaily_pct == -20.00).sum()
    )

    dataDaily_pct5 = round(getDailyReturns(data3M) * 100, 2)
    dfStats['circuit5'] = (
        (dataDaily_pct5 ==  4.99).sum() + (dataDaily_pct5 ==  5.00).sum() +
        (dataDaily_pct5 == -4.99).sum() + (dataDaily_pct5 == -5.00).sum()
    )

    dfStats = dfStats.reset_index().rename(columns={'index': 'Ticker'})
    dfStats['Ticker'] = dfStats['Ticker'].astype(str).str.replace('.NS', '', regex=False)

    for col in ['avgSharpe12_6_3','avg_All','avgSharpe9_6_3',
                'avgZScore12_6_3','avgZScore12_9_6_3']:
        if col in dfStats.columns:
            dfStats[col] = dfStats[col].replace([np.inf, -np.inf], np.nan).fillna(0)

    if ranking_method in ["avg_All", "sharpe12M"]:
        dfStats = dfStats.sort_values(by=[ranking_method, 'roc12M'], ascending=[False, False])
    elif ranking_method in ["avgSharpe12_6_3", "sharpe3M"]:
        dfStats = dfStats.sort_values(by=[ranking_method, 'roc3M'],  ascending=[False, False])
    elif ranking_method == "avgSharpe9_6_3":
        dfStats = dfStats.sort_values(by=[ranking_method, 'roc6M'],  ascending=[False, False])
    elif ranking_method == "avgZScore12_6_3":
        dfStats = dfStats.sort_values(by=[ranking_method, 'roc3M'],  ascending=[False, False])
    elif ranking_method == "avgZScore12_9_6_3":
        dfStats = dfStats.sort_values(by=[ranking_method, 'roc6M'],  ascending=[False, False])

    dfStats['Rank'] = range(1, len(dfStats) + 1)
    dfStats = dfStats.set_index('Rank')
    return dfStats


def apply_filters(dfStats, filter_params: dict = None):
    """
    Apply momentum filters. filter_params dict overrides defaults:
      volm_cr_min  : float  default 1
      use_dma200   : bool   default True
      use_roc12    : bool   default True
      circuit_max  : int    default 20
      use_away_ath : bool   default True
      use_roc_cap  : bool   default True
      close_min    : float  default 30
      circuit5_max : int    default 10
    """
    p = filter_params or {}
    volm_min     = p.get('volm_cr_min',  1.0)
    use_dma200   = p.get('use_dma200',   True)
    use_roc12    = p.get('use_roc12',    True)
    circuit_max  = p.get('circuit_max',  20)
    use_away_ath = p.get('use_away_ath', True)
    use_roc_cap  = p.get('use_roc_cap',  True)
    close_min    = p.get('close_min',    30.0)
    circuit5_max = p.get('circuit5_max', 10)

    mask = pd.Series([True] * len(dfStats), index=dfStats.index)
    if volm_min     > 0:    mask &= dfStats['volm_cr']   > volm_min
    if use_dma200:          mask &= dfStats['Close']      > dfStats['dma200d']
    if use_roc12:           mask &= dfStats['roc12M']     > 5.5
    if circuit_max  < 999:  mask &= dfStats['circuit']   < circuit_max
    if use_away_ath:        mask &= dfStats['AWAY_ATH']  > -25
    if use_roc_cap:         mask &= dfStats['roc12M']    < 1000
    if close_min    > 0:    mask &= dfStats['Close']     > close_min
    if circuit5_max < 999:  mask &= dfStats['circuit5']  <= circuit5_max

    dfStats['final_momentum'] = mask
    return dfStats[mask].sort_values('Rank', ascending=True)
