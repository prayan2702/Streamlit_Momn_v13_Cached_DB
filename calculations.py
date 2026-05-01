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
    # ffill() → interior/trailing NaN fill (holidays ke beech ke gaps)
    # bfill(limit=5) → sirf max 5 din ka leading NaN fill (Indian market holidays max 3-4 din hote hain)
    # limit=5 kyun: unbounded bfill() AHLWEST jaise thin/suspended stocks mein weeks of missing data
    # backward fill karta hai → spurious roc (40%+) + near-zero vol → sharpe ~1356 → outlier z-score
    # → poori universe ki z-scores collapse ho jaati hain ek stock ki wajah se.
    d = data.ffill().bfill(limit=5)
    return round((d.iloc[-1] / d.iloc[0] - 1) * 100, 2)

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
    # inf replace pehle karo — agar kisi stock ka sharpe=inf (vol=0 wale stocks: GOLDBEES etc.)
    # to mean=inf → std=NaN → sabka z_score=NaN → fillna(0) → sabka z_score=0!
    # Ye hi pre-cached mode mein "all z-scores zero" bug ka root cause tha.
    clean = data.replace([np.inf, -np.inf], np.nan)
    mean, std = clean.mean(), clean.std()
    if std == 0 or pd.isna(std):
        return pd.Series(0.0, index=data.index)
    return ((clean - mean) / std).round(2)


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

    # ── 200 DMA — Dedicated buffer (data12M se alag) ─────────────────────────
    # FIX 1: data12M pe depend karna fragile tha — agar 12M window mein NSE
    #         holidays zyada hon aur rows < 200 aaye to sab stocks ka dma200d=NaN.
    #         Ab data20Y se last 290 calendar days ka dedicated slice use karo.
    #         290 calendar days ≈ 200 NSE trading days + ~45 days buffer
    #         (NSE ~245 trading days/year; 290 cal days ≈ 207 trading days).
    #
    # FIX 2: min_periods=150 — Indian market ke T-group / Z-group / SME stocks
    #         jo kabhi kabhi weeks ke liye halt hote hain. Unke liye 200 rows
    #         nahi milte ffill ke baad bhi. min_periods=150 se approximate DMA
    #         milta hai (NaN se behtar — warna filter mein silently exclude hote hain).
    #
    # FIX 3: fillna(0) — agar kisi naye listed stock ka data 150 rows se bhi kam
    #         ho, dma200d=0 rahega. valid_dma filter (dma200d > 0) already aise
    #         stocks ko regime breadth calculation se bahar rakhta hai. ✅
    dma_end   = dates['endDate']
    dma_start = dma_end - pd.DateOffset(days=290)
    data_dma  = data20Y.loc[dma_start:dma_end].ffill()
    dfStats['dma200d'] = (
        data_dma
        .rolling(window=200, min_periods=150)
        .mean()
        .iloc[-1]
        .round(2)
        .fillna(0)
    )

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

    # ── Circuit detection — tolerance-based (±0.015%) ────────────────────────
    # FIX: Pehle exact == comparison (4.99, 5.00) use hota tha.
    #      IEEE 754 floating point mein round(x*100, 2) ke baad bhi
    #      5.005... → 5.01 ya 4.994... → 4.99 ho sakta hai — exact == miss karta.
    #      Tolerance ±0.015%: 5.0 ± 0.015 = 4.985 to 5.015. Itna band
    #      normal returns ko galti se include nahi karta (4.98 = valid return),
    #      lekin floating point artifacts se koi circuit hit miss nahi hoga.
    # _CIRCUIT_LEVELS: NSE ke standard upper circuit limits — 5%, 10%, 20%.
    _CIRCUIT_LEVELS = [5.0, 10.0, 20.0]
    _TOL = 0.015  # ±0.015% tolerance — float artifact se bada, real gap se chhota

    dataDaily_pct = getDailyReturns(data12M) * 100
    dfStats['circuit'] = sum(
        ((dataDaily_pct - lvl).abs() < _TOL).sum() +
        ((dataDaily_pct + lvl).abs() < _TOL).sum()
        for lvl in _CIRCUIT_LEVELS
    )

    # circuit5 — sirf 5% circuit, last 3M data (recent manipulation filter)
    dataDaily_pct5 = getDailyReturns(data3M) * 100
    dfStats['circuit5'] = (
        ((dataDaily_pct5 - 5.0).abs() < _TOL).sum() +
        ((dataDaily_pct5 + 5.0).abs() < _TOL).sum()
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


# ══════════════════════════════════════════════════════════════════════════════
# REGIME-TAA FUNCTIONS  (added for Multi-Asset Overlay)
# ══════════════════════════════════════════════════════════════════════════════

def get_regime_score(dfStats, equity_nav_series=None):
    """
    3-signal market regime score.

    S1 — Equity Curve Trend : MOMN PF NAV > 200DMA (from Portfolio Dashboard)
    S2 — Market Breadth     : % stocks with Close > 200DMA > 50%
    S3 — Universe Momentum  : Median stock 3M ROC > 0%

    equity_nav_series : list/Series of daily NAV values (momnPF from benchmarking.rows)
                        If None or empty → S1 defaults to 1 (conservative fallback)

    Returns dict: score, label, equity, gold, cash, signals, breadth_pct,
                  median_roc3m, nav_current, nav_dma200, nav_series_len
    """
    if len(dfStats) == 0:
        return _regime_default()

    # ── S2: Market Breadth ────────────────────────────────────────
    valid_dma   = dfStats[dfStats['dma200d'] > 0]
    above_dma   = (valid_dma['Close'] > valid_dma['dma200d']).sum()
    breadth_pct = round(above_dma / len(valid_dma) * 100, 1) if len(valid_dma) > 0 else 0.0
    s2 = 1 if breadth_pct > 50.0 else 0

    # ── S3: Median 3M ROC ─────────────────────────────────────────
    median_roc3m = round(float(dfStats['roc3M'].median()), 2)
    s3 = 1 if median_roc3m > 0.0 else 0

    # ── S1: Equity Curve vs 200DMA ────────────────────────────────
    nav_current = nav_dma200 = None
    nav_series_len = 0
    s1 = 1   # safe fallback

    if equity_nav_series is not None and len(equity_nav_series) >= 5:
        nav_s = pd.Series(equity_nav_series).dropna()
        nav_series_len = len(nav_s)
        nav_current    = round(float(nav_s.iloc[-1]), 4)
        window         = min(200, nav_series_len)
        nav_dma200     = round(float(nav_s.rolling(window).mean().iloc[-1]), 4)
        s1 = 1 if nav_current > nav_dma200 else 0

    score = s1 + s2 + s3
    alloc = {
        # SOP v2026.06 — Gold floor 15%, Equity/Cash rebalanced
        3: {'label': 'Strong Bull', 'equity': 0.80, 'gold': 0.15, 'cash': 0.05},
        2: {'label': 'Mild Bull',   'equity': 0.65, 'gold': 0.20, 'cash': 0.15},
        1: {'label': 'Neutral',     'equity': 0.45, 'gold': 0.25, 'cash': 0.30},
        0: {'label': 'Bear',        'equity': 0.25, 'gold': 0.30, 'cash': 0.45},
    }[score]

    return {
        'score'           : score,
        'breadth_pct'     : breadth_pct,
        'median_roc3m'    : median_roc3m,
        'stocks_above_dma': int(above_dma),
        'total_stocks'    : int(len(valid_dma)),
        'nav_current'     : nav_current,
        'nav_dma200'      : nav_dma200,
        'nav_series_len'  : nav_series_len,
        'signals'         : {'s1_equity_curve': s1, 's2_breadth': s2, 's3_momentum': s3},
        **alloc,
    }


def _regime_default():
    return {
        'score': 2, 'label': 'Mild Bull', 'equity': 0.65, 'gold': 0.20, 'cash': 0.15,
        'breadth_pct': 0.0, 'median_roc3m': 0.0, 'stocks_above_dma': 0, 'total_stocks': 0,
        'nav_current': None, 'nav_dma200': None, 'nav_series_len': 0,
        'signals': {'s1_equity_curve': 1, 's2_breadth': 0, 's3_momentum': 0},
    }


def get_next_rebalance_dates(n_weeks=8):
    """
    Returns next Friday (weekly check) and next month's 1st trading day (monthly RB).
    """
    import datetime
    today = datetime.date.today()
    days_to_friday = (4 - today.weekday()) % 7 or 7
    fridays = []
    nxt = today + datetime.timedelta(days=days_to_friday)
    for _ in range(n_weeks):
        fridays.append(nxt)
        nxt += datetime.timedelta(days=7)
    if today.month == 12:
        first_next = datetime.date(today.year + 1, 1, 1)
    else:
        first_next = datetime.date(today.year, today.month + 1, 1)
    while first_next.weekday() >= 5:
        first_next += datetime.timedelta(days=1)
    return {
        'next_friday'      : fridays[0],
        'upcoming_fridays' : fridays[:4],
        'next_monthly_rb'  : first_next,
    }


def get_weekly_deployment_plan(prev_score, curr_score, total_pf,
                                goldbees_curr=0, liquid_curr=0,
                                weekly_nav_ret=None, vix_curr=None):
    """
    Week-by-week deployment plan when regime changes.

    Recovery (score up)  → 3 weeks. Faster deploy to avoid missing rally.
    Defensive (score dn) → 4 weeks. Slower to avoid panic selling.

    Pause condition : weekly_nav_ret < -5% AND vix_curr > 30
    Accelerate rule : if score increases 2 consecutive Fridays → complete in 1 week

    Returns dict with weeks list, paused flag, accelerate_msg
    """
    _alloc = {
        3: (0.80, 0.15, 0.05), 2: (0.65, 0.20, 0.15),
        1: (0.45, 0.25, 0.30), 0: (0.25, 0.30, 0.45),
    }
    prev_alloc = _alloc.get(prev_score, (0.65, 0.20, 0.15))
    curr_alloc = _alloc.get(curr_score, (0.65, 0.20, 0.15))
    delta_e = curr_alloc[0] - prev_alloc[0]
    delta_g = curr_alloc[1] - prev_alloc[1]
    delta_c = curr_alloc[2] - prev_alloc[2]

    is_recovery  = delta_e > 0
    is_defensive = delta_e < 0
    n_weeks      = 3 if is_recovery else 4

    pause = bool(weekly_nav_ret is not None and weekly_nav_ret < -5.0
                 and vix_curr is not None and vix_curr > 30)

    weeks = []
    for wk in range(1, n_weeks + 1):
        frac     = wk / n_weeks
        target_e = round(prev_alloc[0] + delta_e * frac, 3)
        target_g = round(prev_alloc[1] + delta_g * frac, 3)
        target_c = round(prev_alloc[2] + delta_c * frac, 3)

        if pause and wk == 1:
            action = "⏸ HOLD — VIX>30 & weekly return<-5%. Next Friday dobara check karo."
        elif is_recovery and wk == 1:
            action = "🔺 Tranche 1 deploy — Liquid Fund se equity khareeedo."
        elif is_recovery and wk == n_weeks:
            action = "🔺 Final tranche — equity deployment complete. Target reached."
        elif is_recovery:
            action = f"🔺 Tranche {wk} — equity mein aur add karo."
        elif is_defensive and wk == 1:
            action = "🔻 Weakest ranked stocks exit karo → GOLDBEES + Liquid mein shift."
        elif is_defensive and wk == n_weeks:
            action = "🔻 Final defensive shift — target allocation complete."
        else:
            action = f"🔻 Defensive shift tranche {wk} — equity reduce, buffer badhaao."

        weeks.append({
            'week'   : wk,
            'eq_pct' : round(target_e * 100, 1),
            'gd_pct' : round(target_g * 100, 1),
            'cs_pct' : round(target_c * 100, 1),
            'eq_val' : round(total_pf * target_e),
            'gd_val' : round(total_pf * target_g),
            'cs_val' : round(total_pf * target_c),
            'action' : action,
            'paused' : pause and wk == 1,
        })

    accel_msg = (
        "⚡ Accelerate: Agar agle hafte bhi score badhta hai → remaining deployment ek hi hafte mein complete karo."
        if is_recovery else
        "🛡️ Hold rule: Agar agle hafte score wapas badhta hai → deployment pause karo, fresh score se reassess karo."
    )

    return {
        'prev_score': prev_score, 'curr_score': curr_score,
        'is_recovery': is_recovery, 'is_defensive': is_defensive,
        'n_weeks': n_weeks, 'weeks': weeks, 'paused': pause,
        'accelerate_msg': accel_msg,
        'vix_curr': vix_curr, 'weekly_nav_ret': weekly_nav_ret,
    }
