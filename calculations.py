"""
core/calculations.py  —  SOP v2026.09
======================================
Pure pandas / numpy momentum metric functions.
Zero dependency on Streamlit.

v2026.09 Changes vs v2026.08:
  • Gold FIXED 20% all regimes (was 15-30% variable)
  • Equity FIXED 80% all regimes (was 25-80% variable)
  • Cash FIXED 0% — fully invested (except DD override: Eq60/Gold20/Liq20)
  • Regime signals now drive QFSM equity factor weights (not asset split)
  • TWO-STAGE EQUITY SELECTION:
      Stage 1: AvgZScore gate → Top STAGE1_TOP_N=100 momentum candidates
      Stage 2: QFSM composite score (F1-F5) on Top 100 → final Top 30
  • Risk parity position sizing (inverse volatility)
  • Threshold-based exit rule (not worst-rank-held)
  • Gold drift band ±7% (13%-27%) — rebalance only outside
  • dma50d added to build_dfStats (needed for F2 trend)
  • regime_band param added to build_dfStats → computes qfsmZScore
"""

import datetime as _dt
import math
import numpy as np
import pandas as pd


# ══════════════════════════════════════════════════════════════════════════════
# v2026.09 CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

GOLD_FIXED_PCT   = 0.20
EQUITY_FIXED_PCT = 0.80
CASH_FIXED_PCT   = 0.00

GOLD_TARGET     = 0.20
GOLD_DRIFT_BAND = 0.07
GOLD_FLOOR      = 0.13    # below this → TOP UP
GOLD_CEIL       = 0.27    # above this → TRIM

DD_OVERRIDE_THRESHOLD  = 20.0
DD_OVERRIDE_EQUITY_PCT = 0.60
DD_OVERRIDE_LIQUID_PCT = 0.20
DD_OVERRIDE_GOLD_PCT   = 0.20

# Equity factor weights (F1 mom, F2 trend, F3 meanrev, F4 size, F5 vol)
EQUITY_FACTOR_WEIGHTS = {
    3: dict(f1=0.55, f2=0.25, f3=0.05, f4=0.10, f5=0.05),
    2: dict(f1=0.45, f2=0.25, f3=0.10, f4=0.10, f5=0.10),
    1: dict(f1=0.35, f2=0.25, f3=0.20, f4=0.10, f5=0.10),
    0: dict(f1=0.25, f2=0.30, f3=0.30, f4=0.10, f5=0.05),
}

QFSM_RANK_WEIGHTS = {
    3: (0.20, 0.30, 0.50),
    2: (0.30, 0.40, 0.30),
    1: (0.40, 0.35, 0.25),
    0: (0.50, 0.35, 0.15),
}

STAGE1_TOP_N       = 100
PORTFOLIO_SIZE     = 30
RISK_PARITY_TARGET = 0.20
MAX_POSITION_PCT   = 0.08
MIN_POSITION_PCT   = 0.01
MIN_TXN_AMOUNT     = 15000

EXIT_THRESHOLD_BY_REGIME = {3: 0.25, 2: 0.30, 1: 0.35, 0: 0.40}


# ══════════════════════════════════════════════════════════════════════════════
# CORE STATISTICAL FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

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
    clean = data.replace([np.inf, -np.inf], np.nan)
    mean, std = clean.mean(), clean.std()
    if std == 0 or pd.isna(std):
        return pd.Series(0.0, index=data.index)
    return ((clean - mean) / std).round(2)


# ══════════════════════════════════════════════════════════════════════════════
# BUILD DFSTATS  (adds dma50d, vol, qfsmZScore vs v2026.08)
# ══════════════════════════════════════════════════════════════════════════════

def build_dfStats(close, high, volume, dates, ranking_method, regime_band=None):
    """
    Main metrics DataFrame.
    regime_band (0-3): If given, also computes qfsmZScore.
    New in v2026.09: dma50d column, vol column, qfsmZScore.
    """
    today = _dt.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    if dates['endDate'] > today:
        raise ValueError(
            f"Lookback Date '{dates['endDate'].strftime('%d-%m-%Y')}' future date hai!\n"
            f"Aaj ki date select karo: {today.strftime('%d-%m-%Y')}"
        )
    if close.empty:
        raise ValueError("Koi data download nahi hua. Internet check karo aur retry karo.")

    symbol    = list(close.columns)
    data20Y   = close.loc[:dates['endDate']].copy()
    volume20Y = volume.loc[:dates['endDate']].copy()
    high20Y   = high.loc[:dates['endDate']].copy()
    data12M   = data20Y.loc[dates['date12M']:].copy()
    data9M    = data20Y.loc[dates['date9M']:].copy()
    data6M    = data20Y.loc[dates['date6M']:].copy()
    data3M    = data20Y.loc[dates['date3M']:].copy()
    data1M    = data20Y.loc[dates['date1M']:].copy()
    volume12M = volume20Y.loc[dates['date12M']:].copy()

    for name, df in [('12M', data12M), ('9M', data9M), ('6M', data6M),
                     ('3M', data3M), ('1M', data1M)]:
        if df.empty or len(df) < 2:
            raise ValueError(
                f"data{name} empty hai!\nFuture date ya market holiday.\n"
                f"Aaj ki ya recent past trading date select karo."
            )

    dfStats = pd.DataFrame(index=symbol)
    dfStats['Close'] = round(data12M.iloc[-1], 2)

    # ── 200 DMA ───────────────────────────────────────────────────────────────
    dma_end   = dates['endDate']
    dma_start = dma_end - pd.DateOffset(days=290)
    data_dma  = data20Y.loc[dma_start:dma_end].ffill()
    dfStats['dma200d'] = (
        data_dma.rolling(window=200, min_periods=150).mean().iloc[-1].round(2).fillna(0)
    )

    # ── 50 DMA (NEW — for F2 trend factor) ───────────────────────────────────
    dma50_start = dma_end - pd.DateOffset(days=100)
    data_dma50  = data20Y.loc[dma50_start:dma_end].ffill()
    dfStats['dma50d'] = (
        data_dma50.rolling(window=50, min_periods=35).mean().iloc[-1].round(2).fillna(0)
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
    dfStats['vol']    = dfStats['vol12M']   # primary annualized vol for risk parity

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

    # Always ensure avgZScore12_6_3 exists (Stage 1 needs it)
    if 'avgZScore12_6_3' not in dfStats.columns:
        dfStats['avgZScore12_6_3'] = dfStats[['z_score12M','z_score6M','z_score3M']].mean(axis=1).round(2)

    # QFSM dynamic ranking (regime-adaptive)
    if regime_band is not None:
        w12, w6, w3 = QFSM_RANK_WEIGHTS.get(int(regime_band), QFSM_RANK_WEIGHTS[2])
        dfStats['qfsmZScore'] = (
            w12 * dfStats['z_score12M'] +
            w6  * dfStats['z_score6M']  +
            w3  * dfStats['z_score3M']
        ).round(4)

    dfStats['volm_cr']  = (getMedianVolume(volume12M) / 1e7).round(2)
    dfStats['ATH']      = round(high20Y.max(), 2)
    dfStats['AWAY_ATH'] = round((dfStats['Close'] / dfStats['ATH'] - 1) * 100, 2)

    # ── Circuit detection ─────────────────────────────────────────────────────
    _CIRCUIT_LEVELS = [5.0, 10.0, 20.0]
    _TOL = 0.015
    dataDaily_pct  = getDailyReturns(data12M) * 100
    dfStats['circuit'] = sum(
        ((dataDaily_pct - lvl).abs() < _TOL).sum() +
        ((dataDaily_pct + lvl).abs() < _TOL).sum()
        for lvl in _CIRCUIT_LEVELS
    )
    dataDaily_pct5 = getDailyReturns(data3M) * 100
    dfStats['circuit5'] = (
        ((dataDaily_pct5 - 5.0).abs() < _TOL).sum() +
        ((dataDaily_pct5 + 5.0).abs() < _TOL).sum()
    )

    dfStats = dfStats.reset_index().rename(columns={'index': 'Ticker'})
    dfStats['Ticker'] = dfStats['Ticker'].astype(str).str.replace('.NS', '', regex=False)

    for col in ['avgSharpe12_6_3','avg_All','avgSharpe9_6_3',
                'avgZScore12_6_3','avgZScore12_9_6_3','qfsmZScore']:
        if col in dfStats.columns:
            dfStats[col] = dfStats[col].replace([np.inf, -np.inf], np.nan).fillna(0)

    # Sort
    sort_map = {
        "avg_All":          ('avg_All',          'roc12M'),
        "sharpe12M":        ('sharpe12M',         'roc12M'),
        "avgSharpe12_6_3":  ('avgSharpe12_6_3',   'roc3M'),
        "sharpe3M":         ('sharpe3M',           'roc3M'),
        "avgSharpe9_6_3":   ('avgSharpe9_6_3',     'roc6M'),
        "avgZScore12_6_3":  ('avgZScore12_6_3',    'roc3M'),
        "qfsmZScore":       ('qfsmZScore',         'roc3M'),
        "avgZScore12_9_6_3":('avgZScore12_9_6_3',  'roc6M'),
    }
    sc, tc = sort_map.get(ranking_method, ('avgZScore12_6_3', 'roc3M'))
    if sc not in dfStats.columns:
        sc = 'avgZScore12_6_3'
    dfStats = dfStats.sort_values(by=[sc, tc], ascending=[False, False])
    dfStats['Rank'] = range(1, len(dfStats) + 1)
    dfStats = dfStats.set_index('Rank')
    return dfStats


# ══════════════════════════════════════════════════════════════════════════════
# APPLY FILTERS (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

def apply_filters(dfStats, filter_params: dict = None):
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
# v2026.09 — GOLD DRIFT
# ══════════════════════════════════════════════════════════════════════════════

def check_gold_drift(gold_current_value: float, total_portfolio_value: float) -> dict:
    """Check GOLDBEES drift vs fixed 20% target. Band: 13%-27%."""
    if total_portfolio_value <= 0:
        return {'status':'UNKNOWN','gold_pct':0,'action':'NONE','amount':0,
                'message':'Portfolio value zero.'}
    gp = gold_current_value / total_portfolio_value
    if gp > GOLD_CEIL:
        excess = gold_current_value - GOLD_TARGET * total_portfolio_value
        return {'status':'ABOVE_BAND','gold_pct':round(gp*100,1),'action':'TRIM',
                'amount':round(excess),
                'message':f'⚠️ Gold {gp*100:.1f}% > 27% — TRIM ₹{excess:,.0f} GOLDBEES'}
    elif gp < GOLD_FLOOR:
        deficit = GOLD_TARGET * total_portfolio_value - gold_current_value
        return {'status':'BELOW_BAND','gold_pct':round(gp*100,1),'action':'TOP_UP',
                'amount':round(deficit),
                'message':f'🔴 Gold {gp*100:.1f}% < 13% — TOP UP ₹{deficit:,.0f} GOLDBEES'}
    return {'status':'IN_BAND','gold_pct':round(gp*100,1),'action':'NONE','amount':0,
            'message':f'✅ Gold {gp*100:.1f}% — 13-27% band. No action.'}


# ══════════════════════════════════════════════════════════════════════════════
# v2026.09 — EQUITY FACTOR WEIGHTS
# ══════════════════════════════════════════════════════════════════════════════

def get_equity_factor_weights(regime_band: int) -> dict:
    """Factor weights for equity selection by regime (controls HOW equity is invested)."""
    band = max(0, min(3, int(regime_band)))
    return EQUITY_FACTOR_WEIGHTS.get(band, EQUITY_FACTOR_WEIGHTS[2])


# ══════════════════════════════════════════════════════════════════════════════
# v2026.09 — STAGE 1: MOMENTUM GATE
# ══════════════════════════════════════════════════════════════════════════════

def get_stage1_momentum_candidates(dfStats, top_n: int = STAGE1_TOP_N) -> pd.DataFrame:
    """Stage 1: extract top-n momentum stocks from filtered+ranked dfStats."""
    return dfStats[dfStats.index <= top_n].copy()


# ══════════════════════════════════════════════════════════════════════════════
# v2026.09 — STAGE 2: FACTOR SCORES
# ══════════════════════════════════════════════════════════════════════════════

def add_f1_to_stage2(df: pd.DataFrame) -> pd.DataFrame:
    """F1 Momentum: tanh-normalize avgZScore → [0,1]."""
    df = df.copy()
    z = df['avgZScore12_6_3'].fillna(0)
    df['f1_momentum'] = ((1 + np.tanh(z / 1.5)) / 2).clip(0, 1).round(4)
    return df


def add_f2_trend_score(df: pd.DataFrame) -> pd.DataFrame:
    """F2 Trend: T1(Close/200DMA) + T2(Close/50DMA) + T3(trend direction)."""
    df    = df.copy()
    close = df['Close'].fillna(0)
    d200  = df['dma200d'].replace(0, np.nan).fillna(close)
    d50   = df.get('dma50d', d200).replace(0, np.nan).fillna(d200) \
            if 'dma50d' in df.columns else d200

    r200 = (close / d200).clip(0.7, 1.3)
    t1   = ((r200 - 0.90) / 0.20).clip(0, 1)

    r50  = (close / d50).clip(0.7, 1.3)
    t2   = ((r50 - 0.90) / 0.20).clip(0, 1)

    # T3: roc3M as DMA-direction proxy
    roc3 = df.get('roc3M', pd.Series(0, index=df.index)).fillna(0)
    t3   = ((roc3 / 10) + 0.5).clip(0, 1)

    df['f2_trend']  = (0.40*t1 + 0.35*t2 + 0.25*t3).clip(0, 1).round(4)
    df['t1_200dma'] = t1.round(3)
    df['t2_50dma']  = t2.round(3)
    return df


def add_f3_mean_reversion_score(df: pd.DataFrame) -> pd.DataFrame:
    """F3 Mean Reversion: triangular peak at 30-35% below ATH + recovery bonus."""
    df       = df.copy()
    away_ath = df.get('AWAY_ATH', pd.Series(-10, index=df.index)).fillna(-10)
    dd       = np.clip(-away_ath, 0, 100)
    score    = np.where(dd < 20,  0.0,
               np.where(dd <= 35, (dd-20)/15.0,
               np.where(dd <= 60, 1.0-(dd-35)/25.0, 0.0)))
    roc1m    = df.get('roc1M', pd.Series(0, index=df.index)).fillna(0)
    recovery = np.where(roc1m > 2, 1.20, np.where(roc1m > 0, 1.10, 1.00))
    df['f3_mean_rev'] = np.clip(score * recovery, 0, 1).round(4)
    return df


def add_f4_size_score(df: pd.DataFrame) -> pd.DataFrame:
    """F4 Size: bell curve — midcap sweet spot (~₹30 Cr/day turnover)."""
    df     = df.copy()
    vol_cr = np.clip(df.get('volm_cr', pd.Series(5.0, index=df.index)).fillna(5.0), 0.5, 2000)
    log_v  = np.log10(vol_cr)
    df['f4_size'] = np.clip(np.exp(-0.5 * ((log_v - 1.5) / 1.2)**2), 0, 1).round(4)
    return df


def add_f5_vol_score(df: pd.DataFrame) -> pd.DataFrame:
    """F5 Volatility: bell curve — 25% annualized vol = peak."""
    df  = df.copy()
    vol = df.get('vol', df.get('vol12M', pd.Series(30.0, index=df.index))).fillna(30.0)
    if hasattr(vol, 'mean') and vol.mean() > 2:
        vol = vol / 100.0
    vol = vol.clip(0.05, 2.0)
    df['f5_vol_score'] = np.clip(np.exp(-0.5 * ((vol - 0.25) / 0.20)**2), 0, 1).round(4)
    return df


def compute_stage2_composite(stage1_df: pd.DataFrame, regime_band: int) -> pd.DataFrame:
    """
    Stage 2 master: adds F1-F5 scores + composite_score + composite_rank to stage1_df.
    """
    df = stage1_df.copy()
    df = add_f1_to_stage2(df)
    df = add_f2_trend_score(df)
    df = add_f3_mean_reversion_score(df)
    df = add_f4_size_score(df)
    df = add_f5_vol_score(df)

    w = get_equity_factor_weights(regime_band)
    df['composite_score'] = (
        w['f1'] * df['f1_momentum']  +
        w['f2'] * df['f2_trend']     +
        w['f3'] * df['f3_mean_rev']  +
        w['f4'] * df['f4_size']      +
        w['f5'] * df['f5_vol_score']
    ).clip(0, 1).round(4)

    df['composite_rank'] = df['composite_score'].rank(ascending=False, method='min').astype(int)
    return df


def select_final_portfolio(stage2_df: pd.DataFrame,
                            portfolio_size: int = PORTFOLIO_SIZE) -> pd.DataFrame:
    """Top portfolio_size stocks by composite_score."""
    return stage2_df.sort_values('composite_score', ascending=False).head(portfolio_size)


# ══════════════════════════════════════════════════════════════════════════════
# v2026.09 — RISK PARITY SIZING
# ══════════════════════════════════════════════════════════════════════════════

def get_risk_parity_weights(stage2_df: pd.DataFrame,
                             portfolio_symbols: list) -> dict:
    """Inverse-vol weights. Returns {ticker: weight} summing to 1.0."""
    ticker_col = 'Ticker' if 'Ticker' in stage2_df.columns else None
    if ticker_col:
        pf = stage2_df[stage2_df['Ticker'].isin(portfolio_symbols)].copy()
    else:
        pf = stage2_df[stage2_df.index.isin(portfolio_symbols)].copy()

    if len(pf) == 0:
        n = max(len(portfolio_symbols), 1)
        return {s: 1.0/n for s in portfolio_symbols}

    vc  = 'vol' if 'vol' in pf.columns else 'vol12M'
    vol = pf[vc].fillna(30.0)
    if hasattr(vol, 'mean') and vol.mean() > 2:
        vol = vol / 100.0
    vol     = vol.clip(0.08, 1.50)
    inv_vol = 1.0 / vol
    raw     = inv_vol / inv_vol.sum()
    capped  = raw.clip(MIN_POSITION_PCT, MAX_POSITION_PCT)
    final   = capped / capped.sum()

    keys = pf[ticker_col].tolist() if ticker_col else pf.index.tolist()
    return dict(zip(keys, final))


def get_position_values(weights: dict, equity_budget: float) -> dict:
    return {s: round(w * equity_budget) for s, w in weights.items()}


# ══════════════════════════════════════════════════════════════════════════════
# v2026.09 — EXIT RULE
# ══════════════════════════════════════════════════════════════════════════════

def get_exit_list(stage2_df: pd.DataFrame,
                  current_portfolio_tickers: list,
                  regime_band: int,
                  max_exits: int = None) -> list:
    """
    Threshold-based exit. Returns list of {ticker, reason, score}.
    Lazy: max 2-5 per rebalance.
    """
    threshold = EXIT_THRESHOLD_BY_REGIME.get(int(regime_band), 0.30)
    if max_exits is None:
        max_exits = max(2, len(current_portfolio_tickers) // 12)

    ticker_col = 'Ticker' if 'Ticker' in stage2_df.columns else None
    if ticker_col:
        lkp = stage2_df.set_index('Ticker') if stage2_df.index.name != 'Ticker' else stage2_df
        present = set(stage2_df['Ticker'].tolist())
    else:
        lkp     = stage2_df
        present = set(stage2_df.index.tolist())

    exits = []
    for ticker in current_portfolio_tickers:
        if ticker not in present:
            exits.append({'ticker': ticker, 'reason': 'Dropped from Top-100', 'score': 0.0})
            continue
        try:
            row   = lkp.loc[ticker]
            score = float(row.get('composite_score', 0.5) if hasattr(row,'get') else 0.5)
            f2    = float(row.get('f2_trend',        0.5) if hasattr(row,'get') else 0.5)
            f1    = float(row.get('f1_momentum',     0.5) if hasattr(row,'get') else 0.5)
        except Exception:
            score, f2, f1 = 0.5, 0.5, 0.5

        if score < threshold:
            exits.append({'ticker':ticker,
                          'reason':f'Score {score:.2f} < {threshold:.2f}','score':score})
        elif f2 < 0.15 and f1 < 0.25:
            exits.append({'ticker':ticker,
                          'reason':f'Trend {f2:.2f} + Mom {f1:.2f} broken','score':score})

    exits.sort(key=lambda x: x['score'])
    return exits[:max_exits]


def get_entry_list(stage2_final: pd.DataFrame,
                   current_portfolio_tickers: list,
                   n_entries: int) -> list:
    """Top composite_score stocks not already in portfolio."""
    held   = set(current_portfolio_tickers)
    tc     = 'Ticker' if 'Ticker' in stage2_final.columns else None
    ranked = stage2_final.sort_values('composite_score', ascending=False)
    out    = []
    for _, row in ranked.iterrows():
        t = row['Ticker'] if tc else str(row.name)
        if t not in held:
            out.append(t)
        if len(out) >= n_entries:
            break
    return out


# ══════════════════════════════════════════════════════════════════════════════
# v2026.09 — FACTOR ALLOCATION DISPLAY
# ══════════════════════════════════════════════════════════════════════════════

def get_factor_allocation_pct(stage2_df: pd.DataFrame,
                               portfolio_tickers: list,
                               regime_band: int) -> dict:
    """Effective factor % for current portfolio. Momentum ~50% = equilibrium signal."""
    w  = get_equity_factor_weights(regime_band)
    tc = 'Ticker' if 'Ticker' in stage2_df.columns else None
    pf = stage2_df[stage2_df['Ticker'].isin(portfolio_tickers)] if tc else \
         stage2_df[stage2_df.index.isin(portfolio_tickers)]
    if len(pf) == 0:
        return {k: round(v,4) for k,v in w.items()}

    eff = {
        'Momentum (F1)':   w['f1'] * float(pf['f1_momentum'].mean()  if 'f1_momentum'  in pf.columns else 0.5),
        'Trend (F2)':      w['f2'] * float(pf['f2_trend'].mean()     if 'f2_trend'     in pf.columns else 0.5),
        'Mean Rev (F3)':   w['f3'] * float(pf['f3_mean_rev'].mean()  if 'f3_mean_rev'  in pf.columns else 0.1),
        'Size (F4)':       w['f4'] * float(pf['f4_size'].mean()      if 'f4_size'      in pf.columns else 0.5),
        'Volatility (F5)': w['f5'] * float(pf['f5_vol_score'].mean() if 'f5_vol_score' in pf.columns else 0.5),
    }
    total = sum(eff.values()) or 1.0
    return {k: round(v/total, 4) for k,v in eff.items()}


# ══════════════════════════════════════════════════════════════════════════════
# v2026.09 — REGIME SCORE (FIXED 80/20/0)
# ══════════════════════════════════════════════════════════════════════════════

def get_regime_score(dfStats, equity_nav_series=None, portfolio_dd_pct=None):
    """
    3-signal regime score. v2026.09: equity/gold/cash FIXED.
    Regime band drives factor weights, not asset split.
    """
    if len(dfStats) == 0:
        return _regime_default()

    valid_dma   = dfStats[dfStats['dma200d'] > 0]
    above_dma   = (valid_dma['Close'] > valid_dma['dma200d']).sum()
    breadth_pct = round(above_dma / len(valid_dma) * 100, 1) if len(valid_dma) > 0 else 0.0
    s2 = 1 if breadth_pct > 50.0 else 0

    median_roc3m = round(float(dfStats['roc3M'].median()), 2)
    s3 = 1 if median_roc3m > 0.0 else 0

    nav_current = nav_dma200 = None
    nav_series_len = 0
    s1 = 1
    if equity_nav_series is not None and len(equity_nav_series) >= 5:
        nav_s = pd.Series(equity_nav_series).dropna()
        nav_series_len = len(nav_s)
        nav_current    = round(float(nav_s.iloc[-1]), 4)
        window         = min(200, nav_series_len)
        nav_dma200     = round(float(nav_s.rolling(window).mean().iloc[-1]), 4)
        s1 = 1 if nav_current > nav_dma200 else 0

    score       = s1 + s2 + s3
    regime_band = score
    band_labels = {3:'Strong Bull', 2:'Mild Bull', 1:'Neutral', 0:'Bear'}

    # v2026.09 FIXED allocation
    equity = EQUITY_FIXED_PCT
    gold   = GOLD_FIXED_PCT
    cash   = CASH_FIXED_PCT
    dd_override = False
    if portfolio_dd_pct is not None and portfolio_dd_pct >= DD_OVERRIDE_THRESHOLD:
        equity = DD_OVERRIDE_EQUITY_PCT
        cash   = DD_OVERRIDE_LIQUID_PCT
        gold   = DD_OVERRIDE_GOLD_PCT
        dd_override = True

    return {
        'score'           : score,
        'regime_band'     : regime_band,
        'label'           : band_labels[regime_band],
        'equity'          : equity,
        'gold'            : gold,
        'cash'            : cash,
        'factor_weights'  : get_equity_factor_weights(regime_band),
        'dd_override'     : dd_override,
        'breadth_pct'     : breadth_pct,
        'median_roc3m'    : median_roc3m,
        'stocks_above_dma': int(above_dma),
        'total_stocks'    : int(len(valid_dma)),
        'nav_current'     : nav_current,
        'nav_dma200'      : nav_dma200,
        'nav_series_len'  : nav_series_len,
        'signals'         : {'s1_equity_curve':s1, 's2_breadth':s2, 's3_momentum':s3},
    }


def _regime_default():
    return {
        'score':2, 'regime_band':2, 'label':'Mild Bull',
        'equity':EQUITY_FIXED_PCT, 'gold':GOLD_FIXED_PCT, 'cash':CASH_FIXED_PCT,
        'factor_weights': get_equity_factor_weights(2),
        'dd_override':False,
        'breadth_pct':0.0,'median_roc3m':0.0,
        'stocks_above_dma':0,'total_stocks':0,
        'nav_current':None,'nav_dma200':None,'nav_series_len':0,
        'signals':{'s1_equity_curve':1,'s2_breadth':0,'s3_momentum':0},
    }


# ══════════════════════════════════════════════════════════════════════════════
# DATE HELPERS (unchanged)
# ══════════════════════════════════════════════════════════════════════════════

def get_next_rebalance_dates(n_weeks=8):
    import datetime
    today = datetime.date.today()
    days_to_friday = (4 - today.weekday()) % 7 or 7
    fridays = []
    nxt = today + datetime.timedelta(days=days_to_friday)
    for _ in range(n_weeks):
        fridays.append(nxt)
        nxt += datetime.timedelta(days=7)
    if today.month == 12:
        first_next = datetime.date(today.year+1, 1, 1)
    else:
        first_next = datetime.date(today.year, today.month+1, 1)
    while first_next.weekday() >= 5:
        first_next += datetime.timedelta(days=1)
    return {'next_friday':fridays[0],'upcoming_fridays':fridays[:4],'next_monthly_rb':first_next}


# ══════════════════════════════════════════════════════════════════════════════
# v2026.09 — WEEKLY DEPLOYMENT PLAN (UPDATED)
# Asset allocation FIXED. Plan handles DD-override recovery & factor shifts.
# ══════════════════════════════════════════════════════════════════════════════

def get_weekly_deployment_plan(prev_score, curr_score, total_pf,
                                goldbees_curr=0, liquid_curr=0,
                                weekly_nav_ret=None, vix_curr=None):
    """
    v2026.09: equity always 80%, gold always 20%.
    Plan covers:
      (a) DD override recovery — liquid back to equity in tranches
      (b) Factor weight shifts — messaging for composite score rebalance
    """
    band_labels = {3:'Strong Bull', 2:'Mild Bull', 1:'Neutral', 0:'Bear'}
    fw_prev = get_equity_factor_weights(int(prev_score))
    fw_curr = get_equity_factor_weights(int(curr_score))

    is_dd_recovery  = liquid_curr > (total_pf * 0.05) if total_pf > 0 else False
    is_factor_shift = prev_score != curr_score
    pause = bool(weekly_nav_ret is not None and weekly_nav_ret < -5.0
                 and vix_curr   is not None and vix_curr   > 30)

    n_weeks = 2 if is_dd_recovery else (3 if is_factor_shift else 1)

    weeks = []
    for wk in range(1, n_weeks + 1):
        frac = wk / n_weeks
        if is_dd_recovery:
            liq_remain = liquid_curr * (1 - frac)
            eq_val = round(total_pf * EQUITY_FIXED_PCT - liq_remain)
            cs_val = round(liq_remain)
            gd_val = round(total_pf * GOLD_FIXED_PCT)
            action = (f"🔺 DD Recovery Wk{wk}: ₹{round(liquid_curr*frac):,} Liquid → Equity deploy karo."
                      if not pause else "⏸ HOLD — VIX>30 & weekly ret<-5%")
        else:
            eq_val = round(total_pf * EQUITY_FIXED_PCT)
            gd_val = round(total_pf * GOLD_FIXED_PCT)
            cs_val = 0
            m_old  = fw_prev['f1'] * 100
            m_new  = fw_curr['f1'] * 100
            action = (f"⚛ Factor shift: Momentum {m_old:.0f}%→{m_new:.0f}% "
                      f"({band_labels.get(prev_score,'?')}→{band_labels.get(curr_score,'?')}). "
                      "Monthly screener → Stage 2 composite → naye Top-30 select karo.")

        weeks.append({
            'week'   : wk,
            'eq_pct' : round(eq_val/total_pf*100, 1) if total_pf else 80.0,
            'gd_pct' : round(gd_val/total_pf*100, 1) if total_pf else 20.0,
            'cs_pct' : round(cs_val/total_pf*100, 1) if total_pf else 0.0,
            'eq_val' : eq_val,
            'gd_val' : gd_val,
            'cs_val' : cs_val,
            'action' : action,
            'paused' : pause and wk == 1,
        })

    if is_dd_recovery:
        accel = "🔺 DD Recovery: Liquid fund gradually equity mein deploy karo. Score stable ho tab accelerate karo."
    elif is_factor_shift:
        fw    = fw_curr
        accel = (f"⚛ Factor weights: Mom {fw['f1']*100:.0f}% | Trend {fw['f2']*100:.0f}% | "
                 f"MeanRev {fw['f3']*100:.0f}%. Monthly screener run se auto-implement hoga.")
    else:
        accel = "✅ No shift — normal monthly rebalance. Composite score se Top-30 update karo."

    return {
        'prev_score':prev_score, 'curr_score':curr_score,
        'is_dd_recovery':is_dd_recovery, 'is_factor_shift':is_factor_shift,
        'is_recovery':is_factor_shift and curr_score > prev_score,
        'is_defensive':is_factor_shift and curr_score < prev_score,
        'n_weeks':n_weeks, 'weeks':weeks, 'paused':pause,
        'accelerate_msg':accel,
        'vix_curr':vix_curr, 'weekly_nav_ret':weekly_nav_ret,
        'factor_weights_curr':fw_curr, 'factor_weights_prev':fw_prev,
    }
