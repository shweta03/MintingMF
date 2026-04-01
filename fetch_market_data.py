"""
Market Breadth Data Pipeline
Runs daily at 9:30 AM IST via GitHub Actions
Outputs: breadth_data.json + vix_data.json → deployed to Netlify
"""

import json, time, requests
from datetime import datetime, timedelta
from pathlib import Path

try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable,'-m','pip','install','yfinance','pandas','numpy','--quiet'])
    import yfinance as yf
    import pandas as pd
    import numpy as np

TODAY     = datetime.now().strftime('%Y-%m-%d')
LOOK_BACK = 365

# ── Verified Nifty 750 tickers (working on Yahoo Finance) ────────
NIFTY750_TICKERS = [
    "RELIANCE.NS","TCS.NS","HDFCBANK.NS","ICICIBANK.NS","INFY.NS",
    "HINDUNILVR.NS","SBIN.NS","BHARTIARTL.NS","ITC.NS","KOTAKBANK.NS",
    "LT.NS","AXISBANK.NS","ASIANPAINT.NS","MARUTI.NS","SUNPHARMA.NS",
    "TITAN.NS","ULTRACEMCO.NS","WIPRO.NS","NESTLEIND.NS","HCLTECH.NS",
    "POWERGRID.NS","NTPC.NS","TECHM.NS","BAJFINANCE.NS","BAJAJFINSV.NS",
    "ONGC.NS","COALINDIA.NS","ADANIENT.NS","ADANIPORTS.NS","JSWSTEEL.NS",
    "TATASTEEL.NS","HINDALCO.NS","GRASIM.NS","CIPLA.NS","DRREDDY.NS",
    "DIVISLAB.NS","EICHERMOT.NS","BRITANNIA.NS","APOLLOHOSP.NS","BPCL.NS",
    "HEROMOTOCO.NS","SHRIRAMFIN.NS","TATACONSUM.NS","INDUSINDBK.NS",
    "SBILIFE.NS","BAJAJ-AUTO.NS","HDFCLIFE.NS","ICICIPRULI.NS","VEDL.NS",
    "PIDILITIND.NS","DABUR.NS","MARICO.NS","COLPAL.NS","GODREJCP.NS",
    "BERGEPAINT.NS","HAVELLS.NS","VOLTAS.NS","MUTHOOTFIN.NS","CHOLAFIN.NS",
    "LICHSGFIN.NS","SBICARD.NS","MANAPPURAM.NS","BANKBARODA.NS","PNB.NS",
    "CANBK.NS","UNIONBANK.NS","IDFCFIRSTB.NS","AUBANK.NS","RBLBANK.NS",
    "FEDERALBNK.NS","KARURVYSYA.NS","TATAPOWER.NS","ADANIGREEN.NS",
    "TORNTPOWER.NS","CESC.NS","NHPC.NS","SJVN.NS","RECLTD.NS","PFC.NS",
    "IRFC.NS","HUDCO.NS","DLF.NS","GODREJPROP.NS","PRESTIGE.NS",
    "OBEROIRLTY.NS","PHOENIXLTD.NS","ZOMATO.NS","NYKAA.NS","DELHIVERY.NS",
    "INDUSTOWER.NS","TATACOMM.NS","IRCTC.NS","RVNL.NS","IRCON.NS",
    "RITES.NS","CONCOR.NS","JUBLFOOD.NS","DEVYANI.NS","WESTLIFE.NS",
    "PAGEIND.NS","APLAPOLLO.NS","RATNAMANI.NS","PVRINOX.NS","NAZARA.NS",
    "LALPATHLAB.NS","METROPOLIS.NS","IPCALAB.NS","NATCOPHARM.NS",
    "GLENMARK.NS","TORNTPHARM.NS","ALKEM.NS","AUROPHARMA.NS","LUPIN.NS",
    "BIOCON.NS","PFIZER.NS","ABBOTINDIA.NS","MRF.NS","BALKRISIND.NS",
    "APOLLOTYRE.NS","JKTYRE.NS","ESCORTS.NS","SONACOMS.NS","MOTHERSON.NS",
    "PIIND.NS","UPL.NS","COROMANDEL.NS","GNFC.NS","AAVAS.NS",
    "HOMEFIRST.NS","CANFINHOME.NS","ASTRAL.NS","SUPREMEIND.NS","ATUL.NS",
    "TRENT.NS","SHOPERSTOP.NS","VMART.NS","DMART.NS","AMBER.NS",
    "DIXON.NS","TATAELXSI.NS","MPHASIS.NS","COFORGE.NS","PERSISTENT.NS",
    "LTTS.NS","CYIENT.NS","KPITTECH.NS","MASTEK.NS","ECLERX.NS",
    "TANLA.NS","NEWGEN.NS","INTELLECT.NS","NUCLEUS.NS","CAMS.NS",
    "CDSL.NS","BSE.NS","MCX.NS","ICICIGI.NS","NIACL.NS","STARHEALTH.NS",
    "INDIGRID.NS","TATATECH.NS","KAYNES.NS","SYRMA.NS","THERMAX.NS",
    "ELGIEQUIP.NS","KIRLOSENG.NS","CUMMINSIND.NS","BHEL.NS","BEL.NS",
    "HAL.NS","COCHINSHIP.NS","GRSE.NS","BEML.NS","ABFRL.NS",
    "CLEAN.NS","GHCL.NS","NOCIL.NS","ZYDUSLIFE.NS","ERIS.NS",
    "SOLARA.NS","LAURUSLABS.NS","GRANULES.NS","CROMPTON.NS","POLYCAB.NS",
    "KEI.NS","BAJAJHLDNG.NS","SOLARINDS.NS","DEEPAKNTR.NS","FINCABLES.NS",
    "RAILTEL.NS","PATANJALI.NS","NUVAMA.NS","360ONE.NS","ANGELONE.NS",
    "MFSL.NS","CHOLAHLDNG.NS","PNBHOUSING.NS","APTUS.NS","CREDITACC.NS",
    "UJJIVANSFB.NS","EQUITASBNK.NS","SURYODAY.NS","ESAFSFB.NS",
    "HFCL.NS","STLTECH.NS","OPTIEMUS.NS","ROUTE.NS","GTLINFRA.NS",
    "ZENSARTECH.NS","MASTECH.NS","FIRSTSOURCE.NS","MPHL.NS","DATAMATICS.NS",
    "AFFLE.NS","HAPPSTMNDS.NS","LATENTVIEW.NS","NETWEB.NS","RATEGAIN.NS",
    "DELHIVERY.NS","MAPMYINDIA.NS","BIKAJI.NS","CAMPUS.NS","ETHOS.NS",
    "SENCO.NS","DOMS.NS","KAYNES.NS","WAAREEENER.NS","PREMIER.NS",
    "JBMA.NS","SANSERA.NS","CRAFTSMAN.NS","GALAXYSURF.NS","FLAIR.NS",
    "SIGNATURE.NS","NUVOCO.NS","JKCEMENT.NS","RAMCOCEM.NS","HEIDELBERG.NS",
    "BIRLACORPN.NS","DALMIA.NS","SHREECEM.NS","ACC.NS","AMBUJACEMENT.NS",
    "ORIENTCEM.NS","PRISMJOHNSN.NS","JKLAKSHMI.NS","STARCEMENT.NS",
    "SHYAMMETL.NS","JSPL.NS","SAIL.NS","NMDC.NS","MOIL.NS","GMDC.NS",
    "NATIONALUM.NS","WELCORP.NS","RATNAMANI.NS","MAHINDCIE.NS",
    "TDPOWERSYS.NS","GRINDWELL.NS","TIMKEN.NS","SKF.NS","SCHAEFFLER.NS",
    "FINEORG.NS","SUDARSCHEM.NS","NAVINFLUOR.NS","FLUOROCHEM.NS",
]
NIFTY750_TICKERS = list(dict.fromkeys(NIFTY750_TICKERS))


def fetch_breadth():
    print(f"Fetching {len(NIFTY750_TICKERS)} stocks for breadth...")
    start = (datetime.now() - timedelta(days=LOOK_BACK + 250)).strftime('%Y-%m-%d')
    end   = datetime.now().strftime('%Y-%m-%d')

    batch_size = 50
    all_data   = {}

    for i in range(0, len(NIFTY750_TICKERS), batch_size):
        batch = NIFTY750_TICKERS[i:i+batch_size]
        try:
            df = yf.download(batch, start=start, end=end,
                             auto_adjust=True, progress=False, threads=True)
            # Handle multi-level columns from yfinance
            if isinstance(df.columns, pd.MultiIndex):
                closes = df['Close']
            else:
                closes = df[['Close']] if 'Close' in df.columns else df

            for t in batch:
                try:
                    if t in closes.columns:
                        s = closes[t].dropna()
                        if len(s) > 50:
                            all_data[t] = s
                except:
                    pass
        except Exception as e:
            print(f"  Batch {i//batch_size+1} error: {e}")
        time.sleep(1)

    print(f"  Got data for {len(all_data)} stocks")

    # For each trading day compute breadth
    end_dt   = datetime.now()
    start_dt = end_dt - timedelta(days=LOOK_BACK)

    # Get trading days from any valid series
    sample = next(iter(all_data.values()))
    trading_days = [d for d in sample.index
                    if start_dt <= d.to_pydatetime().replace(tzinfo=None) <= end_dt]

    dates, above_counts = [], []

    for day in trading_days:
        above = 0; total = 0
        for ticker, series in all_data.items():
            try:
                hist = series[series.index <= day]
                if len(hist) < 200:
                    continue
                sma200 = hist.iloc[-200:].mean()
                price  = float(hist.iloc[-1])
                total += 1
                if price > sma200:
                    above += 1
            except:
                continue
        if total > 50:
            dates.append(day.strftime('%Y-%m-%d'))
            above_counts.append(above)

    print(f"  Breadth computed for {len(dates)} trading days")
    return {
        "dates":     dates,
        "above":     above_counts,
        "below":     [len(all_data)-a for a in above_counts],
        "total":     len(all_data),
        "generated": TODAY
    }


def fetch_vix_ratio():
    print("Fetching Nifty 50 and India VIX...")
    start = (datetime.now() - timedelta(days=365*3 + 60)).strftime('%Y-%m-%d')
    end   = datetime.now().strftime('%Y-%m-%d')

    # Download with group_by to avoid MultiIndex issues
    nifty_raw = yf.download('^NSEI',     start=start, end=end,
                            auto_adjust=True, progress=False)
    vix_raw   = yf.download('^INDIAVIX', start=start, end=end,
                            auto_adjust=True, progress=False)

    # Extract Close — handle both flat and MultiIndex
    def extract_close(df, symbol):
        if isinstance(df.columns, pd.MultiIndex):
            return df['Close'][symbol].dropna()
        elif 'Close' in df.columns:
            return df['Close'].dropna()
        else:
            return df.iloc[:, 0].dropna()

    nifty = extract_close(nifty_raw, '^NSEI')
    vix   = extract_close(vix_raw,   '^INDIAVIX')

    if not isinstance(nifty, pd.Series) or len(nifty) < 10:
        raise ValueError(f"Nifty data invalid: got {type(nifty)}, len={len(nifty) if hasattr(nifty,'__len__') else 'N/A'}")
    if not isinstance(vix, pd.Series) or len(vix) < 10:
        raise ValueError(f"VIX data invalid: got {type(vix)}, len={len(vix) if hasattr(vix,'__len__') else 'N/A'}")

    # Align on common dates
    combined = pd.DataFrame({'nifty': nifty, 'vix': vix}).dropna()
    if len(combined) < 50:
        raise ValueError(f"Combined data too short: {len(combined)} rows")

    combined['ratio'] = combined['nifty'] / combined['vix']

    # 256-day rolling percentile rank
    pct_ranks = []
    ratio_vals = combined['ratio'].values
    for i in range(len(ratio_vals)):
        if i < 256:
            pct_ranks.append(None)
            continue
        window  = ratio_vals[i-256:i]
        current = ratio_vals[i]
        rank    = float((window < current).sum() / 256 * 100)
        pct_ranks.append(round(rank, 1))

    combined['pct_rank'] = pct_ranks
    combined = combined.dropna(subset=['pct_rank'])

    # Last LOOK_BACK days only
    cutoff  = datetime.now() - timedelta(days=LOOK_BACK)
    combined = combined[combined.index.tz_localize(None) >= cutoff] if combined.index.tz else combined[combined.index >= cutoff]

    print(f"  VIX ratio computed for {len(combined)} trading days")
    return {
        "dates":      [d.strftime('%Y-%m-%d') for d in combined.index],
        "percentile": [float(v) for v in combined['pct_rank']],
        "nifty":      [float(v) for v in combined['nifty']],
        "ratio":      [round(float(v), 2) for v in combined['ratio']],
        "generated":  TODAY
    }


def upload_to_netlify(filename, data, site_id, token):
    data_str = json.dumps(data, indent=2)
    r = requests.put(
        f"https://api.netlify.com/api/v1/sites/{site_id}/files/{filename}",
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/octet-stream"},
        data=data_str.encode("utf-8"), timeout=30
    )
    return r.status_code in (200, 201)


def main():
    import os
    site_id = os.environ.get('NETLIFY_SITE_ID', '')
    token   = os.environ.get('NETLIFY_TOKEN', '')

    print(f"Market Breadth Pipeline — {TODAY}")
    print("=" * 50)

    # Breadth
    try:
        breadth = fetch_breadth()
        Path('breadth_data.json').write_text(json.dumps(breadth, indent=2))
        print(f"✅ breadth_data.json — {len(breadth['dates'])} days, latest above: {breadth['above'][-1]}/{breadth['total']}")
    except Exception as e:
        print(f"❌ Breadth failed: {e}")
        breadth = None

    # VIX
    try:
        vix = fetch_vix_ratio()
        Path('vix_data.json').write_text(json.dumps(vix, indent=2))
        print(f"✅ vix_data.json — {len(vix['dates'])} days, latest pct: {vix['percentile'][-1]:.1f}th")
    except Exception as e:
        print(f"❌ VIX failed: {e}")
        vix = None

    # Upload
    if site_id and token:
        print("\nUploading to Netlify...")
        if breadth and upload_to_netlify('breadth_data.json', breadth, site_id, token):
            print("✅ breadth_data.json uploaded")
        if vix and upload_to_netlify('vix_data.json', vix, site_id, token):
            print("✅ vix_data.json uploaded")
    else:
        print("⚠ Set NETLIFY_SITE_ID and NETLIFY_TOKEN as GitHub secrets")

    print("\nDone.")


if __name__ == "__main__":
    main()
