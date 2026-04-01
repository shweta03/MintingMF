"""
Market Breadth Data Pipeline
Runs daily at 9:30 AM IST via GitHub Actions
Outputs: breadth_data.json + vix_data.json → deployed to Netlify
"""

import json
import time
import requests
from datetime import datetime, timedelta
from pathlib import Path

# ── Install yfinance if needed ────────────────────────────────
try:
    import yfinance as yf
    import pandas as pd
    import numpy as np
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, '-m', 'pip', 'install',
                           'yfinance', 'pandas', 'numpy', '--quiet'])
    import yfinance as yf
    import pandas as pd
    import numpy as np

TODAY     = datetime.now().strftime('%Y-%m-%d')
LOOK_BACK = 365   # days of history to show in chart

# ── Nifty 750 tickers ────────────────────────────────────────
# Full Nifty 750 = Nifty 500 + Next 250. NSE tickers with .NS suffix.
# This list covers the major constituents. Full list from NSE India.
NIFTY750_TICKERS = [
    "RELIANCE.NS","TCS.NS","HDFCBANK.NS","ICICIBANK.NS","INFY.NS",
    "HINDUNILVR.NS","SBIN.NS","BHARTIARTL.NS","ITC.NS","KOTAKBANK.NS",
    "LT.NS","AXISBANK.NS","ASIANPAINT.NS","MARUTI.NS","SUNPHARMA.NS",
    "TITAN.NS","ULTRACEMCO.NS","WIPRO.NS","NESTLEIND.NS","HCLTECH.NS",
    "POWERGRID.NS","NTPC.NS","TECHM.NS","BAJFINANCE.NS","BAJAJFINSV.NS",
    "ONGC.NS","COALINDIA.NS","TATAMOTORS.NS","ADANIENT.NS","ADANIPORTS.NS",
    "JSWSTEEL.NS","TATASTEEL.NS","HINDALCO.NS","GRASIM.NS","CIPLA.NS",
    "DRREDDY.NS","DIVISLAB.NS","EICHERMOT.NS","BRITANNIA.NS","APOLLOHOSP.NS",
    "BPCL.NS","HEROMOTOCO.NS","SHRIRAMFIN.NS","TATACONSUM.NS","INDUSINDBK.NS",
    "SBILIFE.NS","BAJAJ-AUTO.NS","HDFCLIFE.NS","ICICIPRULI.NS","VEDL.NS",
    "PIDILITIND.NS","DABUR.NS","MARICO.NS","COLPAL.NS","GODREJCP.NS",
    "BERGEPAINT.NS","HAVELLS.NS","VOLTAS.NS","WHIRLPOOL.NS","BLUESTARCO.NS",
    "MUTHOOTFIN.NS","CHOLAFIN.NS","LICHSGFIN.NS","SBICARD.NS","MANAPPURAM.NS",
    "BANKBARODA.NS","PNB.NS","CANBK.NS","UNIONBANK.NS","IDFCFIRSTB.NS",
    "AUBANK.NS","RBLBANK.NS","FEDERALBNK.NS","KARURVYSYA.NS","SOUTHBANK.NS",
    "TATAPOWER.NS","ADANIGREEN.NS","TORNTPOWER.NS","CESC.NS","NHPC.NS",
    "SJVN.NS","RECLTD.NS","PFC.NS","IRFC.NS","HUDCO.NS",
    "DLF.NS","GODREJPROP.NS","PRESTIGE.NS","OBEROIRLTY.NS","PHOENIXLTD.NS",
    "ZOMATO.NS","NYKAA.NS","PAYTM.NS","POLICYBZR.NS","DELHIVERY.NS",
    "INDUSTOWER.NS","BHARTIHEXA.NS","TATACOMM.NS","MTNL.NS","RAILTEL.NS",
    "IRCTC.NS","RVNL.NS","IRCON.NS","RITES.NS","CONCOR.NS",
    "JUBLFOOD.NS","DEVYANI.NS","SAPPHIRE.NS","WESTLIFE.NS","BARBEQUE.NS",
    "PAGEIND.NS","APLAPOLLO.NS","RATNAMANI.NS","MAN.NS","HIKAL.NS",
    "PVRINOX.NS","INOXLEISUR.NS","NAZARA.NS","DELTACORP.NS","MHRIL.NS",
    "LALPATHLAB.NS","METROPOLIS.NS","THYROCARE.NS","KIMS.NS","RAINBOW.NS",
    "IPCALAB.NS","NATCOPHARM.NS","GLENMARK.NS","TORNTPHARM.NS","ALKEM.NS",
    "AUROPHARMA.NS","LUPIN.NS","BIOCON.NS","PFIZER.NS","ABBOTINDIA.NS",
    "MRF.NS","BALKRISIND.NS","APOLLOTYRE.NS","CEAT.NS","JKTYRE.NS",
    "ESCORTS.NS","SONACOMS.NS","MOTHERSON.NS","APTUS.NS","GRINDWELL.NS",
    "PIIND.NS","UPL.NS","COROMANDEL.NS","GNFC.NS","DEEPAKNITRITE.NS",
    "AAVAS.NS","HOMEFIRST.NS","CANFINHOME.NS","REPCO.NS","GRUH.NS",
    "ASTRAL.NS","SUPREMEIND.NS","ATUL.NS","FINPIPE.NS","HATSUN.NS",
    "TTKPRESTIG.NS","HAWKINCOOK.NS","RELAXO.NS","BATA.NS","KHADIM.NS",
    "VARUNBEV.NS","RADICO.NS","UNITDSPR.NS","GLOBUSSPR.NS","TILAKNAGAR.NS",
    "ZYDUSLIFE.NS","ERIS.NS","SOLARA.NS","LAURUSLABS.NS","GRANULES.NS",
    "CROMPTON.NS","ORIENTELEC.NS","BAJAJELEC.NS","VGUARD.NS","POLYCAB.NS",
    "KEI.NS","FINOLEX.NS","RCF.NS","GNFC.NS","CHAMBAL.NS",
    "TATAELXSI.NS","MPHASIS.NS","COFORGE.NS","PERSISTENT.NS","HEXAWARE.NS",
    "LTTS.NS","CYIENT.NS","KPITTECH.NS","BIRLASOFT.NS","MASTEK.NS",
    "ECLERX.NS","ZENSAR.NS","RAMSARUP.NS","NIIT.NS","APTECH.NS",
    "ZENSARTECH.NS","TANLA.NS","NEWGEN.NS","INTELLECT.NS","NUCLEUS.NS",
    "CAMS.NS","CDSL.NS","BSE.NS","MCX.NS","NSDL.NS",
    "ICICIGI.NS","NIACL.NS","STARHEALTH.NS","GODIGIT.NS","GICRE.NS",
    "INDIGRID.NS","STLTECH.NS","TATATECH.NS","KAYNES.NS","SYRMA.NS",
    "PRAJ.NS","THERMAX.NS","ELGIEQUIP.NS","GREAVESCOT.NS","KIRLOSENG.NS",
    "CUMMINSIND.NS","BHEL.NS","BEL.NS","HAL.NS","COCHINSHIP.NS",
    "MAZAGON.NS","GRSE.NS","BEML.NS","MIDHANI.NS","PARAS.NS",
    "ABFRL.NS","TRENT.NS","SHOPERSTOP.NS","VMART.NS","DMART.NS",
    "ZSWITCH.NS","AMBER.NS","DIXON.NS","VOLTAS.NS","BLUESTARCO.NS",
    "LXCHEM.NS","CLEAN.NS","GHCL.NS","VINATI.NS","NOCIL.NS",
]
# Deduplicate
NIFTY750_TICKERS = list(dict.fromkeys(NIFTY750_TICKERS))

def fetch_breadth():
    """Fetch 1 year daily closes for all tickers, compute 200 SMA, count above/below"""
    print(f"Fetching {len(NIFTY750_TICKERS)} stocks...")
    start = (datetime.now() - timedelta(days=LOOK_BACK + 250)).strftime('%Y-%m-%d')
    end   = datetime.now().strftime('%Y-%m-%d')

    # Download all at once in batches
    batch_size = 50
    all_data   = {}
    for i in range(0, len(NIFTY750_TICKERS), batch_size):
        batch = NIFTY750_TICKERS[i:i+batch_size]
        try:
            df = yf.download(batch, start=start, end=end,
                             auto_adjust=True, progress=False, threads=True)
            closes = df['Close'] if 'Close' in df.columns else df
            for t in batch:
                if t in closes.columns:
                    all_data[t] = closes[t].dropna()
        except Exception as e:
            print(f"  Batch {i//batch_size+1} error: {e}")
        time.sleep(0.5)

    print(f"  Got data for {len(all_data)} stocks")

    # For each trading day in the last LOOK_BACK days,
    # count how many stocks are above their 200-day SMA
    end_date   = datetime.now()
    start_date = end_date - timedelta(days=LOOK_BACK)

    # Get common dates
    sample_series = next(iter(all_data.values()))
    trading_days  = [d for d in sample_series.index if start_date <= d.to_pydatetime() <= end_date]

    dates  = []
    above_counts = []

    for day in trading_days:
        above = 0
        total = 0
        for ticker, series in all_data.items():
            try:
                # Get 200 SMA up to this day
                hist = series[series.index <= day]
                if len(hist) < 200:
                    continue
                sma200 = hist.iloc[-200:].mean()
                price  = hist.iloc[-1]
                total += 1
                if price > sma200:
                    above += 1
            except:
                continue
        if total > 100:  # only include days with sufficient data
            dates.append(day.strftime('%Y-%m-%d'))
            above_counts.append(above)

    print(f"  Computed breadth for {len(dates)} trading days")
    return {
        "dates":      dates,
        "above":      above_counts,
        "below":      [len(all_data)-a for a in above_counts],
        "total":      len(all_data),
        "generated":  TODAY
    }


def fetch_vix_ratio():
    """Fetch 3 years of Nifty50 + India VIX, compute ratio and 256-day rolling percentile"""
    print("Fetching Nifty 50 and India VIX...")
    start = (datetime.now() - timedelta(days=365*3 + 60)).strftime('%Y-%m-%d')
    end   = datetime.now().strftime('%Y-%m-%d')

    nifty = yf.download('^NSEI',     start=start, end=end, auto_adjust=True, progress=False)['Close']
    vix   = yf.download('^INDIAVIX', start=start, end=end, auto_adjust=True, progress=False)['Close']

    # Align
    combined = pd.DataFrame({'nifty': nifty, 'vix': vix}).dropna()
    combined['ratio'] = combined['nifty'] / combined['vix']

    # 256-day rolling percentile rank
    def rolling_pct_rank(series, window=256):
        ranks = []
        for i in range(len(series)):
            if i < window:
                ranks.append(None)
                continue
            window_vals = series.iloc[i-window:i]
            current     = series.iloc[i]
            rank = (window_vals < current).sum() / window * 100
            ranks.append(round(rank, 1))
        return ranks

    combined['pct_rank'] = rolling_pct_rank(combined['ratio'])
    combined = combined.dropna()

    # Only return last LOOK_BACK days for the chart
    cutoff = datetime.now() - timedelta(days=LOOK_BACK)
    combined = combined[combined.index >= cutoff]

    print(f"  VIX ratio computed for {len(combined)} trading days")
    return {
        "dates":      [d.strftime('%Y-%m-%d') for d in combined.index],
        "percentile": [float(v) for v in combined['pct_rank']],
        "nifty":      [float(v) for v in combined['nifty']],
        "ratio":      [round(float(v), 2) for v in combined['ratio']],
        "generated":  TODAY
    }


def upload_to_netlify(filename, data, site_id, token):
    """Upload a JSON file to Netlify"""
    data_str = json.dumps(data, indent=2)
    r = requests.put(
        f"https://api.netlify.com/api/v1/sites/{site_id}/files/{filename}",
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type": "application/octet-stream"},
        data=data_str.encode("utf-8"),
        timeout=30
    )
    return r.status_code in (200, 201)


def main():
    import os
    site_id = os.environ.get('NETLIFY_SITE_ID', '')
    token   = os.environ.get('NETLIFY_TOKEN', '')

    print(f"Market Breadth Pipeline — {TODAY}")
    print("=" * 50)

    # Fetch breadth data
    breadth = fetch_breadth()
    Path('breadth_data.json').write_text(json.dumps(breadth, indent=2))
    print(f"✅ breadth_data.json saved ({len(breadth['dates'])} days)")

    # Fetch VIX data
    vix = fetch_vix_ratio()
    Path('vix_data.json').write_text(json.dumps(vix, indent=2))
    print(f"✅ vix_data.json saved ({len(vix['dates'])} days)")

    # Upload to Netlify
    if site_id and token:
        print("\nUploading to Netlify...")
        if upload_to_netlify('breadth_data.json', breadth, site_id, token):
            print("✅ breadth_data.json uploaded")
        else:
            print("❌ breadth_data.json upload failed")
        if upload_to_netlify('vix_data.json', vix, site_id, token):
            print("✅ vix_data.json uploaded")
        else:
            print("❌ vix_data.json upload failed")
    else:
        print("⚠ NETLIFY_SITE_ID or NETLIFY_TOKEN not set — skipping upload")
        print("  Set these as GitHub repository secrets")

    print(f"\n{'='*50}")
    latest_above = breadth['above'][-1] if breadth['above'] else 0
    latest_pct   = vix['percentile'][-1] if vix['percentile'] else 50
    print(f"Breadth: {latest_above}/{breadth['total']} stocks above 200 SMA")
    print(f"VIX Percentile: {latest_pct:.0f}th")


if __name__ == "__main__":
    main()
