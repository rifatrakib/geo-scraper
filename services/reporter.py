from datetime import datetime
from functools import partial
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.animation import FuncAnimation

columns_to_read = [
    "download_latency",
    "request_sent",
    "response_received",
    "yield_time",
]
timestamps = ["request_sent", "response_received", "yield_time"]


def plotter(i, ax, scraper, skip=0):
    date = datetime.utcnow().date().isoformat()
    file = f"data/meta/{scraper}-{date}.csv"

    while True:
        if Path(file).exists():
            break

    rows_to_skip = None
    if skip > 0:
        rows_to_skip = range(1, skip + 1)

    df = pd.read_csv(
        file,
        usecols=columns_to_read,
        parse_dates=timestamps,
        skiprows=rows_to_skip,
    )
    df["download_time"] = (df["request_sent"] - df["response_received"]).dt.microseconds // 1000
    df = df.set_index("request_sent")
    # row_count = df.shape[0]
    grouped_df = df["download_time"].resample("1S").mean(numeric_only=True)

    ax.cla()
    ax.plot(grouped_df)
    ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.set_title("Scraper Performance")


def scraper_report(scraper):
    fig, ax = plt.subplots(figsize=(6, 5), dpi=150)
    ani = FuncAnimation(fig, partial(plotter, ax=ax, scraper=scraper), interval=1000)
    print(f"{type(ani) = }")
    plt.tight_layout()
    plt.show()
