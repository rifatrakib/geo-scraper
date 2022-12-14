import os
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


def plotter(i, ax, scraper):
    date = datetime.utcnow().date().isoformat()
    file = f"data/meta/{scraper}-{date}.csv"

    df = pd.read_csv(
        file,
        usecols=columns_to_read,
        parse_dates=timestamps,
    )
    df["download_time"] = (df["response_received"] - df["request_sent"]).dt.microseconds // 1000
    df["download_time"] = df["download_time"].where(df["download_time"] >= 0, 0.01)
    df = df.set_index("request_sent")
    grouped_df = df["download_time"].resample("1S").mean(numeric_only=True)

    ax.cla()
    ax.plot(grouped_df, color="#007EA7")
    ax.set_xlabel("timeline", color="#003459")
    ax.set_ylabel("download time in ms", color="#003459")
    ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    ax.set_title(
        "Changes over time for Average Download Time",
        fontweight="bold",
        fontsize=16,
        color="#003459",
    )


def scraper_report(scraper):
    fig, ax = plt.subplots(figsize=(9, 6), constrained_layout=True)
    ani = FuncAnimation(
        fig,
        partial(plotter, ax=ax, scraper=scraper),
        interval=int(os.environ.get("REFRESH_DELAY")),
    )
    plt.show()

    location = f"reports/{scraper}"
    Path(location).mkdir(parents=True, exist_ok=True)
    date = datetime.utcnow().date().isoformat()
    ani.save(f"{location}/{scraper}-{date}.png", writer="imagemagick")
    fig.savefig(f"{location}/{scraper}-{date}.png")
