import numpy as np
import os

import pandas as pd
from datetime import datetime

import altair as alt


def make_racer_plot_alt(df_racer):
    """Plot each racer's rating over time using altair"""
    chart = alt.Chart(df_racer.reset_index()).mark_line().encode(
        x="index",
        y="Points"
    )

    return chart.to_json()
