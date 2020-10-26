import numpy as np
import os

import pandas as pd
from datetime import datetime

import altair as alt


def make_racer_plot_alt(df_racer):
    """Plot each racer's rating over time using altair"""
    chart = alt.Chart(df_racer.reset_index()).encode(
        x='index',
        y='Points',
        tooltip=['index', 'Points']
    )

    chart = chart.mark_line() + chart.mark_point()

    return chart.to_json()
