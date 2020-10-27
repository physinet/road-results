import numpy as np
import os

import pandas as pd
from datetime import datetime

import altair as alt


def make_racer_plot_alt(df_racer):
    """Plot each racer's rating over time using altair"""

    np.random.seed(42)

    df_racer['sigma'] = 10
    df_racer['sigma'] = df_racer['sigma'].apply(
        lambda x: x + np.random.randint(10, 50))
    df_racer['uppersigma'] = df_racer['Points'] + df_racer['sigma']
    df_racer['lowersigma'] = df_racer['Points'] - df_racer['sigma']

    mu = alt.Chart(df_racer.reset_index()).encode(
        x=alt.X('index', title='Race number',
                axis=alt.Axis(tickMinStep=1, grid=False)),
        y=alt.Y('Points', title='Rating', axis=alt.Axis(grid=False)),
        tooltip=['RaceName', 'Place']
    )

    mu = mu.mark_line(color='black') + mu.mark_point(color='black')

    sigma = alt.Chart(df_racer.reset_index()).encode(
        x='index',
        y='uppersigma',
        y2='lowersigma'
    ).mark_area(opacity=0.3)

    chart = sigma + mu

    chart = chart.configure_axis(
        labelFontSize=16,
        titleFontSize=20
    ).properties(width=600, height=300, background="transparent")

    return chart.to_json()
