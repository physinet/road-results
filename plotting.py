import numpy as np
import os

import pandas as pd
from datetime import datetime

import altair as alt


def make_racer_plot_alt(racer_table):
    """Plot each racer's rating over time using altair"""

    df = pd.DataFrame.from_records(racer_table, exclude=['_sa_instance_state'])
    df['date'] = df['date'].dt.strftime('%m/%d/%y')
    df['Place'] = df['Place'].astype(str) + ' / ' + df['num_racers'].astype(str)

    # Add row for "zeroth" race - initial rating
    df.loc[len(df)] = 0  # row at the end
    df = df.shift().fillna('')  # shift to reveal NaN row at beginning
    df.loc[0, 'new_mu'] = df.loc[1, 'prior_mu']
    df.loc[0, 'new_sigma'] = df.loc[1, 'prior_sigma']
    df.loc[0, 'RaceName'] = 'Initial Rating'
    df.loc[0, 'Place'] = 'n/a'

    df['mu+sigma'] = df['new_mu'] + df['new_sigma']
    df['mu-sigma'] = df['new_mu'] - df['new_sigma']


    df = df.reset_index()  # Replace "index" column with index of df

    mu = alt.Chart(df).encode(
        x=alt.X('index', title='Race number',
                axis=alt.Axis(tickMinStep=1, grid=False)),
        y=alt.Y('new_mu', title='Rating', axis=alt.Axis(grid=False)),
        tooltip=['RaceName', 'Place']
    )

    mu = mu.mark_line(color='black') + mu.mark_point(color='black')

    sigma = alt.Chart(df).encode(
        x='index',
        y='mu+sigma',
        y2='mu-sigma'
    ).mark_area(opacity=0.3)

    chart = mu + sigma

    chart = chart.configure_axis(
        labelFontSize=16,
        titleFontSize=20
    ).properties(width=600, height=300, background="transparent")

    return chart.to_json()
