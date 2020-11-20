import numpy as np
import os

import pandas as pd
from datetime import datetime

import altair as alt


def make_racer_plot_alt(racer_table):
    """Plot each racer's rating over time using altair"""

    df = pd.DataFrame.from_records(racer_table, exclude=['_sa_instance_state'])
    df['date'] = df['date'].dt.strftime('%B %d, %Y')  #.strftime('%m/%d/%y')

    dnf = df['Place'].isna()
    df.loc[~dnf, 'place_string'] = \
        df.loc[~dnf, 'Place'].astype(int).astype(str) \
        + ' / ' + df.loc[~dnf, 'num_racers'].astype(str)
    df.loc[dnf, 'place_string'] = 'DNF'

    # Add row for "zeroth" race - initial rating
    df.loc[len(df)] = 0  # row at the end
    df = df.shift().fillna('')  # shift to reveal NaN row at beginning
    df.loc[0, 'mu'] = df.loc[1, 'prior_mu']
    df.loc[0, 'sigma'] = df.loc[1, 'prior_sigma']
    df.loc[0, 'RaceName'] = 'Initial Rating'
    df.loc[0, 'Place'] = 'n/a'
    df.loc[0, 'place_string'] = 'n/a'

    df['mu+sigma'] = df['mu'] + df['sigma']
    df['mu-sigma'] = df['mu'] - df['sigma']

    df = df.rename(columns={'RaceName': 'Race Name', 'date': 'Date'})
    df = df.reset_index()  # Replace "index" column with index of df

    scalex = alt.Scale(domain=[df.index.min(), df.index.max()],
                      nice=False, zero=True)

    scaley = alt.Scale(zero=False)

    mu = alt.Chart(df).encode(
        x=alt.X('index', title='Race number',
                scale=scalex,
                axis=alt.Axis(tickMinStep=1, grid=False)),
        y=alt.Y('mu', title='Rating', scale=scaley, axis=alt.Axis(grid=False)),
        tooltip=['Race Name', 'Date', 'place_string']
    )

    mu = mu.mark_line(color='black') + mu.mark_point(color='black')

    sigma = alt.Chart(df).encode(
        x=alt.X('index', scale=scalex),
        y=alt.Y('mu+sigma', scale=scaley),
        y2='mu-sigma'
    ).mark_area(opacity=0.3)

    chart = sigma + mu

    chart = chart.configure_axis(
        labelFontSize=16,
        titleFontSize=20
    ).properties(width=600, height=300, background="transparent"
    ).interactive(bind_y=False
    )

    return chart.to_json()
