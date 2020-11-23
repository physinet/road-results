import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import altair as alt
import seaborn as sns

sns.set()

import evaluation

def make_corr_plot():
    """Makes a histogram of the Spearman correlation for each race."""
    corr = evaluation.correlation()
    fig, ax = plt.subplots()
    ax.hist(corr, bins=30)
    ax.set_xlim(-1, 1)
    ax.set_xlabel('Spearman correlation')
    ax.set_ylabel('Counts')
    fig.savefig('static/plots/correlation.png')

def make_hist():
    """Makes a histogram of all mean skill rating values."""
    mus = evaluation.get_mean_ratings()
    fig, ax = plt.subplots()
    ax.hist(mus, bins=30)
    ax.set_xlim(0, 45)
    ax.set_xlabel('Skill rating')
    ax.set_ylabel('Counts')
    fig.savefig('static/plots/ratings.png')

def make_racer_plot(racer_table, avg=25):
    """Plot each racer's rating over time using altair. Avg = average rating
    to plot as a dashed line.
    """

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
    df['avg'] = avg

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

    df_avg = pd.DataFrame({'x': [-100, 10000], 'y': [avg, avg]})

    avg_rating = alt.Chart(df_avg) \
                    .mark_line(strokeDash=[10, 10], color='#444444') \
                    .encode(x=alt.X('x', scale=scalex),
                            y=alt.Y('y', scale=scaley))

    chart = avg_rating + sigma + mu

    chart = chart.configure_axis(
                    labelFontSize=16,
                    titleFontSize=20) \
                 .properties(width=600, height=300, background="transparent") \
                 .interactive(bind_y=False)

    return chart.to_json()
