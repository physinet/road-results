import numpy as np

from bokeh.embed import components, file_html
from bokeh.plotting import figure
from bokeh.resources import CDN
from bokeh.models import DatetimeTickFormatter, DatetimeTicker

import folium
from folium.plugins import FastMarkerCluster

import pandas as pd
from datetime import datetime


def make_plot():
    fig = figure(title='Title',
                 plot_width=700,
                 plot_height=300,
                 tools=['ypan,ywheel_zoom,reset'],
                 active_scroll='ywheel_zoom')

    for i in range(10):
        fake_times = np.random.random(5)
        fake_ratings = np.random.random(5) * 10
        argsort = fake_times.argsort()
        fake_times = fake_times[argsort]
        fake_ratings = fake_ratings[argsort]
        fig.line(x=fake_times, y=fake_ratings,
                 line_color='#000000', line_width=3)

    fig.yaxis.axis_label = "Rating"

    # return file_html(fig, CDN)

    script, div = components(fig, CDN)

    return script, div


def make_racer_plot(df_racer):
    name = df_racer['FirstName'][0] + ' ' + df_racer['LastName'][0]
    fig = figure(title=f'Race history for {name}',
                 plot_width=700,
                 plot_height=300,
                 tools=['ypan,ywheel_zoom,reset'],
                 active_scroll='ywheel_zoom')

    data = df_racer['Points'].rolling(3).mean()
    fig.line(x=data.index, y=data,
             line_color='#000000', line_width=3)

    fig.yaxis.axis_label = "Rating"

    # fig.xaxis.formatter = DatetimeTickFormatter(days=['%b %d %Y'])
    # fig.xaxis.ticker = DatetimeTicker(desired_num_ticks=10)

    # return file_html(fig, CDN)

    script, div = components(fig, CDN)

    return script, div


def race_map():
    df = pd.read_pickle(os.path.join('data', 'df.pkl'))
    # df = pd.read_pickle('C:/data/results/df.pkl')
    df_coord = df.dropna(subset=['coord'])

    m = folium.Map(location=[39.8283, -98.5795], zoom_start=4)

    def add_marker(coord, name=None):
        folium.Marker([coord[0], coord[1]], popup=None).add_to(m)

    df_coord[df_coord['date'].dt.year == 2019].apply(
        lambda row: add_marker(row['coord'], row['name']), axis=1)

    m.save('templates/map.html')
