import dill
import os

from flask import Flask, render_template, request, jsonify, redirect
from flask_wtf import FlaskForm
from wtforms import SelectField, validators
import pandas as pd


import commands
import database
from model import Model


from plotting import make_plot, make_racer_plot, race_map
race_map()

rows = [
    {'a': 1, 'b': 2, 'c': 3},
    {'a': 3, 'b': 5, 'c': -6}
] * 17

df = pd.DataFrame(rows)

# file = r'C:\data\results\races\9532.pkd'
file = os.path.join('data', 'results', '9532.pkd')
json = dill.load(open(file, 'rb'))
df_race = pd.read_json(json)
columns = [str(i) for i in range(28)]
df_race = df_race.drop(columns=columns)

df_race = df_race[df_race['RaceCategoryName'].str.strip() ==
                  'Men 45-49 Masters']


def get_placing(df):
    # sort_values().reset_index(drop=True).index + 1
    df['PredictedPlace'] = df['PriorPoints'].rank(method='max').astype(int)
    return df


df = df_race.groupby('RaceCategoryName').apply(get_placing)


race_names = ['test1', 'test2', 'test3', 'bucknell']
possible_names = {'0': 'hans', '1': 'sepp', '3': 'max'}

# My results
# file = r'C:\data\racers\177974.pkd'
file = os.path.join('data', 'racers', '177974.pkd')
json = dill.load(open(file, 'rb'))
df_brian = pd.read_json(json)
columns = [str(i) for i in range(28)] + ['OffTheFront', 'OffTheBack',
                                         'FieldSprintPlace', 'GroupSprintPlace',
                                         'RaceTypeID', 'MetaDataUrl']
df_brian = df_brian.drop(columns=columns).dropna(subset=['Points'])
df_brian['RaceDate'] = df_brian['RaceDate'].apply(
    lambda x: pd.to_datetime(x['date']))
# df_brian = df_brian.set_index('RaceDate')
df_brian['Place'] = df_brian.apply(
    lambda x: f"{x['Place']} / {x['RacerCount']}", axis=1)
df_brian['Points'] = 500 - df_brian['Points']

# Other's results
# file = r'C:\data\racers\7301.pkd'
file = os.path.join('data', 'racers', '7301.pkd')
json = dill.load(open(file, 'rb'))
df_other = pd.read_json(json)
columns = [str(i) for i in range(28)] + ['OffTheFront', 'OffTheBack',
                                         'FieldSprintPlace', 'GroupSprintPlace',
                                         'RaceTypeID', 'MetaDataUrl']
df_other = df_other.drop(columns=columns).dropna(subset=['Points'])
df_other['RaceDate'] = df_other['RaceDate'].apply(
    lambda x: pd.to_datetime(x['date']))
df_other['Place'] = df_other.apply(
    lambda x: f"{x['Place']} / {x['RacerCount']}", axis=1)
df_other['Points'] = 500 - df_other['Points']

# df_other = df_other.set_index('RaceDate')
# df_other = df_other[df_other['RaceDate'].between(pd.datetime(2015, 1, 1), pd.datetime(2016, 1, 1))]


class RaceForm(FlaskForm):
    name = SelectField('race_name',
                       # choices=race_names,
                       # [("", "")] is needed for a placeholder
                       choices=[("", "")] + [(uuid, name)
                                             for uuid, name in possible_names.items()],
                       validators=[validators.InputRequired()])


app = Flask(__name__)
app.config['SECRET_KEY'] = 'YOUR SECRET KEY'
app.config.from_object(os.environ['APP_SETTINGS'])

database.init_app(app)
commands.init_app(app)


@app.route('/')
def index():
    script, div = make_racer_plot(df_brian)
    return render_template('index.html', race_list=race_names,
                           form=RaceForm(),
                           race_name='Test race name',
                           df=df,
                           script=script,
                           div=div,
                           df_racer=df_brian)


@app.route('/', methods=['POST'])
def index_post():
    if 'racer_url' in request.form:
        racer_url = request.form['racer_url']
    else:
        racer_url = 'https://results.bikereg.com/racer/177974'
    script, div = make_racer_plot(df_other)
    return render_template('index.html', race_list=race_names,
                           form=RaceForm(),
                           scroll='results',
                           racer_url=racer_url,
                           df=df,
                           script=script,
                           div=div,
                           df_racer=df_other)


@app.route("/search/<string:box>")
def process(box):
    query = request.args.get('query')
    suggest_strs = [f'{i}race{i}' for i in range(100)]
    suggestions = [{'value': s} for s in suggest_strs if query in s]
    print(repr(query), suggestions)
    return jsonify({"suggestions": suggestions[:5]})


@app.route('/map')
def create_map():
    return render_template('map.html')


@app.route('/add')
def add_some_items():
    for racer_id, place in zip([53, 15, 62], [1, 2, 3]):
        row = Model(racer_id=racer_id, place=place)
        database.db.session.add(row)
        database.db.session.commit()

    queries = Model.query.all()

    return render_template('add.html', queries=queries)


@app.after_request
def add_header(r):
    """
    https://stackoverflow.com/questions/34066804/disabling-caching-in-flask
    Add headers to both force latest IE rendering engine or Chrome Frame,
    and also to cache the rendered page for 10 minutes.
    """
    r.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    r.headers["Expires"] = "0"
    r.headers['Cache-Control'] = 'public, max-age=0'
    return r


# if __name__ == '__main__':
#     app.run(port=8000, debug=True)
