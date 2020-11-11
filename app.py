import dill
import os
import glob

from flask import Flask, render_template, request, jsonify, redirect
from flask_wtf import FlaskForm
from wtforms import SelectField, SubmitField, validators, StringField
import pandas as pd


import commands
import database
import model
from model import Results, Races, Racers

from preprocess import clean

from plotting import make_racer_plot_alt

# constants to keep track of which race/racer info to show on the main page
RACE_ID = 10000 #11557
CATEGORY_NAME = 'Men Collegiate CAT A'
RACER_ID = 177974
SCROLL = ''


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
df_brian['Points'] = 550 - df_brian['Points']

class RaceForm(FlaskForm):
    name_date = StringField('name_date', id='name_date')
    submit = SubmitField('Show me this race!', id='race_name_submit')

class CategoryForm(FlaskForm):
    category = SelectField('Category', id='category')
    submit = SubmitField('Show me this category!', id='category_submit')

    def __init__(self, categories, *args, **kwargs):
        super(CategoryForm, self).__init__(*args, **kwargs)
        self.category.choices = categories


app = Flask(__name__)
app.config['SECRET_KEY'] = 'YOUR SECRET KEY'
app.config.from_object(os.environ['APP_SETTINGS'])

database.init_app(app)
commands.init_app(app)


@app.route('/', methods=['GET', 'POST'])
def index_post():
    global RACE_ID, CATEGORY_NAME, RACER_ID, SCROLL

    # Get data from the form
    if 'name_date' in request.form:
        RACE_ID = Races.get_race_id(request.form['name_date'])
    elif 'category' in request.form:
        CATEGORY_NAME = request.form['category']
    elif 'racer_url' in request.form:
        RACER_ID = int(request.form['racer_url'])

    if 'name_date' in request.form or 'category' in request.form:
        SCROLL='race'
    elif 'racer_url' in request.form:
        SCROLL='racer'

    racer_url = f'https://results.bikereg.com/racer/{RACER_ID}'

    categories = Races.get_categories(RACE_ID)
    if 'category' not in request.form:
        CATEGORY_NAME = categories[0]
    race_table = Results.get_race_table(RACE_ID, CATEGORY_NAME)

    race_form = RaceForm()
    category_form = CategoryForm(categories)

    chart = make_racer_plot_alt(df_brian)

    return render_template('index.html',
                           race_form=race_form,
                           category_form=category_form,
                           scroll=SCROLL,
                           racer_url=racer_url,
                           df_racer=df_brian,
                           chart=chart,
                           race_table=race_table)


@app.route("/search/<string:box>")
def race_suggestions(box):
    """Create search suggestions when searching races"""
    race_names = Races.get_race_names()
    query = request.args.get('query').lower()
    suggestions = [{'value': name} for name in race_names
                                   if query in name.lower()]
    return jsonify({"suggestions": suggestions[:5]})


@app.route('/map')
def create_map():
    return render_template('map.html')


@app.route('/database')
def preview_database(methods=['GET', 'POST']):
    if request.args.get('drop'):
        commands.db_drop_all()
        commands.db_create_all()

    if request.args.get('add'):
        if Results.query.count() > 0:
            raise Exception('Rows exist in Results table. Can\'t add!')
        df = pd.read_pickle('C:/data/results/df.pkl')
        model.Races.add_from_df(df)
        model.add_table_results()

    if request.args.get('rate'):
        model.get_all_ratings()

    if request.args.get('table'):
        Table = eval(request.args.get('table'))
    else:
        Table = Results
    queries = Table.query.order_by(Table.index)

    if Table == Races:
        queries = queries.filter(Table.index > 10000) # for troubleshooting

    queries = queries.limit(2000)

    cols = Table.__table__.columns.keys()

    return render_template('database.html', cols=cols,
                           data=[q.__dict__ for q in queries])


@app.route('/race')
def display_single_race(methods=['GET', 'POST']):
    race_id = request.args.get('id')
    if not race_id:
        race_id = 10000
    race_id = int(race_id)

    category_idx = request.args.get('cat')  # an index for which category
    if not category_idx:
        category_idx = 0
    category_idx = int(category_idx)

    categories = Races.get_categories(race_id)
    if category_idx >= len(categories):
        category_idx = 0

    race_table = Results.get_race_table(race_id, categories[category_idx])

    cols = Results.__table__.columns.keys()

    return render_template('database.html', cols=cols,
                            data=[q.__dict__ for q in race_table])

@app.route('/racer')
def display_single_racer(methods=['GET', 'POST']):
    if 'id' in request.args:
        racer_id = int(request.args.get('id'))
    else:
        racer_id = RACER_ID

    racer_table = Results.get_racer_results(racer_id)

    cols = Results.__table__.columns.keys()

    return render_template('database.html', cols=cols,
                            data=[q.__dict__ for q in racer_table])

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
