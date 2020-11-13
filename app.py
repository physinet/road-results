import dill
import os
import glob

from flask import Flask, render_template, request, jsonify, redirect
from flask_wtf import FlaskForm
from flask_wtf.csrf import CSRFProtect
from wtforms import SelectField, SubmitField, StringField, validators
from wtforms.validators import AnyOf
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
RACER_ID = 9915  #177974
SCROLL = ''


class RaceForm(FlaskForm):
    name_date = StringField('name_date', id='name_date')
    submit = SubmitField('Show me this race!', id='race_name_submit')

    def __init__(self, race_names, *args, **kwargs):
        super(RaceForm, self).__init__(*args, **kwargs)
        self.name_date.validators = [AnyOf(race_names)]

class CategoryForm(FlaskForm):
    category = SelectField('Category', id='category')
    submit = SubmitField('Show me this category!', id='category_submit')

    def __init__(self, categories, *args, **kwargs):
        super(CategoryForm, self).__init__(*args, **kwargs)
        self.category.choices = categories

class RacerForm(FlaskForm):
    racer_name = StringField('racer_name', id='racer_name')
    submit = SubmitField('Show me this racer!', id='racer_name_submit')

    def __init__(self, racer_names, *args, **kwargs):
        super(RacerForm, self).__init__(*args, **kwargs)
        self.racer_name.validators = [AnyOf(racer_names, message='NO')]

app = Flask(__name__)
app.config.from_object(os.environ['APP_SETTINGS'])

csrf = CSRFProtect(app)
database.init_app(app)
commands.init_app(app)


@app.route('/', methods=['GET', 'POST'])
def index_post():
    global RACE_ID, CATEGORY_NAME, RACER_ID, SCROLL

    race_err_msg = None
    racer_err_msg = None

    race_form = RaceForm(Races.get_race_names())
    name_date = request.form.get('name_date')
    if race_form.validate_on_submit():
        RACE_ID = Races.get_race_id(name_date)
    else:
        race_err_msg = f'Can\'t find a race by the name {name_date}!\n'

    categories = Races.get_categories(RACE_ID)
    category_form = CategoryForm(categories)
    if category_form.validate_on_submit():
        CATEGORY_NAME = request.form['category']
    elif CATEGORY_NAME not in categories:
        CATEGORY_NAME = categories[0]

    racer_form = RacerForm(Racers.get_racer_names())
    racer_name = request.form.get('racer_name')
    if racer_form.validate_on_submit():
        RACER_ID = Racers.get_racer_id(racer_name)
    else:
        racer_err_msg = f'Can\'t find a racer by the name {racer_name}!\n'

    if racer_name:
        SCROLL = 'racer'
    else:
        SCROLL = 'race'

    racer_url = f'https://results.bikereg.com/racer/{RACER_ID}'

    race_table = Results.get_race_table(RACE_ID, CATEGORY_NAME)
    racer_table = model.get_racer_table(RACER_ID)
    racer_name = Racers.get_racer_name(RACER_ID)

    chart = make_racer_plot_alt(racer_table)

    return render_template('index.html',
                           race_form=race_form,
                           category_form=category_form,
                           racer_form=racer_form,
                           scroll=SCROLL,
                           racer_url=racer_url,
                           race_table=race_table,
                           racer_table=racer_table,
                           racer_name=racer_name,
                           race_err_msg=race_err_msg,
                           racer_err_msg=racer_err_msg,
                           chart=chart)


@app.route("/search/<string:box>")
def race_suggestions(box):
    """Create search suggestions when searching races or racers"""

    if box == 'race_name':
        options = Races.get_race_names()
    elif box == 'racer_name':
        options = Racers.get_racer_names()
    query = request.args.get('query').lower()
    suggestions = [{'value': option} for option in options
                                   if query in option.lower()]
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
        model.filter_races()

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
