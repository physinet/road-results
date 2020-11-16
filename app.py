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

# global variables to keep track of which race/racer info to show
RACE_ID = 10000 #11557
CATEGORY_NAME = 'Men Collegiate CAT A'
RACER_ID = 9915  #177974
SCROLL = ''


class RaceForm(FlaskForm):
    name_date = StringField('name_date', id='name_date',
                 filters=[lambda x: x or Races.get_race_name_date(RACE_ID)])
            # Filter to transform empty string into the currently selected race
    submit = SubmitField('Show me this race!', id='race_name_submit')

    def __init__(self, race_names, *args, **kwargs):
        super(RaceForm, self).__init__(*args, **kwargs)
        msg = f'Can\'t find a race by the name {self.name_date.data}!\n'
        self.name_date.validators = [AnyOf(race_names, message=msg)]

class CategoryForm(FlaskForm):
    category = SelectField('Category', id='category')
    submit = SubmitField('Show me this category!', id='category_submit')

    def __init__(self, categories, *args, **kwargs):
        super(CategoryForm, self).__init__(*args, **kwargs)
        self.category.choices = categories

class RacerForm(FlaskForm):
    racer_name = StringField('racer_name', id='racer_name',
                 filters=[lambda x: x or Racers.get_racer_name(RACER_ID)])
            # Filter to transform empty string into the currently selected racer
    submit = SubmitField('Show me this racer!', id='racer_name_submit')

    def __init__(self, racer_names, *args, **kwargs):
        super(RacerForm, self).__init__(*args, **kwargs)
        msg = f'Can\'t find a racer by the name {self.racer_name.data}!\n'
        self.racer_name.validators = [AnyOf(racer_names, message=msg)]

app = Flask(__name__)
app.config.from_object(os.environ['APP_SETTINGS'])

csrf = CSRFProtect(app)
database.init_app(app)
commands.init_app(app)


@app.route('/', methods=['GET', 'POST'])
def index_post():
    global RACE_ID, CATEGORY_NAME, RACER_ID, SCROLL

    # Get whichever fields were submitted - 2 out of 3 of these should be None
    name_date = request.form.get('name_date')
    category = request.form.get('category')
    racer_name = request.form.get('racer_name')

    # Race form - update RACE_ID if valid name_date submitted
    race_form = RaceForm(Races.get_race_names())
    if race_form.validate_on_submit() and name_date:
        RACE_ID = Races.get_race_id(name_date)

    # Category form - update CATEGORY_NAME with first category or selected cat
    categories = Races.get_categories(RACE_ID)
    category_form = CategoryForm(categories)
    if category_form.validate_on_submit():
        CATEGORY_NAME = category
    elif CATEGORY_NAME.lower() not in [c.lower() for c in categories]:
        CATEGORY_NAME = categories[0]

    # Racer form - update RACER_ID if valid racer name submitted
    racer_form = RacerForm(Racers.get_racer_names())
    if racer_form.validate_on_submit() and racer_name:
        RACER_ID = Racers.get_racer_id(racer_name)

    # Scroll to appropriate part of website depending on what field submitted
    if name_date or category:
        SCROLL = 'race'
    elif racer_name:
        SCROLL = 'racer'

    # Reset data in form fields to show placeholder text again
    race_form.name_date.description = Races.get_race_name_date(RACE_ID)
    race_form.name_date.data = ''
    racer_form.racer_name.description = Racers.get_racer_name(RACER_ID)
    racer_form.racer_name.data = ''

    race_table = Results.get_race_table(RACE_ID, CATEGORY_NAME)
    racer_table = model.get_racer_table(RACER_ID)
    racer_name = Racers.get_racer_name(RACER_ID)

    chart = make_racer_plot_alt(racer_table)

    return render_template('index.html',
                           race_form=race_form,
                           category_form=category_form,
                           racer_form=racer_form,
                           scroll=SCROLL,
                           race_table=race_table,
                           racer_table=racer_table,
                           racer_name=racer_name,
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


@app.route('/database')
def preview_database(methods=['GET', 'POST']):
    if app.config.get('DB_WRITE_ACCESS'):
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

        if request.args.get('filter'):
            model.filter_races()

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
