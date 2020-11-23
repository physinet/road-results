import os
import time
import json

from flask import Flask, render_template, request, jsonify, url_for, redirect
from flask_wtf.csrf import CSRFProtect

import commands
import database
import scraping
import model
import ratings
import evaluation
import plotting
from model import Results, Races, Racers
from forms import RaceForm, CategoryForm, RacerForm

from preprocess import clean


app = Flask(__name__)
app.config.from_object(os.environ['APP_SETTINGS'])

csrf = CSRFProtect(app)
database.init_app(app)
commands.init_app(app)

# Pre-compute counts
with app.app_context():
    COUNTS = {table: eval(f'{table}.count()')
                    for table in ['Races', 'Results', 'Racers']}
print('App initialized.')

# global default race/category/racer
RACE_ID = 5291 # 10000 #11557
CATEGORY_INDEX = 1
RACER_ID = 12150  # 9915  #177974

def check_data_selection(race_id=None, category_index=None, racer_id=None):
    """Makes sure that we are trying to show data that is in the database."""

    errors = []
    if not race_id in Races.get_column('race_id'):
        race_id = Races.get_random_id()
        errors.append('race')
    categories = Races.get_categories(race_id)
    if category_index >= len(categories):
        category_index = 0
        errors.append('category')
    if not racer_id in Racers.get_column('RacerID'):
        # Random racer from the currently selected category
        racer_id = Results.get_random_racer_id(racer_id,
                                               categories[category_index])
        errors.append('racer')

    if errors:
        return redirect(url_for('error'))


@app.route('/', methods=['GET'])
def index_get():
    race_id = int(request.args.get('race', RACE_ID))
    category_index = int(request.args.get('category', CATEGORY_INDEX))
    racer_id = int(request.args.get('racer', RACER_ID))

    error_redirect = check_data_selection(race_id, category_index, racer_id)
    if error_redirect:
        return error_redirect

    race_form = RaceForm(race_id)
    categories = Races.get_categories(race_id)
    category_form = CategoryForm(categories)
    racer_form = RacerForm(racer_id)

    # Reset data in form fields to show placeholder text again
    race_form.reset_placeholder(race_id)
    racer_form.reset_placeholder(racer_id)

    category_name = categories[category_index]
    race_table = Results.get_race_table(race_id, category_name).all()
    race_name = Races.get_race_name(race_id)
    race_date = Races.get_race_date(race_id)
    racer_table = model.get_racer_table(racer_id)
    racer_name = Racers.get_racer_name(racer_id)

    # chart = None
    chart = plotting.make_racer_plot(racer_table, avg=Racers.get_avg_rating())

    r = render_template('index.html',
                           race_form=race_form,
                           category_form=category_form,
                           racer_form=racer_form,
                           race_table=race_table,
                           race_name=race_name,
                           race_date=race_date,
                           category_name=category_name,
                           racer_table=racer_table,
                           racer_name=racer_name,
                           counts=COUNTS,
                           chart=chart)

    return r

@app.route('/', methods=['POST'])
def index_post():
    """Translate POST into a GET request"""
    from urllib.parse import parse_qs

    # extract anchor if all else fails
    for anchor in ['race', 'racer']:
        if f'#{anchor}' in request.referrer:
            break
        else:
            anchor = ''

    # extract and parse parameters from last GET request, if any
    if '?' in request.referrer:
        params = {k: int(v[0]) for k, v in
                  parse_qs(request.referrer.split('?', 1)[1]).items()}
    else:
        params = {}
    print(params)

    name_date = request.form.get('name_date')
    category = request.form.get('category')
    racer_name = request.form.get('racer_name')

    race_form = RaceForm(params.get('race', RACE_ID))
    if race_form.validate_on_submit() and name_date:
        params['race'] = Races.get_race_id(name_date)
        params['category'] = 0  # reset category if viewing different race
        anchor = 'race'

    categories = Races.get_categories(params.get('race', RACE_ID))
    category_form = CategoryForm(categories)
    if category_form.validate_on_submit():
        params['category'] = categories.index(category)
        anchor = 'race'

    racer_form = RacerForm(params.get('racer', RACER_ID))
    if racer_form.validate_on_submit() and racer_name:
        params['racer'] = Racers.get_racer_id(racer_name)
        anchor = 'racer'

    return redirect(url_for('.index_get', _anchor=anchor, **params))


@app.route('/error')
def error():
    return render_template('error.html')


@app.route("/search/<string:box>")
def race_suggestions(box):
    """Create search suggestions when searching races or racers"""

    if box == 'race_name':
        Table = Races
    elif box == 'racer_name':
        Table = Racers

    query = request.args.get('query').lower()
    suggestions = [{'value': option} for option in
                    Table.get_suggestions(query, limit=5)]

    return jsonify({"suggestions": suggestions})


@app.route('/database')
def preview_database(methods=['GET', 'POST']):

    def parse_tables(tables_string):
        """Takes a string of table names (e.g. "Races,Results") and returns
        a list of table objects. If tables_string is True, will return a list
        containing all table objects.
        """
        _tables = {'Races': Races, 'Results': Results, 'Racers': Racers}

        tables = []
        if tables_string == 'True':
            tables = [Results, Races, Racers]
        elif tables_string:
            tables = [Table for name, Table in _tables.items()
                        if name in tables_string.split(',')]

        return tables

    if app.config.get('DB_WRITE_ACCESS'):
        drop_tables = parse_tables(request.args.get('drop'))
        commands.db_drop_all(drop_tables)
        commands.db_create_all(drop_tables)

        add_tables = parse_tables(request.args.get('add'))

        if Races in add_tables:
            subset = request.args.get('subset')
            if subset: # format '#,###'
                race_ids = list(range(*map(int, subset.split(','))))
            else:
                race_ids = list(range(1, 13000))
            Races.add_table(race_ids)
        if Results in add_tables:
            Results.add_table(Races.get_urls())
        if Racers in add_tables:
            Racers.add_table()

        if request.args.get('reset'):
            ratings.reset_ratings()
        if request.args.get('rate'):
            ratings.get_all_ratings(request.args.get('limit'))

    # Table is the appropriate class (default Results if no table param)
    Table = eval(str(request.args.get('table'))) or Results

    rows = Table.get_sample(2000, start=(request.args.get('start') or 0))
    cols = Table.get_columns()

    return render_template('database.html', cols=cols, rows=rows)


@app.route('/evaluation')
def accuracy():
    evaluation.correlation()
    evaluation.get_rating_hist()
    # evaluation.accuracy()
    # plotting.plot_hist()
    return render_template('evaluation.html')

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
