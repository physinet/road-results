import os
import time

from flask import Flask, render_template, request, jsonify
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

# global variables to keep track of which race/racer info to show
RACE_ID = 5291 # 10000 #11557
CATEGORY_NAME = 'Men Collegiate Cat A'
RACER_ID = 12150  # 9915  #177974
SCROLL = ''

def check_data_selection():
    """Makes sure that we are trying to show data that is in the database."""

    global RACE_ID, CATEGORY_NAME, RACER_ID

    if not RACE_ID in Races.get_column('race_id'):
        RACE_ID = Races.get_random_id()
    categories = Races.get_categories(RACE_ID)
    if not CATEGORY_NAME in categories:
        CATEGORY_NAME = categories[0]
    if not RACER_ID in Racers.get_column('RacerID'):
        RACER_ID = Results.get_random_racer_id(RACE_ID, CATEGORY_NAME)



@app.route('/', methods=['GET', 'POST'])
def index_post():
    global RACE_ID, CATEGORY_NAME, RACER_ID, SCROLL, COUNTS

    check_data_selection()
    # Get whichever fields were submitted - 2 out of 3 of these should be None
    name_date = request.form.get('name_date')
    category = request.form.get('category')
    racer_name = request.form.get('racer_name')

    # Race form - update RACE_ID if valid name_date submitted
    race_form = RaceForm(RACE_ID)
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
    racer_form = RacerForm(RACER_ID)
    if racer_form.validate_on_submit() and racer_name:
        RACER_ID = Racers.get_racer_id(racer_name)

    # Scroll to appropriate part of website depending on what field submitted
    if name_date or category:
        SCROLL = 'race'
    elif racer_name:
        SCROLL = 'racer'

    # Reset data in form fields to show placeholder text again
    race_form.reset_placeholder(RACE_ID)
    racer_form.reset_placeholder(RACER_ID)

    race_table = Results.get_race_table(RACE_ID, CATEGORY_NAME).all()
    racer_table = model.get_racer_table(RACER_ID)
    racer_name = Racers.get_racer_name(RACER_ID)

    # chart = None
    chart = plotting.make_racer_plot(racer_table)

    r = render_template('index.html',
                           race_form=race_form,
                           category_form=category_form,
                           racer_form=racer_form,
                           scroll=SCROLL,
                           race_table=race_table,
                           racer_table=racer_table,
                           racer_name=racer_name,
                           counts=COUNTS,
                           chart=chart)

    return r


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
    # evaluation.accuracy()
    plotting.plot_hist()
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
