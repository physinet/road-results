import os

from flask import Flask, render_template, request, jsonify
from flask_wtf.csrf import CSRFProtect

import commands
import database
import scraping
import model
from model import Results, Races, Racers
from forms import RaceForm, CategoryForm, RacerForm

from preprocess import clean

from plotting import make_racer_plot_alt

# global variables to keep track of which race/racer info to show
RACE_ID = 5291 # 10000 #11557
CATEGORY_NAME = 'Men Collegiate CAT A'
RACER_ID = 12150  # 9915  #177974
SCROLL = ''


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

    race_table = Results.get_race_table(RACE_ID, CATEGORY_NAME)
    racer_table = model.get_racer_table(RACER_ID)
    racer_name = Racers.get_racer_name(RACER_ID)

    counts = {table: eval(f'{table}.count()')
                    for table in ['Races', 'Results', 'Racers']}

    chart = None
    # chart = make_racer_plot_alt(racer_table)

    return render_template('index.html',
                           race_form=race_form,
                           category_form=category_form,
                           racer_form=racer_form,
                           scroll=SCROLL,
                           race_table=race_table,
                           racer_table=racer_table,
                           racer_name=racer_name,
                           counts=counts,
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
            Races.add_table(list(range(1, 13000)))
        if Results in add_tables:
            Results.add_table(Races.get_urls())
        if Racers in add_tables:
            Racers.add_table()

        if request.args.get('reset'):
            model.reset_ratings()
        if request.args.get('rate'):
            model.get_all_ratings()

    # Table is the appropriate class (default Results if no table param)
    Table = eval(str(request.args.get('table'))) or Results

    rows = Table.get_sample(2000, start=(request.args.get('start') or 0))
    cols = Table.get_columns()

    return render_template('database.html', cols=cols, rows=rows)


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
