from flask import Flask, render_template, request, jsonify
from flask_wtf import FlaskForm
from wtforms import SelectField, validators
import pandas as pd
import dill

rows = [
    {'a': 1, 'b': 2, 'c':3},
    {'a': 3, 'b': 5, 'c':-6}
] * 17

df = pd.DataFrame(rows)

file = r'C:\data\results\races\1000.pkd'
json = dill.load(open(file, 'rb'))
df_race = pd.read_json(json)
columns = [str(i) for i in range(28)]
df_race = df_race.drop(columns=columns)

df_race = df_race[df_race['RaceCategoryName'] == 'Category 3']
df = df_race[['FirstName', 'LastName', 'TeamName', 'RaceTime']]

race_names = ['test1', 'test2', 'test3', 'bucknell']
possible_names = {'0': 'hans', '1': 'sepp', '3': 'max'}


class RaceForm(FlaskForm):
    name = SelectField('race_name',
                            # choices=race_names,
                            choices=[("", "")] + [(uuid, name) for uuid, name in possible_names.items()],  # [("", "")] is needed for a placeholder
                            validators=[validators.InputRequired()])

def create_app(configfile=None):
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'YOUR SECRET KEY'

    @app.route('/')
    def index():
        return render_template('index.html', race_list=race_names,
                                             form=RaceForm(),
                                             race_name='Test race name')

    @app.route('/table')
    def table():
        return render_template('table.html', df=df)

    @app.route('/', methods=['POST'])
    def index_post():
        race_name = request.form['race_name']
        return render_template('index.html', race_list=race_names,
                                             form=RaceForm(),
                                             scroll='results',
                                             race_name=race_name)
    @app.route("/search/<string:box>")
    def process(box):
        query = request.args.get('query')
        suggestions = [{'value': 'joe','data': 'joe'}, {'value': 'jim','data': 'jim'}]
        return jsonify({"suggestions":suggestions})

    @app.route('/df')
    def render_df():
        return render_template('df.html', df=df)

    return app



# if __name__ == '__main__':
    # app.run(port=8000, debug=True)
