from flask import Flask, render_template
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

df = df_race[['FirstName', 'LastName', 'TeamName', 'RaceTime']]

def create_app(configfile=None):
    app = Flask(__name__)

    @app.route('/')
    def index():
        return render_template('index.html')

    @app.route('/table')
    def table():
        return render_template('table.html', df=df)

    @app.route('/df')
    def render_df():
        return render_template('df.html', df=df)

    return app



# if __name__ == '__main__':
    # app.run(port=8000, debug=True)
