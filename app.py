from flask import Flask, render_template
import pandas as pd

rows = [
    {'a': 1, 'b': 2, 'c':3},
    {'a': 3, 'b': 5, 'c':-6}
]

df = pd.DataFrame(rows)

def create_app(configfile=None):
    app = Flask(__name__)

    @app.route('/')
    def index():
        return render_template('index.html')

    @app.route('/table')
    def table():
        return render_template('table.html', rows=rows)

    @app.route('/df')
    def render_df():
        return render_template('df.html', df=df)

    return app



# if __name__ == '__main__':
    # app.run(port=8000, debug=True)
