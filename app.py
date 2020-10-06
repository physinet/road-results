from flask import Flask, render_template

def create_app(configfile=None):
    app = Flask(__name__)

    @app.route('/')
    def index():
        return render_template('index.html')

    return app


# if __name__ == '__main__':
    # app.run(port=8000, debug=True)
