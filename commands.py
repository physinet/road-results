import click
from database import db
from model import Races, Results, Racers
from psycopg2.errors import UndefinedTable

TABLES = [Races, Results, Racers]


def db_create_all(tables=TABLES):
    """Creates specified tables. (Default: all tables)"""
    for Table in tables:
        Table.__table__.create(db.session.bind, checkfirst=True)

def db_drop_all(tables=TABLES):
    """Drops specified tables (Default: all tables)"""
    for Table in tables:
        try:
            Table.__table__.drop(db.session.bind, checkfirst=True)
        except Exception as e:
            print(e)
            pass # table probably doesn't exist for some reason

def init_app(app):
    # add multiple commands in a bulk
    for command in [db_create_all, db_drop_all]:
        app.cli.add_command(app.cli.command()(command))
