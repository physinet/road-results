import click
from database import db
from model import Model


def db_create_all():
    """Creates all tables"""
    db.create_all()


def db_drop_all():
    """Cleans database"""
    db.drop_all()


def create_model_table():
    """ Create table model in the database """
    Model.__table__.create(db.engine)


def init_app(app):
    # add multiple commands in a bulk
    for command in [db_create_all, db_drop_all, create_model_table]:
        app.cli.add_command(app.cli.command()(command))
