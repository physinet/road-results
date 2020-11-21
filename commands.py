import click
from database import db


def db_create_all():
    """Creates all tables"""
    db.create_all()


def db_drop_all():
    """Cleans database"""
    db.drop_all()


def init_app(app):
    # add multiple commands in a bulk
    for command in [db_create_all, db_drop_all]:
        app.cli.add_command(app.cli.command()(command))
