import os
basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    DEBUG = False
    TESTING = False
    WTF_CSRF_ENABLED = True
    CSRF_ENABLED = True
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')
    SECRET_KEY = os.environ.get('SECRET_KEY')

class ProductionConfig(Config):
    DEBUG = False
    DB_WRITE_ACCESS = False

class DevelopmentConfig(Config):
    ENV = "development"
    DEBUG = True
    DB_WRITE_ACCESS = True
