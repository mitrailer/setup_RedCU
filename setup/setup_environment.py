"""
Connection to postgreSQL

This script allows the user to connect to a postgreSQL database.

The script was taken from https://www.datacamp.com/community/tutorials/tutorial-postgresql-python. The author is
Samip Katwal
"""

import yaml
from sqlalchemy import create_engine
import logging

log = logging.getLogger(__name__)


def get_database():
    try:
        engine = get_connection_from_profile()
        log.info("Connected to PostgreSQL database!")
    except IOError:
        log.exception("Failed to get database connection!")
        return None, 'fail'

    return engine


def get_connection_from_profile(config_file_name='setup/default_profile.yaml'):
    """

    @param config_file_name:
    @return: File containing PGHOST, PGUSER, PGPASSWORD, PGDATABASE and PGPORT
             which are the credentials for the PostgreSQL database
    """

    """
    Sets up database connection from config file.
    Input:
    config_file_name: 
    """

    with open(config_file_name, 'r') as f:
        try:
            values = yaml.load(f, Loader=yaml.FullLoader)
        except yaml.YAMLError as exc:
            print(exc)
    if not ('PGHOST' in values.keys() and 'PGUSER' in values.keys() and 'PGPASSWORD' in values.keys() and
            'PGDATABASE' in values.keys() and 'PGPORT' in values.keys()):
        raise Exception('Bad config file: ' + config_file_name)

    return get_engine(values['PGDATABASE'], values['PGUSER'],
                      values['PGHOST'], values['PGPORT'],
                      values['PGPASSWORD'])


def get_engine(db, user, host, port, passwd):
    """
    Get SQLalchemy engine using credentials.
    @param db: database name
    @param user: Username
    @param host: Hostname of the database server
    @param port:  Port number
    @param passwd: Password for the database
    @return:
    """

    url = 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(
        user=user, passwd=passwd, host=host, port=port, db=db)
    engine = create_engine(url, pool_size = 50)
    return engine