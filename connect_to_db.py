import sqlalchemy as db
from configparser import ConfigParser
import os


def connect_to_db(db_group):
    """Connect to a Database with parameters defined in a config file

    :param db_group: the name of the group within the config file to connect to
    :returns: a connection object
     """

    # Full path to the SQL configuration file containing connection parameters
    db_config = "config.ini"
    config = ConfigParser()
    config.read(db_config)

    db_info = config[db_group]

    engine = db.create_engine(
        "mysql+pymysql://{}:{}@db-mysql-lon1-22184-do-user-2034029-0.b.db.ondigitalocean.com:{}/{}"
        .format(db_info["user"], db_info["password"], db_info["port"], db_info["database"]))

    return engine.connect()