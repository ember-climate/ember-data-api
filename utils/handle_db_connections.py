from functools import wraps
from inspect import getfullargspec
from typing import Callable

from utils.connect_to_db import connect_to_db


def handle_db_connections(_func: Callable = None, *, print_connections: bool = False) -> Callable:
    """Find any connection name argument passed, open connections, then close them after function execution.
    Optional ``print_connections`` allows seeing which connections are opened if this is desirable"""

    def connect(func: Callable):

        @wraps(func)
        def wrapper(*args, **kwargs):

            arg_list = getfullargspec(func).args
            db_cons = [arg for arg in arg_list if arg in ['raw_con', 'transformed_con', 'published_con', 'staging_con']]
            db_dict = {db_name: connect_to_db(f"ember-{db_name.split('_')[0]}") for db_name in db_cons}

            if print_connections:
                print(f'Opening {", ".join(db_cons)} for {func.__name__!r}')

            try:
                return func(*args, **kwargs, **db_dict)

            finally:
                if print_connections:
                    print(f'Closing {", ".join(db_cons)} for {func.__name__!r}')

                for db_connection in db_dict.values():
                    db_connection.close()

        return wrapper

    return connect if _func is None else connect(_func)
