import sqlalchemy


def create_table(db_con, table_name, table_structure):

    if sqlalchemy.inspect(db_con.engine).has_table(table_name):
        db_con.execute("DROP TABLE " + table_name)

    db_con.execute("CREATE TABLE " + table_name + table_structure)
