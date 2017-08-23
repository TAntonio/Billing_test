import sqlite3

DB_FILENAME = 'database.db'
SCHEMA_FILENAME = 'schema.sql'


class Database(object):

    def __init__(self):
        try:
            self.connection = sqlite3.connect(DB_FILENAME, check_same_thread=False)
            self.create_table()
            self.delete_from_table()
        except sqlite3.Error as e:
            print("Error connecting to database")


    def __del__(self):
        self.connection.close()


    # we need to create context manager to guarantee
    # closing connection in all cases
    def __enter__(self):
        return self


    def __exit__(self, ext_type, exc_value, traceback):
        if isinstance(exc_value, Exception):
            self.connection.rollback()
        else:
            self.connection.commit()
        self.connection.close()


    def create_table(self):
        with open(SCHEMA_FILENAME, mode='r') as schema:
            self.connection.executescript(schema.read())
            cursor = self.connection.cursor()
            self.connection.commit()
            cursor.close()


    def delete_from_table(self):
        delete_query = "DELETE FROM billing_data"
        cursor = self.connection.cursor()
        cursor.execute(delete_query)
        self.connection.commit()
        cursor.close()


    def save_billing_data(self, data):
        insert_sql = "INSERT INTO billing_data(object_type, object_id, cost) VALUES(?, ?, ?)"
        try:
            cursor = self.connection.cursor()
            cursor.executemany(insert_sql, self.get_billing_data(data))
            self.connection.commit()
            cursor.close()
        except Exception as e:
            print('Problem with saving billing data', e)


    def get_billing_data(self, data):
        for (obj_type, obj_id), cost in iter(data.items()):
            yield (obj_type, obj_id, cost)

