import pandas as pd
import psycopg2
from psycopg2 import sql, connect, OperationalError, errorcodes, errors
import sys
class DatabaseManager:
    """
    A class to manage database connections using psycopg2.

    ...

    Attributes
    ----------
    conn : psycopg2 connection object
        connection to the database to handle database actions

    Methods
    -------
    create_tables():
        Creates the `parameters` and `blendermann_coefficients` tables for the database.

    create_entry(table_name, fields, values):
        Creates an entry in the table given the values and fields to insert them into.
    
    update_value(table_name, fields, entry):
        Updates a specified entry in the database given the fields and entry details.
    
    get_table(table_name):
        Obtains all the entries of a table when given the table name.

    get_value(table_name, fields, values):
        Runs a select statement, filtering the values by supplying the WHERE clause of the SQL query using the provided fields and values.
    
    delete_value(table_name, fields, values):
        Deletes the entries of a table in the database given the fields and their corresponding values.

    get_fields(table_name):
        Returns the fields of the database table given the table name.

    get_table_desc(table_name):
        Returns a tuple of the fields as well as their type codes.
    
    insert_dataframe(table_name, df):
        Inserts a pandas dataframe as a series of INSERT statements into the database table table_name
    """

    def __init__(self, user, dbname, password, host, port):
        """
        Constructs a database connection using the provided parameters.
        Configured in the provided .env file, can be manually set as well by calling an instance of the manager as DatabaseManager(user, dbname, password, host, port)

        ...

        Parameters
        ----------
            user : str
                username credentials of the database
            dbname : str
                name of the database to request access to
            password : str
                password credentials of the database
            host : str
                host address of the database
            port : str
                port address of the database
        
        Returns
        ----------
        True: if the database was connected
        False: if the database was not connected
        """
        connection_string = f'user={user} dbname={dbname} password={password} host={host} port={port}'

        try:
            self.conn = psycopg2.connect(connection_string)
            print('Database manager has connected to ' + str(dbname)) # Logging to stdout
        except Exception as err:
            self.print_psycopg2_exception(err)
            self.conn = None

    def create_entry(self, table_name, fields, values):
        """
        Creates an entry in the database

        ...

        Parameters
        ----------
        table_name: str
            database table to insert the entry into
        fields: tuple
            list of fields to insert the entry into
        values: tuple
            list of values per specfied field of the entry
        
        Returns
        ----------
        True: if the entry was created
        False: if the entry was not created
        """

        cur = self.conn.cursor()
        
        # query for inserting values
        query = sql.SQL("""
        INSERT INTO {table} ({fields})
        VALUES ({values})
        """).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(', ').join(map(sql.Identifier, fields)),
            values=sql.SQL(', ').join(sql.Placeholder() * len(values))
        )

        try:
            cur.execute(query, values)
            self.conn.commit()
            print('Entry insertion of value ' + values[0] + '` complete') # Logging to stdout
        except Exception as err:
            self.print_psycopg2_exception(err)
            self.conn.rollback()
            return False
        cur.close()
        return True

    def update_value(self, table_name, fields, entry):
        """
        Updates an entry given a primary key.

        ...

        Parameters
        ----------
        table_name: str
            database table that contains the entry
        fields: tuple
            list of fields to update the entry to
            first element is assumed to be the primary key
        values: tuple
            list of values to update the entry to
            first element is assumed to be the primary key
        
        Returns
        ----------
        True: if the entry was updated
        False: if the entry was not updated
        """

        cur = self.conn.cursor()
        pk_value = entry[0]
        new_values = entry[1:]

        # query for updating values from table
        set_clause = sql.SQL(', ').join(
            sql.SQL("{field} = %s").format(field=sql.Identifier(field))
            for field in fields[1:]
        )
        where_clause = sql.SQL("{pk_field} = %s").format(pk_field=sql.Identifier(fields[0]))

        query = sql.SQL("""
        UPDATE {table}
        SET {set_clause}
        WHERE {where_clause}
        """).format(
            table=sql.Identifier(table_name),
            set_clause=set_clause,
            where_clause=where_clause
        )

        values = new_values + [pk_value]

        try:
            cur.execute(query, values)
            self.conn.commit()
            print(str(pk_value) + ' updated to new values: ' + str(entry) + '.') # Logging to stdout
        except Exception as err:
            self.print_psycopg2_exception(err)
            self.conn.rollback()
            return False
        cur.close()
        return True

    def get_table(self, table_name):
        """
        Returns the rows of a table as a mutable object.

        ...

        Parameters
        ----------
        table_name: str
            name of the database table to retrieve entries from
        
        Returns
        ----------
        column_names: list or None
            list is returned if no error occurred, None otherwise
        rows: list or None
            list is returned if no error occurred, None otherwise
        """
        cur = self.conn.cursor()

        # query for reading from table
        query = sql.SQL("""
        SELECT * FROM {table}
        """).format(
            table=sql.Identifier(table_name)
        )
        try:
            cur.execute(query, table_name)
            rows = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            print('Fetched ' + str(len(rows)) + ' entries from ' + table_name) # Logging to stdout
            cur.close()
            return column_names, rows
        except Exception as err:
            self.print_psycopg2_exception(err)
            self.conn.rollback()
            return None, None
        
    def get_value(self, table_name, fields, values):
        """
        Gets values from the database table given a set of fields and values to search from
        Note that the template for obtaining the value only works for fields that have one value, i.e. primary key fields such as `ship_id` or `vessel_type`
        The template for obtaining values within an array or timestamp will vary and need to be created in a separate function if needed
        ...

        Parameters
        ----------
        table_name: str
            name of the database table to retrieve entries from
        fields: tuple
            list of fields to find entry
        values: tuple
            list of values to find entry
        Returns
        ----------
        column_name, rows: list or None
            returns mutable objects for both variables if elements that follow the conditions were found, None otherwise
        """

        cur = self.conn.cursor()

        # query for selecting with set values
        where_clause = sql.SQL(' AND ').join(
            sql.SQL("{field} = %s").format(field=sql.Identifier(field))
            for field in fields
        )
        query = sql.SQL("""
        SELECT * FROM {table}
        WHERE {where_clause}
        """).format(
            table=sql.Identifier(table_name),
            where_clause=where_clause
        )
        
        try:
            cur.execute(query, values)
            rows = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            print('Fetched ' + str(len(rows)) + ' entries from ' + table_name) # Logging to stdout
        except Exception as err:
            self.print_psycopg2_exception(err)
            self.conn.rollback()
            return None, None
        cur.close()
        return column_names, rows

    def delete_value(self, table_name, fields, values):
        """
        Deletes values from the database table

        ...

        Parameters
        ----------
        table_name: str
            name of the database table to retrieve entries from
        fields: tuple
            list of fields to find entry to delete
        values: tuple
            list of values to find entry to delete
        Returns
        ----------
        True: if the entry was deleted
        False: if the entry was not deleted
        """

        cur = self.conn.cursor()
        
        # query for deleting with conditions
        where_clause = sql.SQL(' AND ').join(
            sql.SQL("{field} = %s").format(field=sql.Identifier(field))
            for field in fields
        )
        query = sql.SQL("""
        DELETE FROM {table}
        WHERE {where_clause}
        """).format(
            table=sql.Identifier(table_name),
            where_clause=where_clause
        )
        
        try:
            cur.execute(query, values)
            self.conn.commit()
            print('Deleted ' + str(values) + ' from ' + str(table_name)) # Logging to stdout
        except Exception as err:
            self.print_psycopg2_exception(err)
            self.conn.rollback()
            return False
        cur.close()
        return True

    def get_fields(self, table_name):
        """
        Gets the list of fields of the database table

        ...

        Parameters
        ----------
        table_name: str
            name of the database table to get fields from

        Returns
        ----------
        column_names: list or None
            returns a mutable object of the list of table fields or None if an error occurred
        """
        cur = self.conn.cursor()
        # Execute a simple SELECT query
        try:
            cur.execute(sql.SQL("SELECT * FROM {table} LIMIT 0").format(
                table=sql.Identifier(table_name)
            ))
            column_names = [desc[0] for desc in cur.description]
        except Exception as err:
            self.print_psycopg2_exception(err)
            self.conn.rollback()
            return None
        cur.close()
        return column_names

    def get_table_desc(self, table_name):
        """
        Gets the description of the datbaase table as a mutable object of columns where the first element is the name of the column and the second is the type code

        ...

        Parameters
        ----------
        table_name: str
            name of the database table to get fields from

        Returns
        ----------
        descriptions: list or None
            returns a mutable object of the description of the table's fields or None if an error occurred
        """
        cur = self.conn.cursor()
        # Execute a simple SELECT query
        try:
            cur.execute(sql.SQL("SELECT * FROM {table} LIMIT 0").format(
                table=sql.Identifier(table_name)
            ))
        except Exception as err:
            self.print_psycopg2_exception(err)
            self.conn.rollback()
            return None
        cur.close()
        return cur.description

    def insert_dataframe(self, table_name, df):
        """
        Inserts a pandas dataframe as a series of INSERT statements into specified database table
        Used when extracting data from a CSV file
        Could also be used for general purposes such as having a pandas dataframe from any source
        ...

        Parameters
        ----------
        table_name: str
            name of the database table to insert entries into
        df: dataframe
            pandas dataframe to insert all entries from

        Returns
        ----------
        True: if the dataframe was inserted
        False: if the dataframe was not inserted
        """

        cur = self.conn.cursor()
        columns = list(df.columns)
        values = [tuple(row) for row in df.itertuples(index=False, name=None)]
        print('Inserting ' + str(len(values)) + ' to ' + table_name) # Logging to stdout
        for value in values:
            self.create_entry(table_name, tuple(columns), tuple(value))
        print('Dataframe insertion to ' + table_name + ' complete.') # Logging to stdout
        cur.close()

    def execute_query(self, query):
        cur = self.conn.cursor()
        try:
            cur.execute(query)
            print("query executed")
            self.conn.commit()
        except Exception as err:
            self.print_psycopg2_exception(err)
            self.conn.rollback()
            return False

        cur.close()
        return True

    def print_psycopg2_exception(self, err):
        """
        Prints the error obtained from handling database functions with psycopg2
        ...

        Parameters
        ----------
        err: Exception
            error to obtain details from

        Returns
        ----------
        None
        """

        err_type, err_obj, traceback = sys.exc_info()
        line_num = traceback.tb_lineno
        print ("\npsycopg2 ERROR:", err, "on line number:", line_num)
        print ("psycopg2 traceback:", traceback, "-- type:", err_type)
        print ("\nextensions.Diagnostics:", err.diag)
        print ("pgerror:", err.pgerror)
        print ("pgcode:", err.pgcode, "\n")