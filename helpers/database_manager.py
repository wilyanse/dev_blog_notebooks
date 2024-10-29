import pandas as pd
import psycopg2
from psycopg2 import sql, connect, OperationalError, errorcodes, errors
import sys
class DatabaseManager:

    def __init__(self, user, dbname, password, host, port):
        connection_string = f'user={user} dbname={dbname} password={password} host={host} port={port}'

        try:
            self.conn = psycopg2.connect(connection_string)
            print('Database manager has connected to ' + str(dbname)) # Logging to stdout
        except Exception as err:
            self.print_psycopg2_exception(err)
            self.conn = None

    def create_entry(self, table_name, fields, values):
        cur = self.conn.cursor()
        
        # check if entry is present before inserting
        column_names, rows = self.get_value(table_name, fields, values)
        if rows != None or len(rows) >= 1:
            print('Entry already exists. Insertion is not continued.')
            return False

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
            print('Entry insertion of `ship_id:' + values[0] + '` complete') # Logging to stdout
        except Exception as err:
            self.print_psycopg2_exception(err)
            self.conn.rollback()
            return False
        cur.close()
        return True

    def update_value(self, table_name, fields, entry):

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
            # Execute query with the provided values
            cur.execute(query, values)
            rows = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            print(f'Fetched {len(rows)} entries from {table_name}')  # Logging to stdout
        except Exception as err:
            self.print_psycopg2_exception(err)
            self.conn.rollback()
            return None, None
        finally:
            cur.close()

        return column_names, rows


    def delete_value(self, table_name, fields, values):

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

        err_type, err_obj, traceback = sys.exc_info()
        line_num = traceback.tb_lineno
        print ("\npsycopg2 ERROR:", err, "on line number:", line_num)
        print ("psycopg2 traceback:", traceback, "-- type:", err_type)
        print ("\nextensions.Diagnostics:", err.diag)
        print ("pgerror:", err.pgerror)
        print ("pgcode:", err.pgcode, "\n")