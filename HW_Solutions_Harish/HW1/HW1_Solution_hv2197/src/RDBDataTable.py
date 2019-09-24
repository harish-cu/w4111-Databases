from src.BaseDataTable import BaseDataTable
import pymysql
import logging

class RDBDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info=None, key_columns=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """

        self._logger = logging.getLogger()
        self._table_name = table_name
        if connect_info:
            try:
                self._conn = pymysql.connect(
                    host=connect_info['host'],
                    user=connect_info['user'],
                    password=connect_info['password'],
                    db=connect_info['db'],
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor)
            except Exception as e:
                print("Unable to connect with connect_info provided. Here is the exception:\n{}".format(e))
                print("\nProceeding with trying to connect using the default connection parameters")
                self._conn = self._get_default_connection()
        else:
            self._conn = self._get_default_connection()
        if isinstance(key_columns, str):
            self._key_columns = [key_columns]
        else:
            self._key_columns = key_columns

    @staticmethod
    def _get_default_connection():
        conn = pymysql.connect(host='localhost',
                               user='dbuser',
                               password='dbuserdbuser',
                               db='lahman2019raw',
                               charset='utf8mb4',
                               cursorclass=pymysql.cursors.DictCursor)
        return conn

    def validate_template(self, template):
        """
        Check if template is provided in the right format
        """
        if not isinstance(template, dict):
            raise ValueError('Input needs to be a dictionary')

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        if not self._key_columns:
            raise ValueError("""No primary key passed to the constructor for this table. 
                             Please pass a valid primary key to the constructor in order to use this method""")

        if len(key_fields) != len(self._key_columns):
            raise ValueError(
                "The primary key passed for the table is {}. Pass values as a list only for these columns in the same "
                "order".format(self._key_columns))

        template = dict(zip(self._key_columns, key_fields))
        matched_results = self.find_by_template(template, field_list)
        return matched_results


    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        if template == {}:
            return None
        if field_list == []:
            field_list = None

        self.validate_template(template)

        sql, args = self.create_select(self._table_name, template, field_list)
        rows_affected, data = self.run_q(sql, args, conn=self._conn)
        return data


    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """
        if not self._key_columns:
            raise ValueError("""No primary key passed to the constructor for this table. 
                             Please pass a valid primary key to the constructor in order to use this method""")

        if len(key_fields) != len(self._key_columns):
            raise ValueError(
                "The primary key passed for the table is {}. Pass values as a list only for these columns in the same "
                "order".format(self._key_columns))

        template = dict(zip(self._key_columns, key_fields))
        rows_affected = self.delete_by_template(template)
        return rows_affected

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        if template == {}:
            return None

        self.validate_template(template)

        sql, args = self.create_delete(self._table_name, template)
        rows_affected, data = self.run_q(sql, args, conn=self._conn)
        return rows_affected

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        if not self._key_columns:
            raise ValueError("""No primary key passed to the constructor for this table. 
                             Please pass a valid primary key to the constructor in order to use this method""")

        if len(key_fields) != len(self._key_columns):
            raise ValueError(
                "The primary key passed for the table is {}. Pass values as a list only for these columns in the same "
                "order".format(self._key_columns))

        template = dict(zip(self._key_columns, key_fields))
        rows_affected = self.update_by_template(template, new_values)
        return rows_affected

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        if template == {} or new_values == {}:
            return None

        self.validate_template(template)
        self.validate_template(new_values)

        sql, args = self.create_update(self._table_name, new_values, template)
        rows_affected, data = self.run_q(sql, args, conn=self._conn)
        return rows_affected

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        if new_record == {}:
            return None
        sql, args = self.create_insert(self._table_name, new_record)
        rows_affected, data = self.run_q(sql, args, conn=self._conn)
        return rows_affected

    def get_rows(self):
        rowcount, data = self.run_q('select * from {}'.format(self._table_name), conn=self._conn)
        return data

    def template_to_where_clause(self, template):
        """

        :param template: One of those weird templates
        :return: WHERE clause corresponding to the template.
        """

        if template is None or template == {}:
            result = (None, None)
        else:
            args = []
            terms = []

            for k,v in template.items():
                terms.append(" " + k + "=%s ")
                args.append(v)

            w_clause = "AND".join(terms)
            w_clause = " WHERE " + w_clause

            result = (w_clause, args)

        return result

    def create_select(self, table_name, template, fields, order_by=None, limit=None, offset=None):
        """
        Produce a select statement: sql string and args.

        :param table_name: Table name: May be fully qualified dbname.tablename or just tablename.
        :param fields: Columns to select (an array of column name)
        :param template: One of Don Ferguson's weird JSON/python dictionary templates.
        :param order_by: Ignore for now.
        :param limit: Ignore for now.
        :param offset: Ignore for now.
        :return: A tuple of the form (sql string, args), where the sql string is a template.
        """

        if fields is None:
            field_list = " * "
        else:
            field_list = " " + ",".join(fields) + " "

        w_clause, args = self.template_to_where_clause(template)

        sql = "select " + field_list + " from " + table_name + " " + w_clause

        return sql, args

    def create_insert(self, table_name, row):
        """
        :param table_name: A table name, which may be fully qualified.
        :param row: A Python dictionary of the form: { ..., "column_name" : value, ...}
        :return: SQL template string, args for insertion into the template
        """

        sql = "Insert into " + table_name + " "
        cols = []
        args = []

        for k, v in row.items():
            cols.append(k)
            args.append(v)

        col_clause = "(" + ",".join(cols) + ") "

        no_cols = len(cols)
        terms = ["%s"] * no_cols
        terms = ",".join(terms)
        value_clause = " values (" + terms + ")"

        sql += col_clause + value_clause

        return sql, args

    def create_update(self, table_name, new_values, template):
        """
        :param new_values: A dictionary containing cols and the new values.
        :param template: A template to form the where clause.
        :return: An update statement template and args.
        """
        set_terms = []
        args = []

        for k, v in new_values.items():
            set_terms.append(k + "=%s")
            args.append(v)

        s_clause = ",".join(set_terms)
        w_clause, w_args = self.template_to_where_clause(template)

        # There are %s in the SET clause and the WHERE clause. We need to form
        # the combined args list.
        args.extend(w_args)

        sql = "update " + table_name + " set " + s_clause + " " + w_clause

        return sql, args

    def create_delete(self, table_name, template):
        """
        Produce a delete statement: sql string and args.

        :param table_name: Table name: May be fully qualified dbname.tablename or just tablename.
        :param template: One of Don Ferguson's weird JSON/python dictionary templates.
        :return: A tuple of the form (sql string, args), where the sql string is a template.
        """

        w_clause, args = self.template_to_where_clause(template)

        sql = "delete from " + table_name + " " + w_clause

        return sql, args

    def run_q(self, sql, args=None, fetch=True, cur=None, conn=None, commit=True):
        '''
        Helper function to run an SQL statement.

        :param sql: SQL template with placeholders for parameters.
        :param args: Values to pass with statement.
        :param fetch: Execute a fetch and return data.
        :param conn: The database connection to use. The function will use the default if None.
        :param cur: The cursor to use. This is wizard stuff. Do not worry about it for now.
        :param commit: This is wizard stuff. Do not worry about it.

        :return: A tuple of the form (execute response, fetched data)
        '''

        cursor_created = False
        connection_created = False

        try:

            if conn is None:
                connection_created = True
                conn = self._get_default_connection()

            if cur is None:
                cursor_created = True
                cur = conn.cursor()

            if args is not None:
                log_message = cur.mogrify(sql, args)
            else:
                log_message = sql

            self._logger.debug("Executing SQL = " + log_message)

            res = cur.execute(sql, args)

            if fetch:
                data = cur.fetchall()
            else:
                data = None

            if commit:
                conn.commit()

        except Exception as e:
            raise e

        return res, data
