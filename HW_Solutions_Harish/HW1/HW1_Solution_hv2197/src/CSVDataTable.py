from src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        self._primary_key_values_list = []

        if isinstance(key_columns, str):
            self._key_columns = [key_columns]
        else:
            self._key_columns = key_columns

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0, CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1 * temp_r) - 1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        if self._key_columns:
            primary_key_field_values = CSVDataTable.get_columns(r, self._key_columns)
            self._primary_key_values_list.append(primary_key_field_values)
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            self._fieldnames = csv_d_rdr.fieldnames
            for r in csv_d_rdr:
                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """
        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, 'w') as f:
            if len(self._rows) > 0:
                writer = csv.DictWriter(f, fieldnames=self._rows[0].keys())
                writer.writeheader()
                writer.writerows(self._rows)
            else:
                f.write('')

        self._logger.debug("CSVDataTable.save: Saved " + str(len(self._rows)) + " rows")

    def check_columns(self, col_list):
        """
        Check if columns provided in the template (or to any function) are all present in the file/table
        """
        missing_cols = [col for col in col_list if col not in self._fieldnames]
        if missing_cols:
            raise ValueError('Column(s) {} is/are not present in the table'.format(missing_cols))

    def validate_template(self, template):
        """
        Check if template is provided in the right format
        """
        if not isinstance(template, dict):
            raise ValueError('Input needs to be a dictionary')
        self.check_columns(template.keys())

    @staticmethod
    def get_columns(row, col_list):

        if col_list is None:
            return row

        result = {}
        for c in col_list:
            result[c] = row[c]
        return result

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

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
            return [CSVDataTable.get_columns(row, field_list) for row in self._rows]
        self.validate_template(template)
        matched_results = []
        for row in self._rows:
            if CSVDataTable.matches_template(row, template):
                matched_results.append(CSVDataTable.get_columns(row, field_list))

        if matched_results:
            return matched_results
        else:
            return None

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        if key_fields is None or key_fields == []:
            raise ValueError("Key Fields need to be provided in order to use this method")

        if not self._key_columns:
            raise ValueError("""No primary key passed to the CSVDataTable constructor for this table. 
                             Please pass a valid primary key to the constructor in order to use this method""")

        if len(key_fields) != len(self._key_columns):
            raise ValueError(
                "The primary key for the table is {}. Pass values as a list only for these columns in the same "
                "order".format(self._key_columns))

        template = dict(zip(self._key_columns, key_fields))
        matched_results = self.find_by_template(template, field_list)
        return matched_results

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record to delete.
        :return: A count of the rows deleted.
        """
        if key_fields is None or key_fields == []:
            raise ValueError("Key Fields need to be provided in order to use this method")

        if not self._key_columns:
            raise ValueError("""No primary key passed to the CSVDataTable constructor for this table.
                             Please pass a valid primary key to the constructor in order to use this method""")

        if len(key_fields) != len(self._key_columns):
            raise ValueError(
                "The primary key for the table is {}. Pass values as a list only for these columns in the same "
                "order".format(self._key_columns))

        template = dict(zip(self._key_columns, key_fields))
        return self.delete_by_template(template)

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        if template == {}:
            raise ValueError("Please provide a non empty template")
        self.validate_template(template)

        rows_to_del = self.find_by_template(template)
        if not rows_to_del:
            return 0

        for row in rows_to_del:
            self._rows.remove(row)

        if self._key_columns:
            primary_keys_to_del = [CSVDataTable.get_columns(row, self._key_columns) for row in rows_to_del]
            for key_val in primary_keys_to_del:
                self._primary_key_values_list.remove(key_val)

        return len(rows_to_del)

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        if key_fields is None or key_fields == []:
            raise ValueError("Key Fields need to be provided in order to use this method")

        if not self._key_columns:
            raise ValueError("""No primary key passed to the CSVDataTable constructor for this table.
                             Please pass a valid primary key to the constructor in order to use this method""")

        if len(key_fields) != len(self._key_columns):
            raise ValueError(
                "The primary key for the table is {}. Pass values as a list only for these columns in the same "
                "order".format(self._key_columns))

        template = dict(zip(self._key_columns, key_fields))
        return self.update_by_template(template, new_values)

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        if template == {}:
            raise ValueError("Please provide a non empty template")

        if new_values == {}:
            return 0

        self.validate_template(template)
        self.validate_template(new_values)

        matched_results = self.find_by_template(template)
        if not matched_results:
            return 0

        if self._key_columns:
            matched_primary_keys = [CSVDataTable.get_columns(row, self._key_columns) for row in matched_results]

        rows_temp = copy.deepcopy(self._rows)
        primary_key_values_list_temp = copy.deepcopy(self._primary_key_values_list)

        try:
            for row in matched_results:
                self._rows.remove(row)

            if self._key_columns:
                for key_val in matched_primary_keys:
                    self._primary_key_values_list.remove(key_val)

            for k, v in new_values.items():
                for row in matched_results:
                    row[k] = v

            for row in matched_results:
                self.insert(row)

            return len(matched_results)

        except Exception as e:
            print("Update of the data Failed because: \n {}".format(e))
            self._rows = copy.deepcopy(rows_temp)
            self._primary_key_values_list = copy.deepcopy(primary_key_values_list_temp)
            return 0

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        # Add a row(dictionary) to the self._rows list
        if new_record is None:
            raise ValueError("Nothing to insert")

        self.validate_template(new_record)
        for col in self._fieldnames:
            if col not in new_record:
                new_record[col] = ''

        if self._key_columns:
            primary_key_field_values = CSVDataTable.get_columns(new_record, self._key_columns)
            invalid_primary_key_values = [key for key, val in primary_key_field_values.items() if
                                          val == '' or val is None]

            if invalid_primary_key_values:
                raise ValueError(
                    "Primary key field(s) {} cannot be None/empty in the data trying to be inserted. \n".format(
                        invalid_primary_key_values))

            if primary_key_field_values in self._primary_key_values_list:
                raise ValueError(
                    "Row with same values for the primary key already exists: \n{}".format(primary_key_field_values,
                                                                                           json.dumps(new_record, indent=2)))
            else:
                self._primary_key_values_list.append(primary_key_field_values)
        self._add_row(new_record)

    def get_rows(self):
        return self._rows
