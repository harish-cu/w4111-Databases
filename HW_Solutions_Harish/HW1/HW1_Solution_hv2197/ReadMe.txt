This is the ReadMe for HW1 of the Databases Class taught by Prof. Donald Ferguson

Name: Harish Babu Visweswaran
UNI: hv2197

The zip file has three folders: Data, src and tests along with this Readme file. The Data folder contains all the files copied from Prof. Donald Ferguson's Github repo present inside this folder:
https://github.com/donald-f-ferguson/w4111-Databases/tree/master/Data/lahman2019

The src folder contains the BaseDataTable.py, CSVDataTable.py and RDBDataTable.py files. The BaseDataTable.py file hasn't been modified from the template provided but the CSVDataTable.py and RDBDataTable.py files have been updated to populate code for all the required methods and some helper methods have also been included. 

The tests folder contains the unit_test.py file that hasn't been modified and the two test files csv_table_tests.py and rdb_table_tests.py that have tests written on two tables: people and batting. The outputs of the tests have been stored in csv_table_test.txt and rdb_table_test.txt files.

Notes about running the script:
* The CSVDataTable and the RDBDataTable import the BaseDataTable. The code is set up on the assumption that Python is started in the root folder of the solution and the import of the BaseDataTable is made using "from src.RDBDataTable import RDBDataTable". Similarly the test scripts import the CSVDataTable using "from src.CSVDataTable import CSVDataTable". If python in started at a different location, the import statements might not work and have to be updated accordingly
* In the CSVDataTable, the data_dir variable is set at the top of the script. This points to the directory where the data is present and can be updated if the test script is moved to a different location and if the path is not accurate anymore
* For the RDBDataTable tests, the connect info is provided at the top of the file. This can also be changed before the test script is run if necessary
* Note that running the RDBDataTable tests modify the contents of the table (deletes, updates will take place)


Summary of Design decisions (these are design decisions that need to be known before the usage of the scripts):
* In the CSVDataTable, due to the size of the files, the check for duplicates based on the primary key for table modifications has been written in the insert method. It was initially written inside the add_row method but that was making the load of the batting file very slow due to the number of checks required. Hence the code is now part of the insert method which is called by the update method as well - this ensures any inserts or updates do not allow for duplicates in the data. The implication of this design choice is that the load of the original file will not check for duplicates (it was confirmed by the instructors that the raw files will not contain any duplicates on the primary key field) - so this is a valid design choice and it allows testing on the batting file as well.
* In order to use the by key methods, a primary key has to have been passed during the initialization of the CSVDataTable
* The save method saves the updated CSV into the same source file if called. However, the insert, delete or update methods do not call the save method everytime they are invoked. Since the save method was set up to write to the same source file (as required by the instructors), I did not have the insert, delete or update methods call the save method, so as to not modify the source file. All modifications happen in memory as the script is currently set up. If required(for testing the script), the save method can be called in the same session. Calling the save method will write the modifications to the source file.
* Even though the primary keys of the tables have been set using MySQL Workbench, when initializing the RDBDataTable in the python script, the primary key fields of the table of interest still need to be passed. The constructor does not update the keys in the MySQL table but passing the keys allows the use of the by key methods. If the primary fields are not passed, the methods will not allow the use of the by key methods. This design was chosen so as to make sure the order of values passed to the by key methods follows the order of the column names passed to the constructor - this avoids confusion



Complete List of Notes (includes the design decisions listed above):

CSVDataTable:
* The constructor for the CSVDataTable initializes a list that holds the primary key values for all rows in the table (if the primary key columns are provided). This list is updated by the insert, delete and update methods as necessary and is used to ensure there are no duplicates based on primary keys in the table
* Due to the size of the files, the check for duplicates based on the primary key for table modifications has been written in the insert method. It was initially written inside the add_row method but that was making the load of the batting file very slow due to the number of primary key checks required. Hence the code is now part of the insert method which is called by the update method as well - this ensures any inserts or updates do not allow for duplicates in the data. The implication of this design choice is that the load of the original file will be much faster but the load will not check for duplicates (it was confirmed by the instructors that the raw files will not contain any duplicates on the primary key field) - so this is a valid design choice and it allows testing on the batting file as well.
* The by key methods all create a template and then call the by template methods. This is a better design that replicating similar code across different methods
* The save method saves the updated CSV into the same source file if called. However, the insert, delete or update methods do not call the save method everytime they are invoked. Since the save method was set up to write to the same source file (as required by the instructors), I did not have the insert, delete or update methods call the save method, so as to not modify the source file. All modifications happen in memory as the script is currently set up. If required(for testing the script), the save method can be called in the same session. Calling the save method will write the modifications to the source file.
* There are some additional helper methods that check for the validity of the template and also verify that the columns passed as arguments to different methods are all columns present in the data
* In order to use the by key methods, a primary key has to have been passed during the initialization of the CSVDataTable

RDBDataTable:
* The three files people, batting and appearances have been loaded into the MySQL Database and their primary keys have been set to ['playerID'], ['playerID', 'yearID', 'teamID', 'stint'] and ['playerID', 'yearID', 'teamID'] respectively using MySQL Workbench
* When initializing the RDBDataTable, the primary key fields still need to be passed. The constructor does not update the keys in the MySQL table but passing the keys allows the use of the by key methods. If the primary fields are not passed, the methods will not allow the use of the by key methods. This design was chosen so as to make sure the order of values passed to the by key methods follows the order of the column names passed to the constructor - this avoids confusion
* The methods added to the script includes template_to_where_clause, create_select, create_insert, create_delete and create_update. Prof. Ferguson's sample code was very useful when writing the scripts for the RDBDataTable
* Unlike in the CSVDataTable, the database does a lot of the integrity checks (like ensuring the primary key constraints are respected) and addresses incorrect usage. My script just converts the inputs to a SQL script that is then passed on to the MySQL database for execution. The results and errors returned are formatted and printed.


csv_table_tests.py and rdb_table_tests.py
* The two test files csv_table_tests.py and rdb_table_tests.py have various tests written to test the methods in the CSVDataTable and RDBDataTable
* The tests are run on two tables: people and batting (which has a composite Primary Key)
* The tests test the methods on different scenarios in order to ensure the methods work as expected
* The output of these tests are stored in csv_table_test.txt and rdb_table_test.txt respectively


