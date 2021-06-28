# TechnicalTest

This project uses the libraries mysql.connector and pandas to trim, separate and push data from some csv files into a sql database.
To specify the sql connection parameters please edit the <code>config.json</code> file at the root folder.
To test the connection you can use an instance of the class <code>DBConnection</code>. You can also provide a different config file by setting the initial parameter <code>config_file=...</code>.

The class <code>FoodDBLoader</code> stablishes a connection, filters and loads the data.
Running the method <code>load_all_data()</code> will automatically load the main data, but each part of the data can be loaded independently.

### __* A multiprocessing version of the code is coming soon. *__
