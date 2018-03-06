# CSV to DB
A tool to programmatically upload CSVs to a Postgres Database

This short python tool leverages Pandas to read in a CSV file and identify column datatypes. It will load the CSV file into memory and import the file into the database. The largest file tested was 6gb. 
