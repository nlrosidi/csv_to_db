# CSV to DB
A tool to programmatically upload CSVs to a Postgres Database

This short python tool leverages Pandas to read in a CSV file and identify column datatypes. It will load the CSV file into memory and import the file into the database. I've added some data handling components to minimize errors when uploading.

- converted encoding to utf-8
- converted all capital letters in the header to lower case
- removed spaces and special symbols in the header and replaced with _ 
- e.g., a header "Sample Header" will be renamed to "sample_header" to ensure proper import into the db

The largest file tested was 6gb. 
