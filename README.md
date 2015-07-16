# load-and-analyze
Load consumer complaint and ACS data, then "blend" and analyze

**Data Information**

This code base uses two different data sets. The data files consumed are included in the project, but for more information on the data please see below:

*Consumer Complaints*
* http://www.consumerfinance.gov/complaintdatabase/
* Headers are included with the downloaded data file


*American Community Survey (ACS)*
* ACS splits related data into separate files, headers are typically not included and can be found within the documenatation or a separate file. 
* Please read the technical documentation for more information: http://www2.census.gov/acs2013_5yr/summaryfile/ACS_2013_SF_Tech_Doc.pdf


**Instructions to run the program**

1 - Install PostgreSQL (If you wish to use another data store just amend the code appropriately)

3 - Export relevant environment variables
I export "sensitive" program information as environment variables in the code

2 - Install all packages in the "requirements.txt" file. I use a virtual environment to keep my project setup separate from my overall system setup. As long as pip is install the following command will work:
```
pip install -r requirements.txt
```

3 - Make sure the path to your data files is correct in settings.py

4 - Run the code:
```
python load_and_analyze.py
```
