# load-and-analyze
Load consumer complaint and ACS data, then "blend" and analyze


**Reference Sources**

Leveraged and edited pre-existing create table sql scripts from the following repository:
* https://github.com/leehach/census-postgres/tree/master/acs2011_5yr

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

2 - Export relevant environment variables

I export "sensitive" program information as environment variables in the code. A helper script is included in the project, if you choose to use it please change the appropriate variables (password etc.) then run:
```
source initialize_env_vars.sh
```

3 - Install all packages in the "requirements.txt" file. I use a virtual environment to keep my project setup separate from my overall system setup. As long as pip is install the following command will work:
```
pip install -r requirements.txt
```

4 - Make sure the path to your data files is correct in settings.py

5 - Run the code:
```
python load_and_analyze.py
```
