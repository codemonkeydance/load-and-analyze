# load-and-analyze
Load consumer complaint and ACS data, then "blend" and analyze

1 - Install postgreSQL

2 - Install all packages in the "requirements.txt" file. I use a virtual environment to keep my project setup separate from my overall system setup. As long as pip is install the following command will work:
```
pip install -r requirements.txt
```

3 - Make sure the path to your data files is correct in settings.py

4 - Run the code:
```
python load_and_analyze.py
```
