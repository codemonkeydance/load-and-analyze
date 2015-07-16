import jinja2
import psycopg2
import os
import csv
import logging
logging.basicConfig(level=logging.INFO)


CONSUMER_COMPLAINTS_CREATE_SQL = "sql/create_consumer_complaints.sql"
CONSUMER_COMPLAINTS_CSV = "data/Consumer_Complaints.csv"

GEOTABLE_CREATE_SQL = "sql/create_g20135us.sql"
GEOTABLE_CSV = "data/g20135us.csv"

TMP_SEQ0015_CREATE_SQL = "sql/create_tmp_seq0015.sql"
TMP_SEQ0015_CSV = "data/e20135us0015000_header.csv"

#ESTIMATE_INCOME_DATA = "..data/e20135us0015000.txt"


def get_postgres_conn():
    # setup connection with env vars
    username = os.environ['PGUSER']
    password = os.environ['PGPASSWORD']
    database = os.environ['DB_NAME']
    hostname = os.environ['DB_HOST']
    return psycopg2.connect(
        database=database,
        user=username,
        password=password,
        host=hostname
    )


def load_data(path_to_csv, path_to_sql, table_name, text_file=False):
    """
    load consumer complaint csv to postgres
    """

    if text_file:
        f = open(ESTIMATE_INCOME_DATA, 'rb')
        #reader = csv.reader(f)
        csv_data = csv.reader(f)
        headers = csv_data.next()
    else:
        csv_data = open(path_to_csv, 'r')

    conn = get_postgres_conn()
    curs = conn.cursor()

    # if consumer complaints database doesn't exist load it
    try:
        curs.execute(open(path_to_sql, "r").read())
        sql_statement = "COPY {} FROM STDIN WITH CSV HEADER DELIMITER AS ','".format(table_name)
        curs.copy_expert(sql=sql_statement, file=csv_data)
    except psycopg2.Error as e:
        logging.error("Issue executing sql '{}' to postgres".format(sql_statement))
        logging.error(e)

        if conn:
            conn.rollback()

    conn.commit()

    curs.close()
    csv_data.close()
    conn.close()


def create_report():
    """
    generate "mixed media" html report based on meaningful queries against data
    """

    conn = get_postgres_conn()
    curs = conn.cursor()

    # view joining zcta5 with B06010001 - B06010003 (total, no income, with income)
    view_geodata_estimate = """create materialized view zcta5_B06010001_3 as
        select g.logrecno, g.zcta5, t.B06010001, t.B06010002, t.B06010003
        from g20135us as g, tmp_seq0015 as t where g.logrecno = t.logrecno;"""

    # first create any views necessary for report
    try:
        curs.execute(view_geodata_estimate)
        conn.commit()
        logging.info("Check to see if view was correctly created!")
    except psycopg2.Error as e:
        logging.error(e)

    count_of_joined_data = """select count(*)
        from consumer_complaints as cc, g20135us as g, tmp_seq0015 as tmp
        where cc.zip = g.zcta5 and g.logrecno = tmp.logrecno;"""

    # zip_codes_with_most_complaints = """select MAX(f.num), f.zip from
    #     (select count(*) as num, zip from consumer_complaints as cc, g20135us as g
    #     where cc.zip = g.zcta5 group by cc.zip) as f group by f.zip, f.num
    #     order by f.num desc limit 20;"""

    # max complaints sorted by zip code against total Population 15 years and over in the United States (B06010_001)
    max_complaints_by_zip_total = """select MAX(f.num), f.zip, f.B06010001 from
        (select count(*) as num, zip, B06010001 from consumer_complaints as cc, zcta5_B06010001_3 as zb
        where cc.zip = zb.zcta5 group by cc.zip, zb.B06010001) as f
        group by f.B06010001, f.zip, f.num order by f.num desc limit 20;"""

    min_complaints_by_zip_total = """select MIN(f.num), f.zip, f.B06010001 from
        (select count(*) as num, zip, B06010001 from consumer_complaints as cc, zcta5_B06010001_3 as zb
        where cc.zip = zb.zcta5 group by cc.zip, zb.B06010001) as f
        group by f.B06010001, f.zip, f.num order by f.num asc limit 20;"""


    try:
        curs.execute(max_complaints_by_zip_total)
        columns = [desc[0] for desc in curs.description]
        result = curs.fetchall()
        logging.info("********\n{}\n{}".format(columns, result))
    except psycopg2.Error as e:
        logging.error(e)
    #
    # template = jinja2.Template = ("""
    #     <html>
    #     <body>
    #
    #     </body>
    #     </html>
    # """)


if __name__ == "__main__":
    # load tables into postgres
    logging.info("Loading tables, if they don't currently exist, into Postges DB:\n")
    logging.info("Loading the consumer complaints table...")
    load_data(CONSUMER_COMPLAINTS_CSV, CONSUMER_COMPLAINTS_CREATE_SQL, "consumer_complaints")
    logging.info("Done loading consumer complaints table!\n")

    logging.info("Loading estimate table 'tmp_seq0015'...")
    load_data(TMP_SEQ0015_CSV, TMP_SEQ0015_CREATE_SQL, "tmp_seq0015")
    logging.info("Done loading tmp_seq0015 table!\n")

    logging.info("Loading geography table 'g20135us'...")
    load_data(GEOTABLE_CSV, GEOTABLE_CREATE_SQL, "g20135us")
    logging.info("Done loading g20135us table!\n")

    # create the report template in html
    create_report()

    #TODO: Create table with just the sequence 15 data



