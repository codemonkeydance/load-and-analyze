import psycopg2
import os
import csv
import logging
logging.basicConfig(level=logging.INFO)


CONSUMER_COMPLAINTS_CREATE_SQL = "sql/create_consumer_complaints.sql"
CONSUMER_COMPLAINTS_CSV = "../data/Consumer_Complaints.csv"

GEOTABLE_CREATE_SQL = "sql/create_g20135us.sql"
GEOTABLE_CSV = "../data/g20135us.csv"

TMP_SEQ0015_CREATE_SQL = "sql/create_tmp_seq0015.sql"
TMP_SEQ0015_CSV = "../data/e20135us0015000_header.csv"

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


# def load_consumer_data():
#     """
#     load consumer complaint csv to postgres
#     """
#
#     # first read header file and create table with those fields
#     # f = open(CONSUMER_COMPLAINTS_DATA_PATH, 'rb')
#     # reader = csv.reader(f)
#     # headers = reader.next()
#
#     consumer_complaints_data = open(CONSUMER_COMPLAINTS_CSV, 'r')
#
#     conn = get_postgres_conn()
#     curs = conn.cursor()
#
#     # if consumer complaints database doesn't exist load it
#     try:
#         curs.execute(open(CONSUMER_COMPLAINTS_CREATE_SQL, "r").read())
#         sql_statement = "COPY geotable FROM STDIN WITH CSV HEADER DELIMITER AS ','"
#         curs.copy_expert(sql=sql_statement, file=consumer_complaints_data)
#     except Exception:
#         logging.error("Issue executing sql '{}' to postgres".format(sql_statement))
#
#     conn.commit()
#     curs.close()
#
#     consumer_complaints_data.close()
#     conn.close()


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


if __name__ == "__main__":
    # load tables into postgres
    logging.info("Loading tables, if they don't currently exist, into Postges DB:\n")
    logging.info("Loading consumer complaints table...")
    load_data(CONSUMER_COMPLAINTS_CSV, CONSUMER_COMPLAINTS_CREATE_SQL, "consumer_complaints")
    logging.info("Done loading consumer complaints table!\n")

    logging.info("Loading tmp_seq0015 table...")
    load_data(TMP_SEQ0015_CSV, TMP_SEQ0015_CREATE_SQL, "tmp_seq0015")
    logging.info("Done loading tmp_seq0015 table!\n")

    logging.info("Loading g20135us table...")
    load_data(GEOTABLE_CSV, GEOTABLE_CREATE_SQL, "g20135us")
    logging.info("Done loading g20135us table!\n")

    #TODO: Create table with just the sequence 15 data



