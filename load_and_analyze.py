import jinja2
import psycopg2
import os
import csv
import settings
import logging
logging.basicConfig(level=logging.INFO)


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
    Generate "mixed media" html report based on meaningful queries against data
    Add any queries for report here and add them to the template
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

    # min complaints sorted by zip code against total Population 15 years and over in the United States (B06010_001)
    min_complaints_by_zip_total = """select MIN(f.num), f.zip, f.B06010001 from
        (select count(*) as num, zip, B06010001 from consumer_complaints as cc, zcta5_B06010001_3 as zb
        where cc.zip = zb.zcta5 group by cc.zip, zb.B06010001) as f
        group by f.B06010001, f.zip, f.num order by f.num asc limit 20;"""

    try:
        curs.execute(count_of_joined_data)
        total_join_count = curs.fetchone()
    except psycopg2.Error as e:
        logging.error(e)

    try:
        curs.execute(max_complaints_by_zip_total)
        max_columns = [desc[0] for desc in curs.description]
        max_result = curs.fetchall()
    except psycopg2.Error as e:
        logging.error(e)

    try:
        curs.execute(min_complaints_by_zip_total)
        min_columns = [desc[0] for desc in curs.description]
        min_result = curs.fetchall()
    except psycopg2.Error as e:
        logging.error(e)

    template = jinja2.Template("""
        <h2>Report for Consumer Complaints and ACS Data</h2>

        <p>Total count of the joined data set by zip for report: Consumer Complaints and ACS:</p>
        <strong>{{count}}</strong>

        <p>max complaints sorted by zip code against total Population 15 years and over in the United States (B06010_001)</p>
        <table border=1>
            <tr>
                <th>{{max_columns[0]}}</th>
                <th>{{max_columns[1]}}</th>
                <th>{{max_columns[2]}}</th>
            </tr>
            {% for row in max_result %}
                <tr>
                    <td>{{row[0]}}</td>
                    <td>{{row[1]}}</td>
                    <td>{{row[2]}}</td>
                </tr>
            {% endfor %}
        </table>

        <p>max complaints sorted by zip code against total Population 15 years and over in the United States (B06010_001)</p>
        <table border=1>
            <tr>
                <th>{{min_columns[0]}}</th>
                <th>{{min_columns[1]}}</th>
                <th>{{min_columns[2]}}</th>
            </tr>
            {% for row in min_result %}
                <tr>
                    <td>{{row[0]}}</td>
                    <td>{{row[1]}}</td>
                    <td>{{row[2]}}</td>
                </tr>
            {% endfor %}
        </table>
    """)

    report = template.render(max_columns=max_columns, max_result=max_result, min_columns=min_columns, min_result=min_result, count=total_join_count)


    writer = open(settings.REPORT_NAME, 'w')
    writer.write(report)
    writer.close()

if __name__ == "__main__":
    # load tables into postgres
    logging.info("Loading tables, if they don't currently exist, into Postges DB:\n")

    logging.info("Loading the consumer complaints table...")
    load_data(settings.CONSUMER_COMPLAINTS_CSV, settings.CONSUMER_COMPLAINTS_CREATE_SQL, "consumer_complaints")
    logging.info("Done loading consumer complaints table!\n")

    logging.info("Loading estimate table 'tmp_seq0015'...")
    load_data(settings.TMP_SEQ0015_CSV, settings.TMP_SEQ0015_CREATE_SQL, "tmp_seq0015")
    logging.info("Done loading tmp_seq0015 table!\n")

    logging.info("Loading geography table 'g20135us'...")
    load_data(settings.GEOTABLE_CSV, settings.GEOTABLE_CREATE_SQL, "g20135us")
    logging.info("Done loading g20135us table!\n")

    # create the report template in html
    logging.info("Generate the html report...")
    create_report()



