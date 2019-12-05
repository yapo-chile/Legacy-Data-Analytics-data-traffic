import json
import logging
import time
import sys
from optparse import OptionParser
import requests
import psycopg2
import environ
from config import AppConfig

CONFIG = environ.to_config(AppConfig)
LOGGER = logging.getLogger('api_leads')

def get_leads_data(xiti_headers, site, columns, xiti_filter, month_id, xiti_start, xiti_end):
    LOGGER.info('Starting to get data from Xiti API...')
    conn = ""
    try:
        conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (
            CONFIG.db.name, CONFIG.db.user, CONFIG.db.host, CONFIG.db.password))
    except Exception:
        LOGGER.error('Error connecting to database!')
        sys.exit(1)
    cur = conn.cursor()

    # url de xiti
    url_base = CONFIG.xiti.api_url_data
    columns = "%s" % columns
    sort = "%s" % CONFIG.xiti.sort
    xiti_filter = "%s" % xiti_filter
    space = "{s:%s}" % site
    xiti_period = "{D:{start:'%s',end:'%s'}}" % (xiti_start, xiti_end)
    max_results = 10000
    page_num = 1

    url = "%s?&columns=%s&sort=%s&filter=%s&space=%s&period=%s&max-results=%d&page-num=%d" % (
        url_base, columns, sort, xiti_filter, space, xiti_period, max_results, page_num)
    LOGGER.info('Xiti API GetData URL %s', url)

    response = requests.request("GET", url, headers=xiti_headers)
    LOGGER.info('GetData Request result %s', response)
    decoded = json.loads(response.text)
    rows = decoded["DataFeed"][0]["Rows"]

    LOGGER.info('GetData JSON decoded')
    for r in rows:
        category = columns[1:10]
        ad_reply_type = columns[11:20]
        query = """ insert into stg.fact_month_ad_reply_type_xiti
                        (month_id,
                        site,
                        category,
                        ad_reply_type,
                        visitors)
                    values
                        (%s,
                        %s,
                        %s,
                        %s,
                        %s)"""
        data = (month_id, site, r[category], r[ad_reply_type], r["m_visitors"])
        cur.execute(query, data)

    conn.commit()
    cur.close()
    conn.close()


def delete_entries(xiti_period):
    LOGGER.info('Connecting to database...')
    conn = ""
    try:
        conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (
            CONFIG.db.name,
            CONFIG.db.user,
            CONFIG.db.host,
            CONFIG.db.password))
    except Exception as e:
        LOGGER.error('Error connecting to database!')
        LOGGER.error('Error:\n%s', e)
        sys.exit(1)
    cur = conn.cursor()
    try:
        query = "delete from stg.fact_month_ad_reply_type_xiti where month_id = '%s'" % period
        cur.execute(query)
        LOGGER.info('Table deleted for period %s', period)

        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.errors.UndefinedTable:
        LOGGER.error('Table doesnt exits. skip delete')
        return


def get_params():
    parser = OptionParser('usage: %prog month start end')
    (_, command_params) = parser.parse_args()
    if len(params) != 3:
        LOGGER.info('ERROR: Missing params')
        parser.error('Missing params')

    return command_params


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO
    )
    LOGGER.info('Starting process at %s', time.strftime('%Y-%m-%d %H:%M:%S'))
    params = get_params()
    period = params[0]
    start = params[1]
    end = params[2]

    headers = {'authorization': "Basic %s" % CONFIG.xiti.authorization}

    LOGGER.info('Using the following period')
    LOGGER.info('Period: %s', period)

    delete_entries(period)

    # MSITE
    LOGGER.info('Starting to get data for msite platform')
    get_leads_data(headers, CONFIG.xiti.site_msite, CONFIG.xiti.columns_msite,
                   CONFIG.xiti.filter_msite, period, start, end)
    # ANDROID
    LOGGER.info('Starting to get data for android platform')
    get_leads_data(headers, CONFIG.xiti.site_android, CONFIG.xiti.columns_android,
                   CONFIG.xiti.filter_android, period, start, end)
    # IOS
    LOGGER.info('Starting to get data for ios platform')
    get_leads_data(headers, CONFIG.xiti.site_ios, CONFIG.xiti.columns_ios,
                   CONFIG.xiti.filter_ios, period, start, end)

    LOGGER.info('Ending process at %s', time.strftime('%Y-%m-%d %H:%M:%S'))
