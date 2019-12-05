############################################
# coding : utf-8
import requests
import psycopg2
import json
import os
import logging
import time
import environ
from optparse import OptionParser
from config import AppConfig

config = environ.to_config(AppConfig)
logger = logging.getLogger('api_leads')


def main():
    logging.basicConfig(
        level=logging.INFO
    )
    logger.info('Starting process at %s' % time.strftime('%Y-%m-%d %H:%M:%S'))
    params = get_params()
    period = params[0]
    start = params[1]
    end = params[2]

    headers = {'authorization': "Basic %s" % config.xiti.authorization}

    logger.info('Using the following period')
    logger.info('Period: %s' % period)

    delete_table(period)

    # MSITE
    logger.info('Starting to get data for msite platform')
    get_leads_data(headers, config.xiti.site_msite, config.xiti.columns_msite,
                   config.xiti.filter_msite, period, start, end)
    # ANDROID
    logger.info('Starting to get data for android platform')
    get_leads_data(headers, config.xiti.site_android, config.xiti.columns_android,
                   config.xiti.filter_android, period, start, end)
    # IOS
    logger.info('Starting to get data for ios platform')
    get_leads_data(headers, config.xiti.site_ios, config.xiti.columns_ios,
                   config.xiti.filter_ios, period, start, end)

    logger.info('Ending process at %s' % time.strftime('%Y-%m-%d %H:%M:%S'))


def delete_table(period):
    logger.info('Connecting to database...')
    conn = ""
    try:
        conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (
            config.db.name,
            config.db.user,
            config.db.host,
            config.db.password))
    except Exception as e:
        logger.error('Error connecting to database!')
        logger.error('Error:\n%s', e)
        exit(1)
    cur = conn.cursor()
    try:
        query = "delete from stg.fact_month_ad_reply_type_xiti where month_id = '%s'" % period
        cur.execute(query)
        logger.info('Table deleted for period %s' % period)

        conn.commit()
        cur.close()
        conn.close()
    except psycopg2.errors.UndefinedTable:
        logger.warn('Table doesnt exits. skip delete')
        return


def get_leads_data(headers, site, columns, filter, month_id, start, end):
    logger.info('Starting to get data from Xiti API...')
    conn = ""
    try:
        conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (
            config.db.name, config.db.user, config.db.host, config.db.password))
    except Exception:
        logger.error('Error connecting to database!')
        exit
    cur = conn.cursor()

    # url de xiti
    url_base = config.xiti.api_url_data
    columns = "%s" % columns
    sort = "%s" % config.xiti.sort
    filter = "%s" % filter
    space = "{s:%s}" % site
    period = "{D:{start:'%s',end:'%s'}}" % (start, end)
    max_results = 10000
    page_num = 1

    url = "%s?&columns=%s&sort=%s&filter=%s&space=%s&period=%s&max-results=%d&page-num=%d" % (
        url_base, columns, sort, filter, space, period, max_results, page_num)
    logger.info('Xiti API GetData URL %s' % url)

    response = requests.request("GET", url, headers=headers)
    logger.info('GetData Request result %s' % response)
    decoded = json.loads(response.text)
    rows = decoded["DataFeed"][0]["Rows"]

    logger.info('GetData JSON decoded')
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


def get_params():
    parser = OptionParser('usage: %prog month start end')
    (options, params) = parser.parse_args()
    if len(params) != 3:
        logger.info('ERROR: Missing params')
        parser.error('Missing params')

    return params


main()
