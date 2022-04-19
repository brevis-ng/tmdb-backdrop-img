import json
import mysql.connector
from mysql.connector import errorcode
import tmdbsimple as tmdb
from datetime import datetime
import sys
import argparse


IMG_URL = 'https://image.tmdb.org/t/p/original'
OUTPUT_FILE = 'output.txt'
tmdb.API_KEY = '7ade87f75c757f82c658d23699f364a6'
tmdb.REQUESTS_TIMEOUT = 5

USER = ''
PW = ''
SERVER = ''
DB_NAME = ''
TIME = ''


def search_tmdb():
    date = datetime.strftime(datetime.now(), '%Y-%m-%d') if TIME == 'today' else ''
    name_lst = db_movie_name(date)
    search = tmdb.Search()
    result_dict = {}
    res = {}
    conn = connet(user=USER, pw=PW, host=SERVER, db=DB_NAME)
    for item in name_lst:
        id, name, remarks, pic = item[0], item[1], item[2], item[3]
        if 'HD' in remarks:
            res = search.movie(language='vi-VN', query=name, include_adult=True)
        elif 'tập' in remarks.lower():
            res = search.tv(language='vi-VN', query=name, include_adult=True)
        else:
            continue
        results = res['results']
        if results:
            result_dict[id] = {}
            result = results[0]
            backdrop_path = result['backdrop_path']
            result_dict[id]['name'] = name
            result_dict[id]['inguon_pic'] = pic
            result_dict[id]['tmdb_pic'] = f'{IMG_URL}{backdrop_path}'
            result_dict[id]['tmdb_content'] = result['overview']
            if backdrop_path is not None:
                update_now(id, result_dict[id]['tmdb_pic'], conn)
    return json.dumps(result_dict)


def connet(user, pw, host, db):
    conn = None
    try:
        conn = mysql.connector.connect(user=user, password=pw, host=host, database=db)
    except Exception as e:
        print('CONNECT ERROR')
    finally:
        return conn


def executemany(cursor, sql: str, params=None):
    if cursor is None:
        return
    try:
        cursor.executemany(sql, params)
        print(f'Number of rows affected by statement "{cursor.rowcount}"')
        cursor.close()
    except Exception as e:
        print(f'UPDATE ERROR: {e}')


def db_movie_name(date):
    conn = connet(user=USER, pw=PW, host=SERVER, db=DB_NAME)
    cursor = conn.cursor()
    where = f' AND from_unixtime(vod_time, "%Y-%m-%d") >= "{date}"' if date != '' else ''
    sql = f'SELECT vod_id, vod_name, vod_remarks, vod_pic FROM mac_vod WHERE vod_pic_slide = ""' + where
    cursor.execute(sql)
    result = cursor.fetchall()
    print(f'SQL: {cursor.statement}')
    print(f'Row count: {cursor.rowcount}')
    cursor.close()
    conn.close()
    return result


def update_now(vod_id, img_url, conn):
    sql = 'UPDATE mac_vod SET vod_pic_slide = %s WHERE vod_id = %s'
    data =(img_url, vod_id)
    cursor = conn.cursor()
    cursor.execute(sql, data)
    print(cursor.statement)
    cursor.close()


def update():
    now = datetime.strftime(datetime.now(), '%Y%m%d')
    json_file = None
    data = []
    with open(f'{now}_{OUTPUT_FILE}_{SERVER}', 'r') as f:
        json_file = json.load(f)
    for key, value in json_file.items():
        if value:
            data.append(tuple([value['tmdb_pic'], key]))
    print(data)
    sql = 'UPDATE mac_vod SET vod_pic_slide = %s WHERE vod_id = %s'
    conn = connet(user=USER, pw=PW, host=SERVER, db=DB_NAME)
    cursor = conn.cursor()
    executemany(cursor, sql, data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Update vod_pic_slide from TMDB.")
    parser.add_argument('--time', type=str, default='all', required=True, help='ALL, TODAY')
    parser.add_argument('--user', type=str, required=True, help='Database administrator')
    parser.add_argument('--pw', type=str, help='Database administrator password')
    parser.add_argument('--host', type=str, required=True, help='Database host')
    parser.add_argument('--db', type=str, required=True, help='Database name')
    args = parser.parse_args()
    USER = args.user
    PW = args.pw
    SERVER = args.host
    DB_NAME = args.db
    TIME = args.time.lower()

    json_str = search_tmdb()
    now = datetime.strftime(datetime.now(), '%Y%m%d')
    with open(f'{now}_{SERVER}_{OUTPUT_FILE}', 'w') as f:
        f.write(json_str)
    # update()
