import sqlite3
import requests
from pandas import json_normalize
import pandas as pd

def fetch_data(api_url):
    headers = {
        "x-rapidapi-key": "d2182d4965msh39ca4d4ec1ac01ep1a4ea0jsn5025785fa223",
        "x-rapidapi-host": "imdb188.p.rapidapi.com"
    }
    response = requests.get(api_url, headers=headers)
    
    return response


def search_IMDB():
    response = fetch_data("https://imdb188.p.rapidapi.com/api/v1/searchIMDB")
    data = json_normalize(response.json()['data'])
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS imdb_data_api (
            id TEXT PRIMARY KEY,
            qid TEXT,
            title TEXT,
            year INTEGER,
            stars TEXT,
            q TEXT,
            image TEXT
        )
    ''')
    
    for _, row in data.iterrows():
        cursor.execute('''
            INSERT OR IGNORE INTO imdb_data_api (id, qid, title, year, stars, q, image) 
            VALUES (?, ?, ?, ?, ?, ?, ?)''', 
            (
                row.get('id', None),
                row.get('qid', None),
                row.get('title', None),
                row.get('year', None),
                row.get('stars', None),
                row.get('q', None),
                row.get('image', None)
            ))

    conn.commit()


def get_week_top_10():
    response = fetch_data("https://imdb188.p.rapidapi.com/api/v1/getWeekTop10")
    data = json_normalize(response.json()['data'])

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS week_top_10 (
            id TEXT PRIMARY KEY,
            isAdult BOOLEAN,
            canRateTitle BOOLEAN,
            originalTitleText TEXT,
            primaryImage TEXT,
            aggregateRating FLOAT,
            voteCount FLOAT,       
            releaseYear INTEGER,
            titleText TEXT,
            titleType TEXT,
            series TEXT,
            plot TEXT,
            releaseDate TEXT,
            titleCertificate TEXT,
            titleRuntime INTEGER,
            chartMeterRanking INTEGER
        )
    ''')

    
    for _, row in data.iterrows():
        #Convert the date and time to a datetime format
        release_date_str = str(row.get("releaseDate.day")) + '-' + str(row.get("releaseDate.month")) + '-' + str(row.get("releaseDate.year"))
        release_date_pd = pd.to_datetime(release_date_str, format='%d-%m-%Y')
        release_date = release_date_pd.strftime('%d-%m-%Y')
        
        cursor.execute('''
            INSERT OR IGNORE INTO week_top_10 (id, isAdult, canRateTitle, originalTitleText, primaryImage, aggregateRating, voteCount, releaseYear, titleText, titleType, series, plot, releaseDate, titleCertificate, titleRuntime, chartMeterRanking) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (
                row.get('id', None),
                row.get('isAdult', None),
                row.get('canRateTitle.isRatable', None),
                row.get('originalTitleText.text', None),
                row.get('primaryImage.imageUrl', None),
                row.get('ratingsSummary.aggregateRating', 0),
                row.get('ratingsSummary.voteCount', 0),
                row.get('releaseYear.year', None),
                row.get('titleText.text', None),
                row.get('titleType.text', None),
                row.get('series', None),
                row.get('plot.plotText.plainText', None),
                release_date,
                row.get('titleCertificate.rating', None),
                row.get('titleRuntime.seconds', 0),
                row.get('chartMeterRanking.currentRank', None),
            ))
            
    #Calculate the KIP's 
    kpi = data.agg({
        'ratingsSummary.aggregateRating': lambda x: x.mean() if x.notna().any() else 0,
        'ratingsSummary.voteCount': lambda x: x.sum() if x.notna().any() else 0
    })
    print(kpi)
    conn.commit()


def get_fan_favorites():
    response = fetch_data("https://imdb188.p.rapidapi.com/api/v1/getFanFavorites")
    data = json_normalize(response.json()['data']['list'])

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fan_favorites (
            id TEXT PRIMARY KEY,
            isAdult BOOLEAN,
            canRateTitle BOOLEAN,
            originalTitleText TEXT,
            primaryImage TEXT,
            aggregateRating FLOAT,
            voteCount FLOAT,       
            releaseYear INTEGER,
            titleText TEXT,
            titleType TEXT,
            series TEXT,
            plot TEXT,
            releaseDate TEXT,
            titleCertificate TEXT,
            titleRuntime INTEGER
        );
    ''')

    for _, row in data.iterrows():
        #Convert the date and time to a datetime format
        release_date_str = str(row.get("releaseDate.day")) + '-' + str(row.get("releaseDate.month")) + '-' + str(row.get("releaseDate.year"))
        release_date_pd = pd.to_datetime(release_date_str, format='%d-%m-%Y')
        release_date = release_date_pd.strftime('%d-%m-%Y')

        cursor.execute('''
            INSERT OR IGNORE INTO fan_favorites (id, isAdult, canRateTitle, originalTitleText, primaryImage, aggregateRating, voteCount, releaseYear, titleText, titleType, series, plot, releaseDate, titleCertificate, titleRuntime) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (
                row.get('id', None),
                row.get('isAdult', None),
                row.get('canRateTitle.isRatable', None),
                row.get('originalTitleText.text', None),
                row.get('primaryImage.imageUrl', None),
                row.get('ratingsSummary.aggregateRating', 0),
                row.get('ratingsSummary.voteCount', 0),
                row.get('releaseYear.year', None),
                row.get('titleText.text', None),
                row.get('titleType.text', None),
                row.get('series', None),
                row.get('plot.plotText.plainText', None),
                release_date,
                row.get('titleCertificate.rating', None),
                row.get('titleRuntime.seconds', 0),
            ))

    conn.commit()

#Generating Report 
def generate_report():
    query = '''
    SELECT f.id, f.titleText, f.releaseYear, f.aggregateRating, f.voteCount, f.titleType
    FROM fan_favorites f
    WHERE f.aggregateRating > 0
    ORDER BY f.aggregateRating DESC
    LIMIT 10;
    '''

    df = pd.read_sql_query(query, conn)
    df.to_excel('fan_top_10.xlsx', index=False)
    print("Report saved as fan_top_10.xlsx")


conn = sqlite3.connect('imdb_data_api.db')
cursor = conn.cursor()
#GET data from 3 different enpoints
search_IMDB()
get_week_top_10()
get_fan_favorites()
generate_report()
cursor.close()
conn.close()