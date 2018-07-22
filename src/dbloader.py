import json
import sqlite3
from sentiment import sentiment, sentiment_lib

JSON_FILE = "../resources/three_minutes_tweets.json"

con = sqlite3.connect(":memory:")

# Obtain a cursor object
curTweet = con.cursor()

# Create a table in the in-memory database
createSDBTable = "CREATE TABLE IF NOT EXISTS Tweet(tweet_id integer primary key, name varchar(200), tweet_text text, country_code varchar(3), display_url text, lang varchar(3), created_at datetime, location varchar(100))"
addColumnTweetSentiment = "ALTER TABLE Tweet ADD COLUMN tweet_sentiment"
curTweet.execute(createSDBTable)
###### LOAD DATA TO SDB ######

with open(JSON_FILE) as f:
    for line in f:
        data = json.loads(line, "utf=8")
        # get data from json
        if 'user' in data:
            user = data.get('user')
            if user:
                name = user.get('name')
                display_url = user.get('url')
                lang = user.get('lang')
                location = user.get('location')
                tweet_text = data.get('text')
                country_code = data.get('country_code')
                created_at = data.get('created_at')
                curTweet.execute("insert into Tweet(name, tweet_text, country_code, display_url, lang, created_at, location) values(?,?,?,?,?,?,?)", (name, tweet_text, country_code, display_url, lang, created_at, location));
    con.commit()

    curTweet.execute(addColumnTweetSentiment)
###### LOAD DATA TO IDB. DATA NORMALIZATION ######
    createIDBUserTable = "CREATE TABLE IF NOT EXISTS User(user_id integer primary key, name varchar(200), display_url text)"
    createIDBTweetDataTable = "CREATE TABLE IF NOT EXISTS TweetData(tweet_id integer primary key, user_id integer, tweet_text text, country_code varchar(3), lang varchar(3), created_at datetime, location varchar(100))"

    curUser = con.cursor()
    curTweetData = con.cursor()

    curUser.execute(createIDBUserTable)
    curTweetData.execute(createIDBTweetDataTable)

    curTweet.execute('SELECT * FROM Tweet')
    for row in curTweet:
        curUser.execute("INSERT INTO User(name, display_url) VALUES(?,?)", (row[1], row[4]))
        curTweetData.execute("INSERT INTO TweetData(user_id, tweet_text, country_code, lang, created_at, location) values(?,?,?,?,?,?)", (curUser.lastrowid, row[2], row[3], row[5], row[6], row[7]))

con.close()
