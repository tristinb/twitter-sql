
import sqlite3
import json
import ssl

import re

import tweepy
from tweepy import OAuthHandler


#### Set up SQL table #####

filename = input('filename? ')
while len(re.findall('\..+',filename))<1: # re finds an extension
    filename = input('include filename plus extension (i.e. .sqlite): ')

conn = sqlite3.connect(filename)
cur = conn.cursor()


# Time doesn't need to be unique (sometimes two tweets in one minute)
    # Don't know if there is a glitch in Twitter, but on site two tweets can come at the same time. Deal with unique during analysis
# Text also not unique, some only have minor differences (i.e., caps) and won't be caught. Deal with this in analysis

cur.executescript('''
DROP TABLE IF EXISTS Text;
DROP TABLE IF EXISTS TW_Name;
DROP TABLE IF EXISTS Time;
DROP TABLE IF EXISTS Tweet_ID;


CREATE TABLE Text (
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    text   TEXT,
    tw_name_id INTEGER
);

CREATE TABLE TW_Name (
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    tw_name  TEXT UNIQUE
);
CREATE TABLE Time (
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    time  TEXT
);

CREATE TABLE Tweet_ID (
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    tweet_id  TEXT UNIQUE
);


''')

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE




#### Get Twitter data ####

consumer_key = ''
consumer_secret = ''
access_token = ''
access_secret = ''

auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

twitter_name = input('Input twitter screen name: ')
print('twitter name is ', twitter_name)

tweets = api.user_timeline(screen_name=twitter_name,count=200)


oldest = []
for tweet in tweets:
    tw_name = tweet.author.screen_name
    time = tweet.created_at
    text = tweet.text
    tweet_id = tweet.id
    oldest.append(tweet_id)

    cur.execute('''INSERT OR IGNORE INTO TW_Name (tw_name)
        VALUES ( ? )''', ( tw_name, ) )
    cur.execute('SELECT id FROM TW_Name WHERE tw_name = ? ', (tw_name, ))
    tw_name_id = cur.fetchone()[0]

    cur.execute('''INSERT OR IGNORE INTO Time (time)
        VALUES ( ? )''', ( time, ) )

    cur.execute('''INSERT OR IGNORE INTO Text (text, tw_name_id)
    VALUES ( ?, ? )''', ( text, tw_name_id ) )

    cur.execute('''INSERT OR IGNORE INTO Tweet_ID (tweet_id)
    VALUES ( ? )''', ( tweet_id, ) )

    conn.commit()


while len(tweets)>0:
    print('Getting previous 200 tweets, from', time)
    last_tweet = oldest[-1] -1 # max_id includes last ID (take one off)
    tweets = api.user_timeline(screen_name=twitter_name,count=200,max_id=last_tweet)

    for tweet in tweets:
        tw_name = tweet.author.screen_name
        time = tweet.created_at
        text = tweet.text
        tweet_id = tweet.id
        oldest.append(tweet_id)

        cur.execute('''INSERT OR IGNORE INTO TW_Name (tw_name)
            VALUES ( ? )''', ( tw_name, ) )
        cur.execute('SELECT id FROM TW_Name WHERE tw_name = ? ', (tw_name, ))
        tw_name_id = cur.fetchone()[0]

        cur.execute('''INSERT OR IGNORE INTO Time (time)
            VALUES ( ? )''', ( time, ) )

        cur.execute('''INSERT OR IGNORE INTO Text (text, tw_name_id)
        VALUES ( ?, ? )''', ( text, tw_name_id ) )

        cur.execute('''INSERT OR IGNORE INTO Tweet_ID (tweet_id)
        VALUES ( ? )''', ( tweet_id, ) )

        conn.commit()

print('Done!')
