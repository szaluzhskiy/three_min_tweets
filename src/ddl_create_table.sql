CREATE TABLE IF NOT EXISTS Tweet (
 id integer PRIMARY KEY,
 name text,
 tweet_text text,
 country_code text,
 display_url text,
 lang text,
 created_at datetime,
 location text
);
