import tweepy as tw
import psycopg2


def create_twitter_table(conn):
    CONSUMER_KEY = ""
    CONSUMER_SECRET = ""
    ACCESS_TOKEN = ""
    ACCESS_SECRET = ""

    auth = tw.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

    api = tw.API(auth, wait_on_rate_limit=True)

    search_words = ["suicidal", "suicide", "kill myself", "my suicide note",
                    "my suicide letter", "end my life", "never wake up",
                    "can't go on", "not worth living", "ready to jump", "sleep forever",
                    "want to die", "be dead", "better off without me", "better off dead",
                    "suicide plan", "tired of living", "don't want to be here", "die alone", "go to sleep forever"]

    new_attr = {}
    for word in search_words:
        tweets = tw.Cursor(api.search, q=word,
                           lang="en", tweet_mode="extended").items()

        for tweet in tweets:
            data = tweet._json
            attr_list = ["created_at", "id", "full_text", "truncated", "display_text_range",
                         "source", "in_reply_to_status_id", "in_reply_to_user_id", "in_reply_to_screen_name",
                         "contributors", "is_quote_status", "possibly_sensitive",
                         "quoted_status_id", "quoted_status"]
            place = ["country", "country_code", "full_name", "id", "name", "place_type", "url"]
            for value in attr_list:
                if value not in data:
                    new_attr[value] = "NA"

            if "hastags" not in data["entities"]:
                new_attr["hastags"] = "NA"

            if "location" not in data["user"]:
                new_attr["location"] = "NA"

            if "followers_count" not in data["user"]:
                new_attr["followers_count"] = "NA"

            for key, value in data["user"].items():
                if key == "screen_name":
                    new_attr["screen_name"] = value

            for value in place:
                if value not in data:
                    new_attr[value] = "NA"

            for value in attr_list:
                if value not in new_attr:
                    new_attr[value] = data[value]

            data_insert_live = (new_attr["created_at"], new_attr["id"],
                                str(new_attr["possibly_sensitive"]), str(new_attr["quoted_status_id"]),
                                str(new_attr["quoted_status"]), str(new_attr["hastags"]), str(new_attr["screen_name"]),
                                str(new_attr["country"]), str(new_attr["country_code"]), str(new_attr["full_name"]),
                                str(new_attr["name"]), str(new_attr["place_type"]), str(new_attr["url"]),
                                str(new_attr["full_text"]), str(new_attr["truncated"]),
                                str(new_attr["display_text_range"]),
                                str(new_attr["source"]), str(new_attr["in_reply_to_status_id"]),
                                str(new_attr["in_reply_to_user_id"]), str(new_attr["in_reply_to_screen_name"]),
                                str(new_attr["contributors"]), str(new_attr["is_quote_status"]))

            cur = conn.cursor()
            cur.execute("INSERT INTO liveTwitterData VALUES (%s, %s, %s, %s, %s, %s, %s, %s,\
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", data_insert_live)
            conn.commit()

            data_insert = (str(new_attr["screen_name"]), str(new_attr["country"]),
                           new_attr["id"], new_attr["created_at"],
                           str(new_attr["full_text"]))
            cur = conn.cursor()
            cur.execute("INSERT INTO twitterData_source_2 VALUES (%s, %s, %s, %s, %s)", data_insert)
            conn.commit()


def merge_data(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE merged_twitter_data AS\
                SELECT t1.id,\
                t1.created_at,\
                t1.possibly_sensitive,\
                t1.quoted_status_id,\
                t1.quoted_status,\
                t1.hastags,\
                t1.screen_name,\
                t1.country,\
                t1.full_name,\
                t1.url,\
                t1.truncated,\
                t1.source,\
                t1.in_reply_to_status_id,\
                t1.in_reply_to_user_id,\
                t1.is_quote_status,\
                t1.full_text\
                FROM liveTwitterData as t1\
                FULL OUTER JOIN twitterData_source_2 as t2 ON t1.id = t2.twitter_id")
    conn.commit()


def create_table_live_date(conn):
    cur = conn.cursor()
    cur.execute("CREATE TABLE liveTwitterData(created_at DATE,\
    id BIGINT,\
    possibly_sensitive VARCHAR,\
    quoted_status_id VARCHAR,\
    quoted_status VARCHAR,\
    hastags VARCHAR,\
    screen_name VARCHAR,\
    country VARCHAR,\
    country_code VARCHAR,\
    full_name VARCHAR,\
    name VARCHAR,\
    place_type VARCHAR,\
    url VARCHAR,\
    full_text VARCHAR,\
    truncated VARCHAR,\
    display_text_range VARCHAR,\
    source VARCHAR,\
    in_reply_to_status_id VARCHAR,\
    in_reply_to_user_id VARCHAR,\
    in_reply_to_screen_name VARCHAR,\
    contributors VARCHAR,\
    is_quote_status VARCHAR);")

    cur.execute("CREATE TABLE twitterData_source_2(username VARCHAR,\
        country_name VARCHAR,\
        twitter_id BIGINT,\
        time DATE,\
        tweet VARCHAR);")
    conn.commit()


def main():
    try:
        conn = psycopg2.connect(host="localhost", database="BDA", user="postgres", password="Logic0Error!")
        create_table_live_date(conn)
        create_twitter_table(conn)
        merge_data(conn)
    except(Exception, psycopg2.DatabaseError) as error:
        print("Error in transaction\nReverting all other operations. ", error)
        conn.rollback()
    finally:
        if conn is not None:
            conn.close()


# Calling the main function
if __name__ == '__main__':
    main()

