import os
import sys
import ast
import json
import time
import pika
import redis
import psycopg
import logging
import logging
import numpy as np
import pandas as pd
from psycopg import sql
from openai import OpenAI
from typing import Dict, Any
from dotenv import load_dotenv
from psycopg.rows import dict_row
from dateutil.parser import isoparse
from datetime import datetime, timedelta

load_dotenv()

logger = logging.getLogger('tagger')
logging.basicConfig(level=logging.INFO)

#rabbitmq
rabbitmq_host = os.getenv('RABBITMQ_HOST')
rabbitmq_user = os.getenv('RABBITMQ_USER')
rabbitmq_password = os.getenv('RABBITMQ_PASS')
rabbitmq_port = int(os.getenv('RABBITMQ_PORT'))

def create_connection():
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            rabbitmq_host, 
            rabbitmq_port, 
            '/', 
            pika.PlainCredentials(rabbitmq_user, rabbitmq_password), 
            heartbeat=600, 
            blocked_connection_timeout=300
        )
    )

# Point to the local server
llm_client = OpenAI(tag_5_url=os.getenv('LOCALAI_ENDPOINT'), api_key=os.getenv('LOCALAI_API_KEY'))

# Datatag_5 connection parameters
# - Declare the connection as a global variable
connection = None

def initialize_connection():
    """
    Initializes the global datatag_5 connection.
    """
    global connection
    try:
        connection = psycopg.connect(
            dbname=os.getenv('POSTGRESQL_DB_NAME'),
            user=os.getenv('POSTGRESQL_USER'),
            password=os.getenv('POSTGRESQL_PASSWORD'),
            host=os.getenv('POSTGRESQL_HOST'),
            port=os.getenv('POSTGRESQL_PORT')
        )
        logger.info("Datatag_5 connection initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize datatag_5 connection: {e}")
        connection = None

def get_connection():
    """
    Returns the global datatag_5 connection.
    If the connection is not initialized, it raises an error.
    """
    if connection is None:
        raise Exception("Datatag_5 connection is not initialized. Call 'initialize_connection' first.")
    return connection

def close_connection():
    """
    Closes the global datatag_5 connection.
    """
    global connection
    if connection is not None:
        connection.close()
        logger.info("Datatag_5 connection closed.")
        connection = None
    else:
        logger.warning("No active datatag_5 connection to close.")

#-------------------------- CUSTOM SQL FUNCTIONS ----------------------------
# Function for executing Queries
def Send_query_to_DB_silent(query, params=None):
    try:
        #close_old_connections()
        connection = get_connection()
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            data = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(data, columns=col_names)
            return df
    except Exception as error:
        logger.error(f"Error while running a query: {error}")
        return pd.DataFrame()

# helper function for getting names from the table in the db
def get_table_columns(table_name):
    """
    Retrieves column names from the specified table.

    Parameters:
    - cursor (psycopg.Cursor): Active psycopg cursor.
    - table_name (str): Name of the table.

    Returns:
    - List of column names.
    """
    query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE LOWER(table_name) = LOWER(%s)
        ORDER BY ordinal_position
    """
    try:
        # Log the query and the table name
        logger.debug(f"Executing query to retrieve columns for table: {table_name}")

        # Execute the query
        result = Send_query_to_DB_silent(query, [table_name])
        # Log the raw result for debugging
        logger.debug(f"Result from information_schema for table '{table_name}': {result}")

        # Check if the result is empty
        if result.empty:
            logger.error(f"No columns found for table '{table_name}' in information_schema.")
            raise ValueError(f"No columns found for table '{table_name}' in information_schema.")

        # Extract column names and log them
        columns = result['column_name'].tolist()  # Access dictionary keys
        logger.debug(f"Columns retrieved for table '{table_name}': {columns}")
        
        return columns

    except Exception as e:
        logger.exception(f"Failed to get columns for table '{table_name}': {e}")
        raise

def insert_df_to_table(df, table_name):
    """
    Inserts data from a DataFrame into the specified PostgreSQL table with hybrid conflict handling.
    If an error occurs, it attempts to identify the exact column and value causing the error.
    """
    try:
        # Define conflict handling configurations per table
        conflict_config = {
            'table_1': {
                'conflict_fields': ['column_1'],
                'update_fields': ['column_2', 'column_3', 'column_4'],
                'special_handling': None
            },
            'table_2': {
                'conflict_fields': ['column_5'],
                'update_fields': ['column_6', 'column_7'],
                'special_handling': {
                    'column_6': sql.SQL("COALESCE(EXCLUDED.column_6, {table}.column_6)").format(table=sql.Identifier(table_name))
                }
            },
            'table_3': {
                'conflict_fields': ['column_5', 'column_8', 'column_9'],
                'update_fields': ['column_10'],
                'special_handling': None
            },
            'table_4': {
                'conflict_fields': ['column_8'],
                'update_fields': ['column_10', 'column_11', 'column_12', 'column_13', 'column_14', 'column_15', 'column_16', 'column_17', 'column_18', 'column_19', 'column_20', 'column_21', 'column_22', 'column_23', 'column_24', 'column_25', 'column_26', 'column_27', 'column_28', 'column_29', 'column_30', 'column_31'],
                'special_handling': None
            },
            'table_5': {
                'conflict_fields': ['column_8', 'column_10'],
                'update_fields': [],  # Do not update any fields on conflict
                'special_handling': None,
                'do_nothing_on_conflict': True
            },
            'table_6': {
                'conflict_fields': ['column_8', 'column_10'],
                'update_fields': [],  # Do not update any fields on conflict
                'special_handling': None,
                'do_nothing_on_conflict': True
            },
            'table_7': {
                'conflict_fields': ['column_10'],
                'update_fields': ['data', 'column_32'],
                'special_handling': None
            },
            'table_8': {
                'conflict_fields': [],
                'update_fields': ['tags'],
                'key_columns': ['content', 'datetime', 'column_33'],
                'special_handling': None
            },
            'table_9': {
                'conflict_fields': [],
                'update_fields': ['tags'],
                'key_columns': ['id'],
                'special_handling': None
            },
        }

        # Retrieve datatag_5 columns
        db_columns = get_table_columns(table_name)

        # Remove 'column_34' if it exists in the DataFrame
        if 'column_34' in df.columns:
            df = df.drop(columns=['column_34'])

        # Add 'column_10' if not present in df
        if 'column_10' in db_columns and 'column_10' not in df.columns:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df['column_10'] = current_time

        # Reorder DataFrame columns to match datatag_5 columns
        try:
            df = df[db_columns]
        except KeyError as e:
            logger.error(f"Reordering failed: {e}. DataFrame columns: {list(df.columns)}")
            raise

        # Validate DataFrame columns
        if sorted(list(df.columns)) != sorted(db_columns):
            logger.error(f"DataFrame columns {list(df.columns)} do not match datatag_5 columns {db_columns} for table '{table_name}'.")
            raise ValueError(f"DataFrame columns {list(df.columns)} do not match datatag_5 columns {db_columns} for table '{table_name}'.")

        # Replace pd.NA and NaN with None
        df = df.replace({pd.NA: None})
        df = df.where(pd.notnull(df), None)

        # Prepare data for insertion or update
        config = conflict_config.get(table_name, {})
        conflict_fields = config.get('conflict_fields', [])
        update_fields = config.get('update_fields', [])
        key_columns = config.get('key_columns', [])
        special_handling = config.get('special_handling', {})
        do_nothing_on_conflict = config.get('do_nothing_on_conflict', False)

        # Ensure special_handling is a dictionary
        if special_handling is None:
            special_handling = {}

        if not conflict_fields and update_fields:
            # Perform an UPDATE using key_columns
            if not key_columns:
                logger.error(f"No key_columns specified for table '{table_name}'. Cannot perform update without key columns.")
                raise ValueError(f"No key_columns specified for table '{table_name}'.")

            # Ensure key_columns are present in the DataFrame
            missing_keys = [col for col in key_columns if col not in df.columns]
            if missing_keys:
                logger.error(f"Key columns {missing_keys} not found in DataFrame for table '{table_name}'.")
                raise ValueError(f"Key columns {missing_keys} not found in DataFrame for table '{table_name}'.")

            # Build the UPDATE query
            update_assignments = []
            for col in update_fields:
                if col == 'tags':
                    assignment = sql.SQL("{col} = data.{col}::text[]").format(col=sql.Identifier(col))
                else:
                    assignment = sql.SQL("{col} = data.{col}").format(col=sql.Identifier(col))
                update_assignments.append(assignment)

            # Construct the VALUES clause
            values_list = []
            params = []
            data_columns = key_columns + update_fields
            for idx, row in df.iterrows():
                values = []
                for col in data_columns:
                    value = row[col]
                    # For 'tags' column, make sure the value is a list
                    if col == 'tags':
                        if isinstance(value, str):
                            # Convert the string representation of array to an actual list
                            import ast
                            value = ast.literal_eval(value)
                    values.append(value)
                placeholders = [sql.Placeholder() for _ in values]
                values_list.append(sql.SQL('({})').format(sql.SQL(', ').join(placeholders)))
                params.extend(values)

            values_clause = sql.SQL(', ').join(values_list)

            # Build the WHERE clause for matching records
            where_conditions = [
                sql.SQL("{table}.{col} = data.{col}").format(
                    table=sql.Identifier(table_name),
                    col=sql.Identifier(col)
                ) for col in key_columns
            ]
            where_clause = sql.SQL(' AND ').join(where_conditions)

            # Build the full UPDATE query
            update_query = sql.SQL(
                "UPDATE {table} SET {assignments} FROM (VALUES {values}) AS data ({data_columns}) WHERE {where_clause}"
            ).format(
                table=sql.Identifier(table_name),
                assignments=sql.SQL(', ').join(update_assignments),
                values=values_clause,
                data_columns=sql.SQL(', ').join(map(sql.Identifier, data_columns)),
                where_clause=where_clause
            )

            # Execute the UPDATE query
            try:
                connection = get_connection()
                with connection.cursor() as cursor:
                    cursor.execute(update_query, params)
                connection.commit()
                logger.info(f"Data updated successfully in '{table_name}'.")
            except Exception as e:
                connection.rollback()
                logger.exception(f"Failed to update data in '{table_name}': {str(e)}")
                raise

            # Exit the function after successful UPDATE
            return  # Prevent further execution

        else:
            # Existing logic for INSERT with conflict handling
            # Prepare data for insertion using records with preserved data types
            records = [tuple(x) for x in df.itertuples(incolumn_3=False, name=None)]

            # If update_fields is None or empty, update all columns except conflict_fields
            if update_fields is None:
                update_fields = [col for col in db_columns if col not in conflict_fields]

            if do_nothing_on_conflict:
                conflict_clause = sql.SQL("ON CONFLICT ({}) DO NOTHING").format(
                    sql.SQL(', ').join(map(sql.Identifier, conflict_fields))
                )
            else:
                update_assignments = [
                    sql.SQL("{col} = {value}").format(
                        col=sql.Identifier(col),
                        value=special_handling.get(col, sql.SQL("EXCLUDED.{col}").format(col=sql.Identifier(col))))
                    for col in update_fields
                ]
                if conflict_fields:
                    conflict_clause = sql.SQL("ON CONFLICT ({fields}) DO UPDATE SET {assignments}").format(
                        fields=sql.SQL(', ').join(map(sql.Identifier, conflict_fields)),
                        assignments=sql.SQL(', ').join(update_assignments)
                    )
                else:
                    conflict_clause = sql.SQL("")

            # Construct the insert query with placeholders
            insert_query = sql.SQL(
                "INSERT INTO {table} ({fields}) VALUES ({placeholders}) {conflict}"
            ).format(
                table=sql.Identifier(table_name),
                fields=sql.SQL(', ').join(map(sql.Identifier, db_columns)),
                placeholders=sql.SQL(', ').join(sql.Placeholder() for _ in db_columns),
                conflict=conflict_clause
            )

            # Execute the query using batch execution
            try:
                connection = get_connection()
                with connection.cursor() as cursor:
                    cursor.executemany(insert_query, records)
                connection.commit()
                logger.info(f"Data inserted successfully into '{table_name}'.")
            except Exception as e:
                connection.rollback()
                logger.exception(f"Failed to insert data into '{table_name}': {str(e)}")
                raise

    except Exception as e:
        logger.exception(f"Failed to process data for '{table_name}': {str(e)}")
        raise

def function_5():
    query = """
    SELECT id, content, column_44, column_50, tags FROM table_9 WHERE tags IS NULL ORDER BY column_44 DESC LIMIT 300;
    """

    try:
        logger.info("Executing query to retrieve last 100 unlabeled tweets")
        # Execute the query and return the DataFrame
        df = Send_query_to_DB_silent(query)
        
        if df.empty:
            logger.warning("No unlabeled tweets found for recent tweets")
            return pd.DataFrame(columns=['id', 'content', 'column_44', 'column_50', 'tags'])
        else:
            num_tweets = len(df)
            logger.info(f"Retrieved  {num_tweets}  unlabeled tweets")
            return df

    except Exception as e:
        logger.error(f"An error occurred while retrieving unlabeled tweets: {e}")
        return pd.DataFrame(columns=['id', 'content', 'column_44', 'column_50', 'tags'])

def function_4():
    query = """
    WITH updated AS (
        SELECT id,
               '@' || LOWER((regexp_matches(content, '^RT @([^:]+): .*'))[1]) AS handle_to_remove
        FROM table_9
        WHERE content ~ '^RT @[A-Za-z0-9_]+: '
          AND column_44 >= (now() - interval '24 hour')
          AND column_44 <= now()
    ),
    upd AS (
        UPDATE table_9
        SET tags = ARRAY_REMOVE(tags, updated.handle_to_remove)
        FROM updated
        WHERE table_9.id = updated.id
        RETURNING table_9.id
    )
    SELECT COUNT(*) AS removed FROM upd;
    """

    try:
        logger.info("Executing query to remove RT handles from tags")
        # Execute the query and return the DataFrame
        df = Send_query_to_DB_silent(query)
        
        if df.empty:
            logger.warning("Problem with RT handle removal")
            return pd.DataFrame(columns=['removed'])
        else:
            num_tweets = df['removed'].iloc[0]
            if num_tweets > 0:
                logger.info(f"Removed: {num_tweets} handles from labeled tweets")
            else:
                logger.info("No RT handles were found/removed during this run.")
            return df

    except Exception as e:
        logger.error(f"An error occurred while removing RT handles: {e}")
        return pd.DataFrame(columns=['removed'])

def function_3(tag_5_time_str):
    """
    Retrieves the top 20 results by score from the datatag_5.

    Parameters:
    - tag_5_time_str (str): tag_5 time as a string in 'YYYY-MM-DD HH:MM:SS' format.
    - positive_offset_hours (int): Number of hours to add to the tag_5 time.
    - look_back_hours (int): Number of hours to look back from the positive time.

    Returns:
    - pd.DataFrame: DataFrame containing the top 20 results.
    """
    try:
        logger.info("Executing query to retrieve top 20 results by score")

        # Convert tag_5_time_str to datetime
        tag_5_time = datetime.strptime(tag_5_time_str, '%Y-%m-%d %H:%M:%S')
        start_time = tag_5_time

        # Prepare the SQL query
        query = """
        WITH calculated AS (
            SELECT
                t.tag,
                p.column_50 AS "user",
                p.column_44,
                EXTRACT(EPOCH FROM (p.column_44 - %(start_time)s)) / 3600.0 AS timediff,
                p.content
            FROM
                table_9 p
            CROSS JOIN LATERAL
                unnest(p.tags) AS t(tag)
            WHERE
                p.column_44 < %(start_time)s
                AND p.column_44 >= (%(start_time)s - interval '240 hour')
                AND p.tags IS NOT NULL
        ),
        avg_fx_per_user AS (
            SELECT
                tag,
                "user",
                --AVG(f_x) AS avg_fx
                (SUM(f_x)/POWER(0.8, LEAST(GREATEST(COUNT(f_x), 1.0), 3))) as avg_fx
            FROM (
                SELECT
                    tag,
                    "user",
                    CASE
                        WHEN timediff >= -240 AND timediff <= 0 THEN
                            ((timediff + 24) / 24.0)+3
                        ELSE
                            0
                    END AS f_x
                FROM
                    calculated
            ) sub
            WHERE f_x IS NOT NULL
            GROUP BY
                tag,
                "user"
        ),
        total_avg_fx_per_tag AS (
            SELECT
                tag,
                SUM(avg_fx) AS total_avg_fx,
                COUNT(DISTINCT "user") AS unique_user_count
            FROM
                avg_fx_per_user
            GROUP BY
                tag
        ),
        final_score as (
            SELECT
                tag,
                --total_avg_fx AS score, 
                total_avg_fx - 10*unique_user_count AS score,
                unique_user_count as users
            FROM
                total_avg_fx_per_tag
            WHERE
                (tag LIKE '%%$%%' OR tag LIKE '%%#%%' OR tag LIKE '%%@%%')
                AND unique_user_count <= 10
        )
        SELECT ROW_NUMBER() OVER (ORDER BY score DESC) AS pos, * FROM final_score ORDER BY score DESC limit 20;
        """

        # Define the parameters for the query
        params = {
            'start_time': start_time
        }

        # Execute the query and return the DataFrame
        df = Send_query_to_DB_silent(query, params)

        if df.empty:
            logger.warning("No results found for the given parameters")
            return pd.DataFrame(columns=['pos', 'tag', 'score', 'users'])
        else:
            num_results = len(df)
            logger.info(f"Retrieved {num_results} results")
            return df

    except Exception as e:
        logger.error(f"An error occurred while retrieving results: {e}")
        return pd.DataFrame(columns=['pos', 'tag', 'score', 'users'])

def function_2():
    query = """
    SELECT ARRAY_AGG(DISTINCT tag) AS unique_tags
    FROM table_8, UNNEST(tags) AS tag;
    """

    try:
        logger.info("Executing query to retrieve existing tags")
        # Execute the query and return the DataFrame
        df = Send_query_to_DB_silent(query)
        
        if df.empty:
            logger.warning("No tags found???")
            return pd.DataFrame(columns=['unique_tags'])
        else:
            num_tags = len(df)
            logger.info(f"Retrieved  {num_tags}  tags")
            return df

    except Exception as e:
        logger.error(f"An error occurred while retrieving tags: {e}")
        return pd.DataFrame(columns=['unique_tags'])

def function_1():
    query = """
    WITH calculated AS (
        SELECT
            t.tag,
            p.column_33 AS "user",
            p.datetime,
            EXTRACT(EPOCH FROM (p.datetime - (timestamp '2024-11-15 13:00:00' - interval '12 hour'))) / 3600.0 AS timediff,
            p.content
        FROM
            table_8 p
        CROSS JOIN LATERAL
            unnest(p.tags) AS t(tag)
        WHERE
            p.datetime < (timestamp '2024-11-15 13:00:00' - interval '12 hour')
            AND p.datetime >= (timestamp '2024-11-15 13:00:00'  - interval '12 hour') - interval '72 hour'
    ),
    avg_fx_per_user AS (
        SELECT
            tag,
            "user",
            AVG(f_x) AS avg_fx
        FROM (
            SELECT
                tag,
                "user",
                CASE
                    WHEN timediff >= -96 AND timediff < -24 THEN
                        (3 * timediff + 3 * 72) / 72.0
                    WHEN timediff >= -24 AND timediff <= 0 THEN
                        POWER(1.0695, timediff) * 10
                    ELSE
                        NULL
                END AS f_x
            FROM
                calculated
        ) sub
        WHERE f_x IS NOT NULL
        GROUP BY
            tag,
            "user"
    ),
    total_avg_fx_per_tag AS (
        SELECT
            tag,
            SUM(avg_fx) AS total_avg_fx,
            COUNT(DISTINCT "user") AS unique_user_count
        FROM
            avg_fx_per_user
        GROUP BY
            tag
    ),
    top_tags AS (
        SELECT
            tag
        FROM
            total_avg_fx_per_tag
        WHERE
            unique_user_count <= 4 AND
            unique_user_count > 1
        ORDER BY
            total_avg_fx DESC
        LIMIT 20
    ),
    unique_posts AS (
        SELECT DISTINCT ON (tag, content)
            tag,
            p.content,
            p.column_33 AS "user",
            p.datetime
        FROM
            table_8 p
        CROSS JOIN LATERAL
            unnest(p.tags) AS t(tag)
        WHERE
            t.tag IN (SELECT tag FROM top_tags)
            AND p.datetime < (timestamp '2024-11-15 13:00:00' - interval '12 hour')
            AND p.datetime >= (timestamp '2024-11-15 13:00:00' - interval '12 hour') - interval '72 hour'
        ORDER BY
            tag, content, p.datetime DESC
    )
    SELECT
        tag,
        array_agg(content) AS posts,
        array_agg("user") AS users,
        array_agg(datetime) AS post_dates
    FROM
        unique_posts
    GROUP BY
        tag
    ORDER BY
        tag;
    """
    try:
        logger.info("Executing query to retrieve posts with top20 tags")
        # Execute the query and return the DataFrame
        df = Send_query_to_DB_silent(query)
        
        if df.empty:
            logger.warning("No posts found???")
            return pd.DataFrame(columns=['tag', 'posts', 'users', 'post_dates'])
        else:
            num_tags = len(df)
            logger.info(f"Retrieved  {num_tags}  tags with posts 72 hour period")
            return df

    except Exception as e:
        logger.error(f"An error occurred while retrieving posts with tags: {e}")
        return pd.DataFrame(columns=['tag', 'posts', 'users', 'post_dates'])

def update_tags(row):
    system_prompt = """
    Assign the most accurate tags for a give tweet about tag_25. If the very similar tag already exists in Existing_Tags, use that one. If tweet contains a column_14 (starts with ====) ALWAYS extract the column_14 as a tag in small letters. If tweet has a hashtag (starts with #) ALWAYS collect the hashtag. If tweet has a handle (starts with @) ALWAYS collect the handle. try to find and identify names of the tag_25 and save them as well. Do not use tags from Banned_Tags list.
    Existing_Tags: ["tag_1", "tag_2", "tag_3", "tag_4", "tag_5", "tag_6", "tag_7", "tag_8", "tag_9", "tag_10", "tag_11", "tag_12"]
    Banned_Tags: ["tag_25", "tag_27"]
    Examples:
    1.
        Tweet: "Imagine that the king of #hashtag_1 tag_13 will hit $1+ this cycle and the king of AI tag_14 will hit $5K.
            The two biggest column_12s: AI + hashtag_1. @handle_1"
        Output: { "tags": ["tag_13", "tag_14", "ai", "tag_15", "@handle_1", "#hashtag_1"] }
    2.
        Tweet: "Welcome to the $PNUT fam."
        Output: { "tags": ["$pnut"] }
    3.
        Tweet: "tag_16 continues to make new highs. tag_17 is the biggest tag_15 on #hashtag_2. tag_18 is my favorite runner up after tag_17.
            Both will send hard this cycle."
        Output: { "tags": ["#hashtag_2", "tag_16", "tag_15", "tag_17", "tag_18"] }
    4.
        Tweet: "tag_19 IS DESTINED FOR 1 BILLION."
        Output: { "tags": ["tag_19"] }
    5.
        Tweet: "@handle_2 predicted that tag_5 is going to kick off in a big way, The #tag_6 tag_20 has been designed specifically to onboard new retail. Currently accumulating a bunch of tag_5 hashtag_1 including this one $tag_9 - any pullbacks into blue ill be adding"
        Output: { "tags": ["tag_5", "tag_6", "tag_20", "$tag_9", "#tag_6", "@handle_2"] }
    6.
        Tweet: "tag_21 I think you know what’s about to happen "
        Output: { "tags": ["tag_22", "tag_21"] }
    7.
        Tweet: "This 440 point move on tag_23 was shared in the Hedge Experimental zone. 
            tag_9 signals or conservative scalps on big caps, we’ve got you covered. 
            Sign up for our automation: https://tag_24.finance
            tag_26 #tag_24"
        Output: { "tags": ["tag_25", "tag_23", "tag_24", "tag_9", "tag_26", "#tag_24"] }
    8.
        Tweet: "Zero1 Labs is excited to unveil our v2 token vision next week.
            A bold step forward for the only community-run and launched AI ecosystem.
            tag_28 will be the primary asset driving decentralized AI, supporting both tag_29 and tag_30s.
            The first PoS chain with full…"
        Output: { "tags": ["tag_28", "tag_29", "tag_30", "tag_31", "tag_32", "ai", "pos"] }
    9.
        Tweet: "tag_6 and tag_7 to list #moodeng"
        Output: { "tags": ["moodeng", "tag_6", "tag_7"]
    """
    user_prompt = f"""
    Tweet: {row['content']}
    """
    output_schema = {
        "type": "json_schema",
        "json_schema": {
            "name": "tags_schema",
            "schema": {
                "type": "object",
                "properties": {
                    "tags": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "pattern": "^[a-z0-9 @#$_]+$"
                        },
                        "minItems": 1,
                        "maxItems": 17
                    }
                },
                "required": ["tags"],
                "additionalProperties": False
            }
        }
    }
    
    response = llm_client.chat.completions.create(
      model="qwen2.5-14b-instruct",
      messages=[
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
      ],
      response_format=output_schema,
      temperature=0.3,
    )
    resp_content = response.choices[0].message.content
    try:
        extracted_json = json.loads(resp_content)
        tags = extracted_json.get("tags", [])
        unique_tags = list(set(tags))  # Convert the set back to a list
        row['tags'] = unique_tags  # Assign the list to row['tags']
        #logger.info(f"{unique_tags}")
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON: {e}")

    return row

def summarise_posts(row):
    system_prompt = """
    You are an AI assistant that summarizes social media posts for a given tag. Your task is to read the following posts related to a specific tag and provide a concise summary highlighting the most valuable information.
    """
    # Ensure that posts, users, and post_dates are lists
    posts = row['posts']
    users = row['users']
    post_dates = row['post_dates']

    # Convert to lists if they are not already
    if not isinstance(posts, list):
        posts = list(posts)
    if not isinstance(users, list):
        users = list(users)
    if not isinstance(post_dates, list):
        post_dates = list(post_dates)

    # Format the posts into a readable string
    formatted_posts = ''
    for post, user, date in zip(posts, users, post_dates):
        formatted_posts += f"- [{date}] {user}: {post}\n"

    user_prompt = f"""
    Tag: {row['tag']}
    Posts:
    {formatted_posts}
    """
    try:
        response = llm_client.chat.completions.create(
            model="qwen2.5-14b-instruct",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.6,
        )
        # Get the assistant's reply
        assistant_reply = response.choices[0].message.content
        # Assign the summary to the row
        row['summary'] = assistant_reply.strip()
        logger.info(f"Summary for tag '{row['tag']}': {row['summary']}")
    except Exception as e:
        logger.error(f"Error during summarization for tag '{row['tag']}': {e}", exc_info=True)
        row['summary'] = ''
    return row


# ================== MAIN PROGRAM ==================
def post_labeling_program():
    # --------------------- Redis Lock Acquisition ---------------------
    try:
        # Initialize Redis client
        redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),       # Redis server host
            port=int(os.getenv('REDIS_PORT', 6379)),         # Redis server port
            password=os.getenv('REDIS_PASSWORD', None),      # Redis password, if any
            db=int(os.getenv('REDIS_DB', 0)),                # Redis datatag_5 number
            decode_responses=True                            # Decode responses as strings
        )

        # Attempt to acquire the lock
        have_lock = redis_client.set('post_labeling_program_lock', 'locked', nx=True)
        if not have_lock:
            logger.info("Process exited because it already exists.")
            return
        logger.info(f"Redis lock: post_labeling_program_lock acquired, starting application")
    except Exception as e:
        logger.error(f"Falied to connect to redis and get the lock: {e}")
        sys.exit(1)
    # --------------------------------------------------------------
    # -- Get the DB connection --
    initialize_connection()

    # Record start time
    start_time = time.time()

    # Columns from table_8
    table_8_columns = [
        "id", "column_19_id", "content", "column_40", "column_41", "column_42", "name", "column_43", "column_44",
        "column_45", "column_46", "column_47", "column_48", "column_49", "tags", "column_50"
    ]

    # -- labeling posts code --
    try:
        logger.info(f"Getting data from the remote DB...")
        unlabeled_tweets = function_5()
        logger.info(f"Unlabeled tweets: {unlabeled_tweets}")
        if not unlabeled_tweets.empty:
            labeled_tweets = unlabeled_tweets.copy()
            try:
                labeled_tweets = labeled_tweets.apply(update_tags, axis=1)
                logger.info(f"labeled tweets: {labeled_tweets}")
            except Exception as e:
                logger.error(f"Error tagging posts: {e}", exc_info=True)
             # Add missing columns and initialize them with null (None)
            for col in table_8_columns:
                if col not in labeled_tweets.columns:
                    labeled_tweets[col] = None  # Initialize missing columns with None
            labeled_tweets.to_csv("labeled_tweets.csv", incolumn_3=False)

            try:
                insert_df_to_table(labeled_tweets, 'table_9')
            except Exception as e:
                logger.error(f"Error during insert to the table table_8: {e}", exc_info=True)

            # -- removal of RT handles from tags --
            try:
                rmh = function_4()
            except Exception as e:
                logger.warning(f"Warn: tags could not be removed due to a problem: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Failed to get tweets or tags: {e}")

    # Record end time and log the elapsed time
    end_time = time.time()
    elapsed_time = end_time - start_time
    logger.info(f"Main task completed in {elapsed_time:.2f} seconds.")

    # -- Disconnect from DB --
    try:
        close_connection()
    except Exception as e:
        logger.error(f"Failed close the DB connection: {e}")
    # --------------------- Redis Lock Release ---------------------
    try:
        redis_client.delete('post_labeling_program_lock')
        logger.debug("Lock released.")
    except Exception as e:
        logger.error(f"Failed to release lock: {e}")
    # --------------------------------------------------------------

#---------------- Consumer part ------------------------
def start_consuming():
    rabbit_connection = None
    while rabbit_connection is None:
        try:
            rabbit_connection = create_connection()
            channel = rabbit_connection.channel()
            #channel1 = rabbit_connection.channel()
            channel.basic_qos(prefetch_count=1)

            # Deklaracja kolejki tagowania
            channel.queue_declare(
                queue='start_tagging'
            )

            # Deklaracja kolejki do top20
            channel.queue_declare(
                queue='generate_top_20'
            )
            channel.confirm_delivery()

            def publish(routing_key: str, body: Dict[str, Any]):
                body_json = json.dumps(body)

                #ensure_connection()  # Sprawdzenie i ewentualne ponowne nawiązanie połączenia
                try:
                    if channel.basic_publish(exchange='', routing_key=routing_key, body=body_json):
                        print(f"Message published to queue: {routing_key}")
                    else:
                        print(f"NOT PUBLISHED to queue: {routing_key}")
                except Exception as e:
                    logger.error(f"Message was not published to queue: {e}")
                    ensure_connection()  # Próba ponownego połączenia w razie błędu

            # Funkcja callback do obsługi wiadomości
            def callback(ch, method, properties, body):
                try:
                    post_labeling_program()
                    message = {"posty": "gotowe"}
                    publish("generate_top_20", message)
                    logger.info(f"Finished processing request from Q")
                    channel.basic_ack(delivery_tag=method.delivery_tag)
                except Exception as e:
                    logger.error(f"Error processing message from Q: {e}")
                    channel.basic_ack(delivery_tag=method.delivery_tag)

            # Konfiguracja konsumowania wiadomości
            channel.basic_consume(queue='start_tagging', on_message_callback=callback, auto_ack=False)

            logger.info('STARTED CONSUMING')

            channel.start_consuming()

        except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelClosedByBroker) as e:
            logger.warning(f"Connection failed: {e}")
            logger.warning("Retrying connection in 5 seconds...")
            rabbit_connection = None
            time.sleep(5)

if __name__ == "__main__":
    start_consuming()