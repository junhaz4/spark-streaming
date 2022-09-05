def query_example():
  """
  This is just an example query to showcase the format of submission
  Please format the query in Bigquery console and paste it here.
  Submission starts from query_one.
  """
  return """
SELECT
 display_name,
 reputation,
 up_votes,
 down_votes
FROM
 `bigquery-public-data.stackoverflow.users`;
  """

def query_one():
    """Query one"""
    return """
SELECT
  display_name,
  reputation,
  up_votes,
  down_votes
FROM
  `bigquery-public-data.stackoverflow.users`
WHERE
  up_votes>10000
  AND down_votes<300
ORDER BY
  reputation DESC
LIMIT
  10;
    """

def query_two():
    """Query two"""
    return """
SELECT
  location,
  COUNT(id) AS count
FROM
  `bigquery-public-data.stackoverflow.users`
GROUP BY
  location
ORDER BY
  count DESC
LIMIT
  10;
    """

def query_three():
    """Query three"""
    return """
SELECT
  (
    CASE
      WHEN location LIKE '%USA%' OR location LIKE '%United States%' THEN 'USA'
      WHEN location LIKE '%London%'
    OR location LIKE '%United Kingdom%' THEN 'UK'
      WHEN location LIKE '%France%' THEN 'France'
      WHEN location LIKE '%India%' THEN 'India'
    ELSE
    location
  END
    ) AS country,
  COUNT(id) AS num_users
FROM
  `bigquery-public-data.stackoverflow.users`
GROUP BY
  country
HAVING
  country IS NOT NULL
ORDER BY
  num_users DESC
LIMIT
  10;
    """

def query_four():
    """Query four"""
    return """
SELECT
  EXTRACT(YEAR
  FROM
    last_access_date) AS year,
  COUNT(id) AS num_users
FROM
  `bigquery-public-data.stackoverflow.users`
GROUP BY
  year
ORDER BY
  year DESC;
    """

def query_five():
    """Query five"""
    return """
SELECT
  id,
  display_name,
  last_access_date,
  DATE_DIFF(DATE('2021-12-30'),DATE(last_access_date),DAY) AS days_since_last_access,
  DATE_DIFF(DATE(last_access_date), DATE(creation_date),DAY) AS days_since_creation
FROM
  `bigquery-public-data.stackoverflow.users`
ORDER BY
  days_since_last_access DESC,
  days_since_creation DESC
LIMIT
  10;
    """

def query_six():
    """Query six"""
    return """
SELECT
  (
    CASE
      WHEN reputation>=0 AND reputation<= 500 THEN "0-500"
      WHEN reputation>=501
    AND reputation<= 5000 THEN "501-5000"
      WHEN reputation>=5001 AND reputation<= 50000 THEN "5001-50000"
      WHEN reputation>=50001
    AND reputation<= 500000 THEN "50001-500000"
    ELSE
    ">500000"
  END
    ) AS reputation_bucket,
  ROUND(SUM(up_votes)/SUM(down_votes),2) AS upvote_ratio,
  COUNT(id) AS num_users
FROM
  `bigquery-public-data.stackoverflow.users`
GROUP BY
  reputation_bucket
ORDER BY
  num_users DESC;
    """

def query_seven():
    """Query seven"""
    return """
SELECT
  tag,
  COUNT(tag) AS count
FROM
  `bigquery-public-data.stackoverflow.posts_questions`,
  UNNEST(SPLIT(tags,'|') ) AS tag
WHERE
  EXTRACT(YEAR
  FROM
    creation_date)=2020
GROUP BY
  tag
ORDER BY count desc
LIMIT
  10;
    """

def query_eight():
    """Query eight"""
    return """
SELECT
  name,
  COUNT(id) AS num_users
FROM
  `bigquery-public-data.stackoverflow.badges`
WHERE
  class=1
GROUP BY
  name
ORDER BY
  num_users DESC
LIMIT
  10;
    """

def query_nine():
    """Query nine"""
    return """
SELECT
  u.id AS id,
  display_name,
  reputation,
  up_votes,
  down_votes,
  b.count AS num_gold_badges
FROM
  `bigquery-public-data.stackoverflow.users` AS u
JOIN (
  SELECT
    user_id,
    COUNT(user_id) AS count
  FROM
    `bigquery-public-data.stackoverflow.badges`
  WHERE
    class=1
  GROUP BY
    user_id) AS b
ON
  u.id = b.user_id
ORDER BY
  num_gold_badges DESC
LIMIT
  10;
    """

def query_ten():
    """Query ten"""
    return """
SELECT
  id,
  display_name,
  reputation,
  DATE_DIFF(DATE(b.get_date),DATE(creation_date),DAY) AS num_days
FROM
  `bigquery-public-data.stackoverflow.users` AS u
JOIN (
  SELECT
    user_id,
    date AS get_date
  FROM
    `bigquery-public-data.stackoverflow.badges`
  WHERE
    name="Illuminator") AS b
ON
  u.id = b.user_id
ORDER BY
  num_days
LIMIT
  20;
    """

def query_eleven():
    """Query eleven"""
    return """
SELECT
  (
    CASE
      WHEN score<0 THEN '<0'
      WHEN score>=0
    AND score<=100 THEN "0-100"
      WHEN score>=101 AND score<=1000 THEN "101-1000"
      WHEN score>=1001
    AND score<10000 THEN "1001-10000"
    ELSE
    ">10000"
  END
    ) AS socre_bucket,
  ROUND(AVG(view_count),2) AS avg_num_views
FROM
  `bigquery-public-data.stackoverflow.posts_questions`
GROUP BY
  socre_bucket
ORDER BY
  avg_num_views
    """


def query_twelve():
    """Query twelve"""
    return """
SELECT
  CAST(EXTRACT(DAYOFWEEK
    FROM
      creation_date) AS INT64) AS day_name,
  COUNT(id) AS num_answers
FROM
  `bigquery-public-data.stackoverflow.posts_answers`
GROUP BY
  day_name
ORDER BY
  num_answers desc;
    """

def query_thirteen():
    """Query thirteen"""
    return """
SELECT
  a.year AS year,
  a.num_questions AS num_questions,
  ROUND((b.question_answered/a.num_questions)*100,2) AS percentage_answered
FROM (
  SELECT
    EXTRACT(YEAR
    FROM
      creation_date) AS year,
    COUNT(id) AS num_questions
  FROM
    `bigquery-public-data.stackoverflow.posts_questions`
  GROUP BY
    year) AS a
JOIN (
  SELECT
    EXTRACT(YEAR
    FROM
      creation_date) AS year,
    COUNT(id) AS question_answered
  FROM
    `bigquery-public-data.stackoverflow.posts_questions`
  WHERE
    answer_count >0
  GROUP BY
    year) AS b
ON
  a.year=b.year
ORDER BY
  year;
    """

def query_fourteen():
    """Query fourteen"""
    return """
SELECT
  u.id AS id,
  u.display_name AS display_name,
  u.reputation AS reputation,
  a.num_questions AS num_answers
FROM
  `bigquery-public-data.stackoverflow.users` AS u
JOIN (
  SELECT
    owner_user_id AS id,
    COUNT(id) AS num_questions
  FROM
    `bigquery-public-data.stackoverflow.posts_answers`
  GROUP BY
    owner_user_id
  HAVING
    num_questions>50) AS a
ON
  u.id=a.id
ORDER BY
  num_answers DESC
LIMIT
  20
    """

def query_fifteen():
    """Query fifteen"""
    return """
SELECT
  u.id AS id,
  u.display_name AS display_name,
  u.reputation AS reputation,
  v.num_answers AS num_answers
FROM
  `bigquery-public-data.stackoverflow.users` AS u
JOIN (
  SELECT
    a.owner_user_id AS user_id,
    COUNT(a.id) AS num_answers
  FROM
    `bigquery-public-data.stackoverflow.posts_answers` AS a
  JOIN
    `bigquery-public-data.stackoverflow.posts_questions` AS q
  ON
    a.parent_id=q.id
  WHERE
    q.tags LIKE '%python%'
  GROUP BY
    a.owner_user_id) AS v
ON
  u.id=v.user_id
ORDER BY
  v.num_answers DESC
LIMIT
  20
    """

def query_sixteen():
    """Query sixteen"""
    return """
SELECT
  (
    CASE
      WHEN score<0 THEN "<0"
      WHEN score>10000 THEN ">10000"
  END
    ) AS score,
  ROUND(SUM(answer_count)/COUNT(answer_count),2) AS avg_answers,
  ROUND(SUM(favorite_count)/COUNT(favorite_count),2) AS avg_fav_count,
  ROUND(SUM(comment_count)/COUNT(comment_count),2) AS avg_fav_count,
FROM
  `bigquery-public-data.stackoverflow.posts_questions`
GROUP BY
  score
HAVING
  score IS NOT NULL
ORDER BY
  score
    """
