WITH games_per_date AS (
    SELECT
        start_date,
        username,
        FIRST_VALUE(user_rating)
            OVER (PARTITION BY username, start_date ORDER BY CAST(start_date_time AS TIMESTAMP) DESC)
            AS last_rating
    FROM games
),

date_user_rating AS (
    SELECT
        start_date,
        username,
        MAX(last_rating) AS last_rating
    FROM games_per_date
    GROUP BY username, start_date
)

SELECT
    start_date,
    username,
    last_rating,
    last_rating - LAG(last_rating) OVER (PARTITION BY username ORDER BY start_date) AS increase_in_rating
FROM date_user_rating
