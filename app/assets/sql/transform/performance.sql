SELECT
    username,
    COUNT(game_id)                                           AS total_games,
    AVG(user_rating)                                         AS avg_user_rating,
    AVG(opponent_rating)                                     AS avg_opponent_rating,
    SUM(CASE WHEN match_result = 'win' THEN 1 ELSE 0 END)    AS total_wins,
    SUM(CASE WHEN match_result = 'defeat' THEN 1 ELSE 0 END) AS total_losses,
    SUM(CASE WHEN match_result = 'draw' THEN 1 ELSE 0 END)   AS total_draws
FROM
    public.games
GROUP BY
    username
