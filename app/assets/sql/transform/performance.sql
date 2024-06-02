WITH players_last_online AS (
    SELECT
        username,
        MAX(last_online) >= NOW() - INTERVAL '1 month' AS is_active
    FROM
        public.players
    GROUP BY
        username
)

SELECT
    games.username,
    COUNT(games.game_id)                                           AS total_games,
    AVG(games.user_rating)                                         AS avg_user_rating,
    AVG(games.opponent_rating)                                     AS avg_opponent_rating,
    SUM(CASE WHEN games.match_result = 'win' THEN 1 ELSE 0 END)    AS total_wins,
    SUM(CASE WHEN games.match_result = 'defeat' THEN 1 ELSE 0 END) AS total_losses,
    SUM(CASE WHEN games.match_result = 'draw' THEN 1 ELSE 0 END)   AS total_draws,
    CASE WHEN
            players_last_online.is_active
            AND MAX(CAST(games.start_date_time AS TIMESTAMP)) >= NOW() - INTERVAL '1 month'
            THEN 'Active and playing'
        WHEN players_last_online.is_active THEN 'Active not playing'
        WHEN players_last_online.is_active IS NULL THEN 'Unknown (missing players data)'
        ELSE 'Not active'
    END                                                            AS player_status
FROM
    public.games
LEFT JOIN
    players_last_online ON games.username = players_last_online.username
GROUP BY
    games.username,
    players_last_online.is_active
