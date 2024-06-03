SELECT
    username,
    opening,
    game_mode,
    COUNT(CASE WHEN match_result = 'win' THEN 1 END)   AS no_of_win,
    COUNT(CASE WHEN match_result = 'defet' THEN 1 END) AS no_of_defeat,
    COUNT(CASE WHEN match_result = 'draw' THEN 1 END)  AS no_of_draw
FROM
    public.games
GROUP BY
    opening,
    username,
    game_mode
