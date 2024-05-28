SELECT 
    username,
    ROUND((SUM(CASE WHEN match_result = 'win' THEN 1 ELSE 0 END) * 100.0 / COUNT(game_id)), 2) AS win_perc,
    ROUND((SUM(CASE WHEN match_result = 'defet' THEN 1 ELSE 0 END) * 100.0 / COUNT(game_id)), 2) AS defeat_perc,
    ROUND((SUM(CASE WHEN match_result = 'draw' THEN 1 ELSE 0 END) * 100.0 / COUNT(game_id)), 2) AS draw_perc
FROM 
    public.games
GROUP BY 
    username
ORDER BY 
    username ASC;
