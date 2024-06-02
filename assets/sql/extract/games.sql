{% set config = {
    "extract_type": "full",
    "source_table_name": "games"
} %}

select

    game_id,
    game_url,
    game_mode,
    start_date,
    username,
    user_color,
    user_rating,
    user_accuracy,
    opponent,
    opponent_rating,
    opponent_accuracy,
    rating_diff,
    match_result,
    result_subcategory,
    start_date_time,
    end_date_time,
    game_duration,
    game_duration_sec,
    rounds,
    user_avg_move_time_sec,
    opening
    
from
    {{ config["source_table_name"] }}