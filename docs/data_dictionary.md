# Data Dictionary

This document provides an overview of the structure and description of the data fields in this project.

## Sources

Here is the description of incoming sources used in this project. Majority of data are coming from chess.com public api.
The incoming data for this project are represented by two main source objects: players (users) and games. These are coming from chess\.com public api.

### Table: Players

Data about registered users (players) including some basic statistics.

| Column Name   | Data Type               | Description                                      |
|---------------|-------------------------|--------------------------------------------------|
| player_id     | bigint                  | Unique identifier for each user                   |
| snaphot_date  | timestamp without time zone | The date and time when the snapshot was taken (api only provides data "as is now")   |
| name          | character varying       | The name of the user                             |
| username      | character varying       | The username of the user                         |
| title         | character varying       | Achieved title of the user. Not every user has it. The full list of possible titles along with their definitions is available at [this link](https://www.chess.com/terms/chess-titles)                           |
| followers     | bigint                  | The number of followers the user has at the snapshot moment            |
| country       | character varying       | The country of the user                          |
| location      | character varying       | Current location of the user                         |
| last_online   | timestamp without time zone | The date and time when the user was last online |
| joined        | timestamp without time zone | The date and time when the user joined (registered)           |
| is_streamer   | boolean                 | Indicates if the user is a streamer              |

### Table: Games

Data about each game happened on a platform between two players.

| Column Name         | Data Type               | Description                                      |
|---------------------|-------------------------|--------------------------------------------------|
| game_id             | bigint                  | Unique identifier for each game                   |
| game_url            | character varying       | The URL of the game. A game could be reviewed directly on the chess\.com site in interactive mode.                             |
| game_mode           | character varying       | The mode of the game                             |
| start_date          | character varying       | The date of the game. This column is used in api request's header (this date is compared with start and end dates provided in a call)                        |
| username            | character varying       | The username of the user (first player)                         |
| user_color          | character varying       | The color of the first player's pieces                             |
| user_rating         | integer                 | First player's rating at the beginning of the game                            |
| user_accuracy       | double precision        | The accuracy* of the first player                          |
| opponent            | character varying       | The username of the opponent (second player)                           |
| opponent_rating     | integer                 | Second player's rating at the beginning of the game                        |
| opponent_accuracy   | double precision        | The accuracy* of the second player                      |
| rating_diff         | integer                 | The rating difference between the players |
| match_result        | character varying       | The result of the game (win, loss, draw), relative to the first player (user)     |
| result_subcategory  | character varying       | Details of the result**                |
| start_date_time     | character varying       | The timestamp when the game began               |
| end_date_time       | character varying       | The timestamp when the game ended                  |
| game_duration       | character varying       | The duration of the game as timestamp                         |
| game_duration_sec   | integer                 | The duration of the game in seconds               |
| rounds              | integer                 | The number of rounds in the game (white move and black move = one complete round)                 |
| user_avg_move_time_sec | double precision      | The average move time of the user in seconds      |
| opening             | character varying       | The opening of the game (opening is a first white move, every possibility has its own opening name)                          |

\* Accuracy is a measurement of how closely a user played to what the computer has determined to be the best possible play against opponent's specific moves. The closer it is to 100, the closer a player is to 'perfect' play, as determined by the engine. 

<details>
<summary>
** Result Subcategories
</summary> <br />


| Result Subcategory  | Description                                      |
|---------------------|--------------------------------------------------|
| stalemate           | The game ended in a stalemate, where neither player can make a legal move |
| insufficient        | The game ended in a draw due to insufficient material to checkmate |
| agreed              | The players agreed to a draw                      |
| timeout             | The game ended due to one player running out of time |
| timevsinsufficient  | The game ended in a draw due to one player running out of time and the other player having insufficient material to checkmate |
| win                 | The first player (user) won the game by a checkmate             |
| resigned            | The first player (user) resigned the game         |
| repetition          | The game ended in a draw due to threefold repetition of the position |
| abandoned           | The game was abandoned                            |
| checkmated          | The first player (user) was checkmated and lost            |

</details>

## Transformed data

These objects were derived from the original data during the pipeline processing. Additionally, a precomputed analysis in these objects has been materialized into a table for easier access by downstream teams and end users.

### Table: Overall Performance

High-level information about available players' performance.

| Column Name   | Data Type               | Description                                      |
|---------------|-------------------------|--------------------------------------------------|
| username      | character varying       | The username of the player                       |
| win_perc      | numeric                 | The percentage of games won by the player        |
| defeat_perc   | numeric                 | The percentage of games lost by the player       |
| draw_perc     | numeric                 | The percentage of games drawn by the player      |


### Table: Performance

A slightly deeper analytics on players statistics.

| Column Name          | Data Type               | Description                                      |
|----------------------|-------------------------|--------------------------------------------------|
| username             | character varying       | The username of the player                       |
| total_games          | bigint                  | The total number of games played by the player    |
| avg_user_rating      | numeric                 | The average rating of the player                 |
| avg_opponent_rating  | numeric                 | The average rating of the opponents faced by the player |
| total_wins           | bigint                  | The total number of games won by the player       |
| total_losses         | bigint                  | The total number of games lost by the player      |
| total_draws          | bigint                  | The total number of games drawn by the player     |
| player_status        | text                    | The status of the player* |

<details>
<summary>
* Statuses explanation
</summary> <br />

| Status               | Explanation                                      |
|----------------------|--------------------------------------------------|
| Active and playing   | The player is active and has played at least one game within the last month |
| Active not playing   | The player is active but has not played any games within the last month |
| Not active           | The player wasn't online within last month |
| Unknown | The player's status is unknown (missing data for player) |

</details>

### Table: Play Rating Trend

Time series with changes of player's rating over time. After each completed game the rating is changing based on the result.

| Column Name         | Data Type               | Description                                      |
|---------------------|-------------------------|--------------------------------------------------|
| start_date          | character varying       | The date when change in rating occured              |
| username            | character varying       | The username of the player                        |
| last_rating         | integer                 | The rating of the player at the end of the rating period |
| increase_in_rating  | integer                 | The increase in rating during the rating period   |


### Table: Pipeline Logs

This is a technical object which keeps logs and metadata about the pipeline executions.

| Column Name   | Data Type               | Description                                      |
|---------------|-------------------------|--------------------------------------------------|
| pipeline_name | character varying       | The name of the pipeline                          |
| run_id        | integer                 | The unique identifier for each pipeline run       |
| timestamp     | character varying       | The timestamp when the pipeline execution occurred |
| status        | character varying       | The status of the pipeline execution              |
| config        | json                    | The configuration of the pipeline                 |
| logs          | character varying       | The logs generated during the pipeline execution  |

