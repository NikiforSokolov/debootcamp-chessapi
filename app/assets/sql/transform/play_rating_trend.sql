with games_per_date  as (
	select start_date , username , first_value (user_rating) over(partition by username, start_date order by start_date_time::timestamp desc) as last_rating
	from games g
)
, date_user_rating as (
	select start_date, username, max(last_rating) as last_rating
	from games_per_date
	group by username , start_date
)
select start_date , username, last_rating
,last_rating - lag(last_rating) over(partition by username order by start_date) as increase_in_rating
from date_user_rating