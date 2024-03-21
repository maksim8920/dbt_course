SELECT 
	user_value->>'uid' AS user_id,
	user_value->>'display' AS name,
	user_value->>'email' AS email,
	(user_value->>'firstLoginDate')::timestamptz AT TIME ZONE 'Europe/Moscow' AS first_login
FROM {{ source('task_tracker', 'users') }}