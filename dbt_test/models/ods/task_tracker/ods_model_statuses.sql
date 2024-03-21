SELECT 
	(status_value->>'id')::int AS status_id,
	status_value->>'key' AS "key",
	status_value->>'name' AS "name"
FROM {{ source('task_tracker', 'statuses') }}