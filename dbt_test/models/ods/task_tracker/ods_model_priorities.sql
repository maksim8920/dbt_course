SELECT 
	(priority_value->>'id')::int AS task_field_id,
	priority_value->>'key' AS "key",
	priority_value->>'name' AS "name"
FROM {{ source('task_tracker', 'priorities') }}