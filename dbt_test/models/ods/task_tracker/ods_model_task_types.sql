SELECT 
	(type_value->>'id')::int AS task_type_id,
	type_value->>'key' AS "key",
	type_value->>'name' AS "name"
FROM {{ source('task_tracker', 'task_types') }}