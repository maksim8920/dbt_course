SELECT 
	(priority_value->>'id')::int AS priority_id,
	priority_value->>'key' AS "key",
	priority_value->>'name' AS "name"
FROM {{ source('task_tracker', 'priorities') }}