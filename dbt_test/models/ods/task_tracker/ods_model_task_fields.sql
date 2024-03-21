SELECT 
	task_field_value->>'id' AS task_field_id,
	task_field_value->>'key' AS "key",
	task_field_value->>'name' AS "name"
FROM {{ source('task_tracker', 'task_fields') }}