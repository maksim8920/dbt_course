SELECT 
	(queue_value->>'id')::int AS queue_id,
	queue_value->>'key' AS "key",
	queue_value->>'name' AS name,
	queue_value->>'description' AS description,
	((queue_value->'lead')->>'id') AS created_by
FROM {{ source('task_tracker', 'queues') }}