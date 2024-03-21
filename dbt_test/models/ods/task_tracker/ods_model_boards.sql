SELECT 
	(board_value->>'id')::int AS status_id,
	board_value->>'name' AS "name"
FROM {{ source('task_tracker', 'boards') }}