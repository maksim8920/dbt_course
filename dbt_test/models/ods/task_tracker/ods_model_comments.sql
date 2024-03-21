SELECT 
	task_id,
	(comment_value->>'id')::int AS comment_id,
	(comment_value->>'createdAt')::timestamptz AT TIME ZONE 'Europe/Moscow' AS created_at,
	((comment_value->'createdBy')->>'id') AS created_by,
	CASE 
		WHEN (comment_value->>'version')::int = 1 THEN NULL
		ELSE (comment_value->>'updatedAt')::timestamptz AT TIME ZONE 'Europe/Moscow'
	END AS updated_at,
	CASE 
		WHEN (comment_value->>'version')::int = 1 THEN NULL
		ELSE ((comment_value->'updatedBy')->>'id')
	END AS updated_by,
	comment_value->>'text' AS "text",
	(comment_value->>'version')::int AS version
FROM {{ source('task_tracker', 'comments') }}