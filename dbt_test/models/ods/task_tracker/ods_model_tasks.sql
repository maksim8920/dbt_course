SELECT 
	task_value->>'key' AS task_id,
	task_value->>'summary' AS title,
	task_value->>'description' AS description,
	(task_value->>'createdAt')::timestamptz AT TIME ZONE 'Europe/Moscow' AS created_at,
	(task_value->>'updatedAt')::timestamptz AT TIME ZONE 'Europe/Moscow' AS updatedAt,
	(task_value->>'resolvedAt')::timestamptz AT TIME ZONE 'Europe/Moscow' AS resolved_at,
	(task_value->'createdBy')->>'id' AS created_by,
	(task_value->'assignee')->>'id' AS assignee,
	(task_value->'resolvedBy')->>'id' AS resolved_by,
	(SELECT (ARRAY(SELECT (element->>'id') FROM jsonb_array_elements(task_value->'followers') AS element))) AS followers,
	((task_value->'queue')->>'id')::int AS queue,
	((task_value->'type')->>'id')::int AS "type",
	((task_value->'status')->>'id')::int AS status,
	((task_value->'priority')->>'id')::int AS priority,
	(task_value->>'storyPoints')::float AS story_points,
	task_value->>'shirtscore' AS shirt_score,
	(task_value->>'deadline')::date AS deadline,
	(task_value->'parent')->>'key' AS parent_task,
	(SELECT (ARRAY(SELECT (element->>'id')::int FROM jsonb_array_elements(task_value->'boards') AS element))) AS boards,
	(SELECT ARRAY(SELECT jsonb_array_elements_text(task_value->'tags'))) AS tags,
	(SELECT (ARRAY(SELECT (element->>'id')::int FROM jsonb_array_elements(task_value->'components') AS element))) AS components
FROM {{ source('task_tracker', 'tasks') }}