SELECT 
	task_id,
	change_id,
	(((change_value->'links')[0]->'from')->'type')->>'id' AS change_from,
	(((change_value->'links')[0]->'to')->'type')->>'id' AS change_to,
	CASE
		WHEN (((change_value->'links')[0]->'to')->'type')->>'id' IS NULL THEN ((change_value->'links')[0]->'from')->>'direction'
		ELSE ((change_value->'links')[0]->'to')->>'direction'
	END AS direction,
	CASE
		WHEN (((change_value->'links')[0]->'to')->'type')->>'id' IS NULL THEN (((change_value->'links')[0]->'from')->'object')->>'key'
		ELSE (((change_value->'links')[0]->'to')->'object')->>'key'
	END AS linked_task,
	(change_value->'updatedBy')->>'id' AS updated_by,
	(change_value->>'updatedAt')::timestamptz AT TIME ZONE 'Europe/Moscow' AS updated_at
FROM {{ source('task_tracker', 'change_log') }}
WHERE change_value->'links' IS NOT NULL