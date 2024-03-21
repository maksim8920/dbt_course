SELECT 
	task_id,
	change_id,
	((((change_value->'comments')->'updated')[0]->'comment')->>'id')::int AS comment_id,
	((change_value->'comments')->'updated')[0]->>'from' AS change_from,
	CASE
		WHEN ((change_value->'comments')->'updated')[0]->>'removedReaction' IS NOT NULL THEN 'removed_reaction'
		WHEN ((change_value->'comments')->'updated')[0]->>'to' IS NULL THEN ((change_value->'comments')->'updated')[0]->>'addedReaction'
		ELSE ((change_value->'comments')->'updated')[0]->>'to'
	END AS change_to,
	(change_value->'updatedBy')->>'id' AS updated_by,
	(change_value->>'updatedAt')::timestamptz AT TIME ZONE 'Europe/Moscow' AS updated_at
FROM {{ source('task_tracker', 'change_log') }}
WHERE 1=1
	AND (change_value->'comments')->>'updated' IS NOT NULL