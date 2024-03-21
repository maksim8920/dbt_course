SELECT 
	task_id,
	change_id,
	((change_value->'fields')[0]->'field')->>'id' AS change_field_id,
	CASE
		WHEN jsonb_typeof((change_value->'fields')[0]->'from') = 'object' THEN ((change_value->'fields')[0]->'from')->>'id'
		WHEN jsonb_typeof((change_value->'fields')[0]->'from') = 'array' AND ((change_value->'fields')[0]->'from')[0]->'id' IS NOT NULL 
			THEN (SELECT (ARRAY(SELECT (element->>'id') FROM jsonb_array_elements((change_value->'fields')[0]->'from') AS element))::text)
		ELSE (change_value->'fields')[0]->>'from'
	END AS change_from,
	CASE
		WHEN jsonb_typeof((change_value->'fields')[0]->'to') = 'object' THEN ((change_value->'fields')[0]->'to')->>'id'
		WHEN jsonb_typeof((change_value->'fields')[0]->'to') = 'array' AND ((change_value->'fields')[0]->'to')[0]->'id' IS NOT NULL 
			THEN (SELECT (ARRAY(SELECT (element->>'id') FROM jsonb_array_elements((change_value->'fields')[0]->'to') AS element))::text)
		ELSE (change_value->'fields')[0]->>'to'
	END AS change_to,
	(change_value->'updatedBy')->>'id' AS updated_by,
	(change_value->>'updatedAt')::timestamptz AT TIME ZONE 'Europe/Moscow' AS updated_at
FROM {{ source('task_tracker', 'change_log') }}
WHERE change_value->'fields' IS NOT NULL