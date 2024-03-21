SELECT 
	(component_value->>'id')::int AS component_id,
	component_value->>'name' AS name,
	((component_value->'queue')->>'id')::int AS queue,
	component_value->>'description' AS description,
	(component_value->'lead')->>'id' AS created_by
FROM {{ source('task_tracker', 'components') }}