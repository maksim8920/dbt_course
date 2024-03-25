SELECT 
	link_value->>'id' AS link_id,
	link_value->>'inward' AS inward,
	link_value->>'outward' AS outward
FROM {{ source('task_tracker', 'link_types') }}