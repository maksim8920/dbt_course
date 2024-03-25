{{
    config(
        materialized='incremental',
        unique_key='task_uid_bk',
        incremental_strategy = 'merge',
        merge_exclude_columns  = ['task_uid_sk']
    )
}}

-- По умолчанию сегодняшняя дата
{% set def_value = run_started_at.strftime("%Y-%m-%d")  %}


SELECT
    gen_random_uuid() AS task_uid_sk,
	ot.task_id AS task_uid_bk,
	ot.title,
	ot.description,
	ot.created_at,
	ot.updated_at,
	ot.resolved_at,
	duc.user_uid_sk AS created_by,
	dua.user_uid_sk AS assignee,
	dr.user_uid_sk AS resolved_by,
	dt.task_type_uid_sk AS "type",
	ds.status_uid_sk AS status,
	dp.priority_uid_sk AS priority,
	dq.queue_uid_sk AS queue,
	ot.story_points,
	ot.shirt_score,
	ot.deadline 
FROM {{ ref('ods_model_tasks') }} AS ot
LEFT JOIN {{ ref('dds_model_users') }} AS duc
	ON ot.created_by = duc.user_uid_bk
LEFT JOIN {{ ref('dds_model_users') }} AS dua 
	ON ot.assignee = dua.user_uid_bk 
LEFT JOIN {{ ref('dds_model_users') }} AS dr 
	ON ot.resolved_by = dr.user_uid_bk
LEFT JOIN {{ ref('dds_model_task_types') }} AS dt 
	ON ot."type" = dt.task_type_uid_bk 
LEFT JOIN {{ ref('dds_model_statuses') }} AS ds
	ON ot.status = ds.status_uid_bk
LEFT JOIN {{ ref('dds_model_priorities') }} AS dp
	ON ot.priority = dp.priority_uid_bk
LEFT JOIN {{ ref('dds_model_queues') }} AS dq
	ON ot.queue = dq.queue_uid_bk
{% if is_incremental() %}
WHERE ot.updated_at::date = '{{ var("update_dt", def_value) }}'
{% endif %}
