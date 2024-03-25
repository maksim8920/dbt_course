{{
    config(
        materialized='incremental',
        unique_key=['task_id', 'comment_uid_bk'],
        incremental_strategy = 'merge',
        merge_exclude_columns  = ['comment_uid_sk']
    )
}}

-- По умолчанию сегодняшняя дата
{% set def_value = run_started_at.strftime("%Y-%m-%d")  %}

SELECT
    gen_random_uuid() AS comment_uid_sk, 
	dt.task_uid_sk AS task_id,
	oc.comment_id AS comment_uid_bk,
	oc.created_at,
	duc.user_uid_sk AS created_by,
	oc.updated_at,
	duu.user_uid_sk AS updated_by,
	oc."text",
	oc."version"
FROM {{ ref('ods_model_comments') }} AS oc
LEFT JOIN {{ ref('dds_model_tasks') }} AS dt
	ON oc.task_id = dt.task_uid_bk 
LEFT JOIN {{ ref('dds_model_users') }} AS duc
	ON oc.created_by = duc.user_uid_bk
LEFT JOIN {{ ref('dds_model_users') }} AS duu 
	ON oc.updated_by = duu.user_uid_bk
{% if is_incremental() %}
WHERE oc.created_at::date = '{{ var("update_dt", def_value) }}' OR oc.updated_at::date = '{{ var("update_dt", def_value) }}'
{% endif %}