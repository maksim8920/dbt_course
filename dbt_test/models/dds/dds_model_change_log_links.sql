{{
    config(
        materialized='incremental',
        unique_key=['task_id', 'change_id'],
        incremental_strategy = 'merge'
    )
}}


-- По умолчанию сегодняшняя дата
{% set def_value = run_started_at.strftime("%Y-%m-%d")  %}


SELECT 
	dt.task_uid_sk AS task_id,
	cll.change_id,
	dltf.link_uid_sk AS change_from,
	dltt.link_uid_sk AS change_to,
	cll.direction,
	cll.linked_task,
	du.user_uid_sk AS updated_by,
	cll.updated_at
FROM {{ ref('ods_model_change_log_links') }} AS cll
LEFT JOIN {{ ref('dds_model_tasks') }} AS dt 
	ON cll.task_id = dt.task_uid_bk
LEFT JOIN {{ ref('dds_model_link_types') }} AS dltf
	ON cll.change_from = dltf.link_uid_bk 
LEFT JOIN {{ ref('dds_model_link_types') }} AS dltt
	ON cll.change_to  = dltt.link_uid_bk 
LEFT JOIN {{ ref('dds_model_users') }} AS du 
	ON cll.updated_by = du.user_uid_bk
{% if is_incremental() %}
WHERE cll.updated_at::date = '{{ var("update_dt", def_value) }}'
{% endif %}