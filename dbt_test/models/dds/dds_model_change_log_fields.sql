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
	clf.change_id,
	tf.task_field_uid_sk AS change_field_id,
	clf.change_from,
	clf.change_to,
	du.user_uid_sk AS updated_by,
	clf.updated_at 
FROM {{ ref('ods_model_change_log_fields') }} AS clf
LEFT JOIN {{ ref('dds_model_tasks') }} AS dt 
	ON clf.task_id = dt.task_uid_bk
LEFT JOIN {{ ref('dds_model_task_fields') }} AS tf 
	ON clf.change_field_id = tf.task_field_uid_bk 
LEFT JOIN {{ ref('dds_model_users') }} AS du 
	ON clf.updated_by = du.user_uid_bk
WHERE clf.updated_at::date = '{{ var("update_dt", def_value) }}'