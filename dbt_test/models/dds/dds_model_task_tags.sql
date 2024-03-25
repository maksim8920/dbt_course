{{
    config(
        materialized='incremental',
        unique_key=['task_id', 'tag'],
        incremental_strategy = 'merge'
    )
}}


-- По умолчанию сегодняшняя дата
{% set def_value = run_started_at.strftime("%Y-%m-%d")  %}


WITH tags_unset AS 
	(SELECT
		task_id,
		unnest(tags) AS tag
	FROM {{ ref('ods_model_tasks') }}
{% if is_incremental() %}
	WHERE updated_at::date = '{{ var("update_dt", def_value) }}' OR created_at::date = '{{ var("update_dt", def_value) }}'
{% endif %}
    )
SELECT 
	dt.task_uid_sk AS task_id,
	tu.tag AS tag
FROM tags_unset AS tu
JOIN {{ ref('dds_model_tasks') }} AS dt
	ON tu.task_id = dt.task_uid_bk