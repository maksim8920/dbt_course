{{
    config(
        materialized='incremental',
        unique_key=['task_id', 'component_id'],
        incremental_strategy = 'merge'
    )
}}


-- По умолчанию сегодняшняя дата
{% set def_value = run_started_at.strftime("%Y-%m-%d")  %}


WITH components_unset AS 
	(SELECT
		task_id,
		unnest(components) AS component
	FROM {{ ref('ods_model_tasks') }}
{% if is_incremental() %}
	WHERE updated_at::date = '{{ var("update_dt", def_value) }}' OR created_at::date = '{{ var("update_dt", def_value) }}'
{% endif %}
    )
SELECT 
	dt.task_uid_sk AS task_id,
	dc.component_uid_sk AS component_id
FROM components_unset AS cu
JOIN {{ ref('dds_model_tasks') }} AS dt
	ON cu.task_id = dt.task_uid_bk
JOIN {{ ref('dds_model_components') }} AS dc 
	ON cu.component = dc.component_uid_bk