{{
    config(
        materialized='incremental',
        unique_key=['task_id', 'follower_id'],
        incremental_strategy = 'merge'
    )
}}


-- По умолчанию сегодняшняя дата
{% set def_value = run_started_at.strftime("%Y-%m-%d")  %}


WITH followers_unset AS 
	(SELECT
		task_id,
		unnest(followers) AS follower
	FROM {{ ref('ods_model_tasks') }}
{% if is_incremental() %}
	WHERE updated_at::date = '{{ var("update_dt", def_value) }}' OR created_at::date = '{{ var("update_dt", def_value) }}'
{% endif %}
    )
SELECT 
	dt.task_uid_sk AS task_id,
	du.user_uid_sk AS follower_id
FROM followers_unset AS fu
JOIN {{ ref('dds_model_tasks') }}  AS dt
	ON fu.task_id = dt.task_uid_bk
JOIN {{ ref('dds_model_users') }} AS du
	ON fu.follower = du.user_uid_bk