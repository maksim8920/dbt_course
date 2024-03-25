{{
    config(
        materialized='incremental',
        unique_key=['task_id', 'board_id'],
        incremental_strategy = 'merge'
    )
}}


-- По умолчанию сегодняшняя дата
{% set def_value = run_started_at.strftime("%Y-%m-%d")  %}


WITH boards_unset AS 
	(SELECT
		task_id,
		unnest(boards) AS board
	FROM {{ ref('ods_model_tasks') }}
{% if is_incremental() %}
	WHERE updated_at::date = '{{ var("update_dt", def_value) }}' OR created_at::date = '{{ var("update_dt", def_value) }}'
{% endif %}
    )
SELECT 
	dt.task_uid_sk AS task_id,
	db.board_uid_sk AS board_id
FROM boards_unset AS bu
JOIN {{ ref('dds_model_tasks') }} AS dt
	ON bu.task_id = dt.task_uid_bk
JOIN {{ ref('dds_model_boards') }} AS db 
	ON bu.board = db.board_uid_bk