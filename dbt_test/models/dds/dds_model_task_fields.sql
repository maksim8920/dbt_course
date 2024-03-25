{{
    config(
        materialized='incremental',
        unique_key='task_field_uid_bk',
        incremental_strategy = 'merge',
        merge_update_columns = ['name', 'key']
    )
}}


SELECT
    gen_random_uuid() AS task_field_uid_sk,
	task_field_id AS task_field_uid_bk,
	"key",
	"name" 
FROM {{ ref('ods_model_task_fields') }}