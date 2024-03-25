{{
    config(
        materialized='incremental',
        unique_key='priority_uid_bk',
        incremental_strategy = 'merge',
        merge_update_columns = ['name', 'key']
    )
}}


SELECT
    gen_random_uuid() as priority_uid_sk,
	priority_id AS priority_uid_bk,
	"key",
	"name" 
FROM {{ ref('ods_model_priorities') }}