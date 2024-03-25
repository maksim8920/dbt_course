{{
    config(
        materialized='incremental',
        unique_key='status_uid_bk',
        incremental_strategy = 'merge',
        merge_update_columns = ['name', 'key']
    )
}}


SELECT 
    gen_random_uuid() as status_uid_sk,
	status_id AS status_uid_bk,
	"key",
	name
FROM {{ ref('ods_model_statuses') }}