{{
    config(
        materialized='incremental',
        unique_key='user_uid_bk',
        incremental_strategy = 'merge',
        merge_update_columns = ['name', 'email']
    )
}}

SELECT 
    gen_random_uuid() as user_uid_sk,
	user_id AS user_uid_bk,
	"name",
	email,
	first_login 
FROM {{ ref('ods_model_users') }}