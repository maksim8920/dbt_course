{{
    config(
        materialized='incremental',
        unique_key='board_uid_bk',
        incremental_strategy = 'merge',
        merge_update_columns = ['name']
    )
}}



SELECT 
    gen_random_uuid() as board_uid_sk,
	board_id AS board_uid_bk,
	name
FROM {{ ref('ods_model_boards') }}