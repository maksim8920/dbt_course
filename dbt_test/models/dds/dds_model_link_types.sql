{{
    config(
        materialized='incremental',
        unique_key='link_uid_bk',
        incremental_strategy = 'merge',
        merge_update_columns = ['inward', 'outward']
    )
}}



SELECT
    gen_random_uuid() AS link_uid_sk,
	link_id AS link_uid_bk,
	inward,
	outward
FROM {{ ref('ods_model_link_types') }}