{{
    config(
        materialized='incremental',
        unique_key='queue_uid_bk',
        incremental_strategy = 'merge',
        merge_update_columns = ['key', 'name', 'description', 'created_by']
    )
}}


SELECT
    gen_random_uuid() AS queue_uid_sk,
	oq.queue_id AS queue_uid_bk,
	oq."key",
	oq."name",
	oq.description,
	du.user_uid_sk AS created_by
FROM {{ ref('ods_model_queues') }} AS oq
LEFT JOIN {{ ref('dds_model_users') }} AS du
	ON oq.created_by = du.user_uid_bk