{{
    config(
        materialized='incremental',
        unique_key='component_uid_bk',
        incremental_strategy = 'merge',
        merge_exclude_columns = ['component_uid_sk']
    )
}}


SELECT
    gen_random_uuid() AS component_uid_sk,
	oc.component_id AS component_uid_bk,
	oc."name",
	dq.queue_uid_sk AS queue,
	du.user_uid_sk AS created_by,
	oc.description
FROM  {{ ref('ods_model_components') }} AS oc
LEFT JOIN {{ ref('dds_model_users') }}  AS du
	ON oc.created_by = du.user_uid_bk
LEFT JOIN {{ ref('dds_model_queues') }} AS dq 
	ON oc.queue = dq.queue_uid_bk