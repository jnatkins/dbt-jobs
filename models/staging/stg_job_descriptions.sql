{{ config(materialized='table') }}

select * from {{ ref('raw_job_descriptions') }}