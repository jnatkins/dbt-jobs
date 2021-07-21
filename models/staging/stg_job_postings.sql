{{ config(materialized='table') }}

select 
    job_id,
    title,
    company,
    location,
    job_function,
    industries,
    seniority::varchar(100)
from {{ ref('raw_job_postings') }}