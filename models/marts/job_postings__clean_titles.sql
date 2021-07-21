{{ config(materialized='view') }}

with stg_job_postings as (

    select * from {{ ref('stg_job_postings') }}

),

filter_companies as (

    select * from stg_job_postings
    where company not in (
        'Census',
        'Bigeye',
        'Terrain',
        'Novare Interim & Recruitment AB'
    ) and company not ilike '%dbt%'

),

filter_titles as (

    select * from filter_companies
    where title not ilike '%Psyk%'
      and title not ilike '%Psych%'
      and title not ilike '%Eating Disorder Recovery%'
      and title not ilike '%Security Specialist%'

),

extract_seniority as (
    select 
        job_id,
        title,
        company,
        location,
        job_function,
        industries,
        case
            when title ilike '%Sr%' then 'Experienced'
            when title ilike '%Senior%' then 'Experienced'
            when title ilike '%II%' then 'Experienced'
            when title ilike '%Lead%' then 'Experienced'
            when title ilike '%Manager,%' then 'Manager'
            when title ilike '%Dir%' then 'Manager'
            when title ilike '%Head%' then 'Manager'
            when title ilike '%President%' then 'Executive'
            else 'Entry-Level'
        end as seniority
    from filter_titles
),

clean_titles as (
    select 
        job_id,
        title,
        company,
        location,
        job_function,
        industries,
        seniority,
        case
            when title ilike '%Product Manager%' then 'Product Manager'
            when title ilike '%Data Engineer%' then 'Data Engineer'
            when title ilike '%Data Architect%' then 'Data Architect'
            when title ilike '%Business Intelligence Architect%' then 'Data Architect'
            when title ilike '%Analytics Engineer%' then 'Analytics Engineer'
            when title ilike '%Analyst%' then 'Analyst'
            when title ilike '%Data Scientist%' then 'Data Scientist'
            when title ilike '%Software Engineer%' then 'Software Engineer'
            when title ilike '%Business Intelligence Engineer%' then 'BI Engineer'
            when title ilike '%Business Intelligence Developer%' then 'BI Engineer'
            when title ilike '%BI Developer%' then 'BI Engineer'
            when title ilike '%BI Engineer%' then 'BI Engineer'
            when title ilike '%Director%of%Analytics%' then 'Director, Analytics'
            when title ilike '%Head%of%Analytics%' then 'Director, Analytics'
            else 'Other'
        end as cleaned_title
    from extract_seniority
),

final as (
    select
        job_id,
        title,
        cleaned_title,
        company,
        location,
        job_function,
        industries,
        seniority
    from clean_titles
)

select * from final