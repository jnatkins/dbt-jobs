{{ config(materialized='view') }}

{% set tech_list = [
        'dbt','airflow','sql','python','snowflake','etl','looker','tableau','elt',
        'bigquery','redshift','fivetran','spark','postgres','java','kafka',
        'talend','docker','kubernetes','gitlab','github','git','databricks',
        'hadoop','matillion','powerbi','mysql','informatica','pandas','dagster',
        'jupyter','scikit','bitbucket','confluent','cloudera','jinja','scala','metabase',
        'superset','streaming','batch','aws','google','azure','nosql','influence','analysis',
        'stakeholder','insight','align','test','develop','share','collaborate','notebook',
        'prepare','curate','maintain','architect','design'
    ] 
%}

with stg_job_descriptions as (

    select * from {{ ref('stg_job_descriptions') }}

),

job_postings as (

    select job_id, cleaned_title from {{ ref('job_postings__clean_titles') }}

),

joined as (

    select 
        p.cleaned_title,
        jd.job_description
    from stg_job_descriptions jd join job_postings p
        on (jd.job_id = p.job_id)

),

count_technologies as (
    select
        cleaned_title,
        count(*) as total_cnt,
    {% for tech in tech_list %}
        sum((job_description ilike '%{{ tech }}%')::int) as "{{ tech }}_cnt"{% if not loop.last %},{% endif %}
    {% endfor %}

    from joined
    group by cleaned_title

),

pctage_technologies as (
    select
        cleaned_title,
    {% for tech in tech_list %}
        round(("{{ tech }}_cnt"::numeric / total_cnt::numeric) * 100.0,1) as "{{ tech }}"{% if not loop.last %},{% endif %}
    {% endfor %}

    from count_technologies

)

select * from pctage_technologies