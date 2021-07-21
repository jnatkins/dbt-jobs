{{ config(materialized='view') }}

{% set tech_list = [
        'dbt','airflow','sql','python','snowflake','etl','looker','tableau','elt',
        'bigquery','redshift','fivetran','spark','postgres','java','kafka',
        'talend','docker','kubernetes','gitlab','github','git','databricks',
        'hadoop','matillion','powerbi','mysql','informatica','pandas','dagster',
        'jupyter','scikit','bitbucket','confluent','cloudera','jinja' 
    ] 
%}

with stg_job_descriptions as (

    select * from {{ ref('stg_job_descriptions') }}

),

count_technologies as (
    select
        count(*) as total_cnt,
    {% for tech in tech_list %}
        sum((job_description ilike '%{{ tech }}%')::int) as {{ tech }}_cnt{% if not loop.last %},{% endif %}
    {% endfor %}

    from stg_job_descriptions

),

pctage_technologies as (
    select
    {% for tech in tech_list %}
        round(({{ tech }}_cnt::numeric / total_cnt::numeric) * 100.0,1) as {{ tech }}{% if not loop.last %},{% endif %}
    {% endfor %}

    from count_technologies

)

select * from pctage_technologies