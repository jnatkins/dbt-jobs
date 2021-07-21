{{ dbt_utils.unpivot(
  relation=ref('job_descriptions__summarize_tech'),
  cast_to='float',
  field_name='technology',
  value_name='in_job_desc_pct'
) }}