-- One row per month that breaks: Parquet footer rows (manifest) = bronze = silver + quarantine = fact.
-- Months come from all five tables, so a month missing from any of them is reported, and an empty
-- manifest is a failure, not a pass. Zero rows returned = pass.
with manifest as (
    select file_month, rows as metadata_rows from {{ source('bronze', 'ingest_manifest') }}
),
bronze as (
    select file_month, count(*) as bronze_rows from {{ source('bronze', 'yellow_trips') }} group by file_month
),
silver as (
    select file_month, count(*) as silver_rows from {{ source('silver', 'yellow_trips') }} group by file_month
),
quarantine as (
    select file_month, count(*) as quarantine_rows
    from {{ source('silver', 'yellow_trips_quarantine') }} group by file_month
),
fact as (
    select file_month, count(*) as fact_rows from {{ ref('fct_trips') }} group by file_month
),
months as (
    select file_month from manifest
    union select file_month from bronze
    union select file_month from silver
    union select file_month from quarantine
    union select file_month from fact
),
checked as (
    select
        mo.file_month,
        m.metadata_rows,
        b.bronze_rows,
        s.silver_rows,
        coalesce(q.quarantine_rows, 0) as quarantine_rows,
        f.fact_rows
    from months mo
    left join manifest m using (file_month)
    left join bronze b using (file_month)
    left join silver s using (file_month)
    left join quarantine q using (file_month)
    left join fact f using (file_month)
)
select * from checked
where metadata_rows is null
   or bronze_rows is null
   or silver_rows is null
   or fact_rows is null
   or metadata_rows <> bronze_rows
   or bronze_rows <> silver_rows + quarantine_rows
   or fact_rows <> silver_rows
union all
select 'no months in the manifest', null, null, null, null, null
where (select count(*) from manifest) = 0
