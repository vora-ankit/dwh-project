-- Version reconciliation: fails if the warehouse has missed an insert or an update.
--
-- Each pipeline run does: extract rows -> capture keys (with rowversions) -> load -> dbt.
-- So the PREVIOUS capture was taken before THIS run's row extract. Everything that existed in the
-- source at the previous capture must therefore be in the warehouse now, at that version or newer.
--   * claim in the previous capture but missing from stg_claims        -> missed insert
--   * stg_claims row_version older than the previous capture's version -> missed update
-- Comparing against the LATEST capture instead would give false alarms: a claim changed in the few
-- seconds between this run's extract and its key capture is legitimately one version behind.
--
-- Only claims still present in the latest capture are checked (a claim deleted since the previous
-- capture can't be extracted any more; is_deleted handles it).
-- Captures from before row_version existed have NULL versions and are skipped.
-- Caveat: assumes a row extract ran after the previous capture (always true in the DAG). Running
-- extract_keys twice by hand without extract_tables in between can cause a false failure.

with captures as (
    select
        captured_at,
        dense_rank() over (order by captured_at desc) as capture_rank
    from (select distinct captured_at from {{ source('raw', 'claim_keys') }})
),

previous_capture as (
    select k.claimid as claim_id, k.row_version
    from {{ source('raw', 'claim_keys') }} k
    join captures c
      on k.captured_at = c.captured_at
     and c.capture_rank = 2
    where k.row_version is not null
),

latest_capture as (
    select k.claimid as claim_id
    from {{ source('raw', 'claim_keys') }} k
    join captures c
      on k.captured_at = c.captured_at
     and c.capture_rank = 1
)

select
    p.claim_id,
    p.row_version                                   as source_version_at_previous_capture,
    s.row_version                                   as warehouse_version,
    case when s.claim_id is null then 'missed insert' else 'missed update' end as problem
from previous_capture p
join latest_capture l
  on p.claim_id = l.claim_id
left join {{ ref('stg_claims') }} s
  on p.claim_id = s.claim_id
where s.claim_id is null
   or s.row_version < p.row_version
