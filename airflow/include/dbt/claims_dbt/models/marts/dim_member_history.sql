with versions as (
    select
        dbt_scd_id                                  as member_key,
        member_id,
        first_name || ' ' || last_name              as full_name,
        date_of_birth,
        gender,
        state,
        postal_code,
        plan_type,
        group_id,
        effective_date,
        termination_date,
        case
            when row_number() over (partition by member_id order by dbt_valid_from) = 1
                then '1900-01-01'::timestamp_ntz
            else dbt_valid_from
        end                                         as valid_from,
        coalesce(dbt_valid_to, '9999-12-31'::timestamp_ntz) as valid_to,
        dbt_valid_to is null                        as is_current,
        false                                       as is_inferred
    from {{ ref('snap_members') }}
),

-- One open-ended placeholder version per inferred member, so point-in-time joins always match.
-- The key is a stable hash, so it is the same on every build until the real member arrives.
inferred as (
    select
        md5('inferred-member|' || member_id)        as member_key,
        member_id,
        'Unknown (inferred)'                        as full_name,
        null::date                                  as date_of_birth,
        null::varchar                               as gender,
        null::varchar                               as state,
        null::varchar                               as postal_code,
        'Unknown'                                   as plan_type,
        null::number                                as group_id,
        null::date                                  as effective_date,
        null::date                                  as termination_date,
        '1900-01-01'::timestamp_ntz                 as valid_from,
        '9999-12-31'::timestamp_ntz                 as valid_to,
        true                                        as is_current,
        true                                        as is_inferred
    from {{ ref('int_inferred_members') }}
)

select * from versions
union all
select * from inferred
