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
    dbt_valid_to is null                        as is_current
from {{ ref('snap_members') }}