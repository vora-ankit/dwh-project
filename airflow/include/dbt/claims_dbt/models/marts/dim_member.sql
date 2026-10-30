with members as (
    select
        *,
        floor(datediff(day, date_of_birth, current_date()) / 365.25) as age
    from {{ ref('stg_members') }}
)

select
    member_id,
    first_name || ' ' || last_name as full_name,
    date_of_birth,
    age,
    case
        when age < 18 then '0-17'
        when age < 35 then '18-34'
        when age < 50 then '35-49'
        when age < 65 then '50-64'
        else '65+'
    end as age_band,
    gender,
    state,
    postal_code,
    plan_type,
    group_id,
    effective_date,
    termination_date,
    coalesce(termination_date > current_date(), true) as is_active
from members