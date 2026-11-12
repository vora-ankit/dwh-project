with members as (
    select
        *,
        floor(datediff(day, date_of_birth, current_date()) / 365.25) as age
    from {{ ref('stg_members') }}
),

real_members as (
    select
        member_id,
        first_name || ' ' || last_name as full_name,
        date_of_birth,
        age,
        case
            when age is null then 'Unknown'
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
        coalesce(termination_date > current_date(), true) as is_active,
        false as is_inferred
    from members
),

-- Placeholders for members that claims refer to but that haven't arrived yet
inferred_members as (
    select
        member_id,
        'Unknown (inferred)'  as full_name,
        null::date            as date_of_birth,
        null::number          as age,
        'Unknown'             as age_band,
        null::varchar         as gender,
        null::varchar         as state,
        null::varchar         as postal_code,
        'Unknown'             as plan_type,
        null::number          as group_id,
        null::date            as effective_date,
        null::date            as termination_date,
        null::boolean         as is_active,
        true                  as is_inferred
    from {{ ref('int_inferred_members') }}
)

select * from real_members
union all
select * from inferred_members
