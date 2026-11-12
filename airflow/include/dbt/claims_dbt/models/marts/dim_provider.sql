with real_providers as (
    select
        provider_id,
        npi,
        provider_name,
        provider_type,
        specialty,
        city,
        state,
        postal_code,
        network_status,
        network_status = 'IN' as is_in_network,
        contract_start_date,
        is_active,
        last_updated_at,
        false as is_inferred
    from {{ ref('stg_providers') }}
),

-- Placeholders for providers that claims refer to but that haven't arrived yet
inferred_providers as (
    select
        provider_id,
        null::varchar          as npi,
        'Unknown (inferred)'   as provider_name,
        'Unknown'              as provider_type,
        null::varchar          as specialty,
        null::varchar          as city,
        null::varchar          as state,
        null::varchar          as postal_code,
        'Unknown'              as network_status,
        null::boolean          as is_in_network,
        null::date             as contract_start_date,
        null::boolean          as is_active,
        null::timestamp_ntz    as last_updated_at,
        true                   as is_inferred
    from {{ ref('int_inferred_providers') }}
)

select * from real_providers
union all
select * from inferred_providers
