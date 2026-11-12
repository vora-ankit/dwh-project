with versions as (
    select
        dbt_scd_id                                  as provider_key,
        provider_id,
        npi,
        provider_name,
        provider_type,
        specialty,
        city,
        state,
        postal_code,
        network_status,
        network_status = 'IN'                       as is_in_network,
        case
            when row_number() over (partition by provider_id order by dbt_valid_from) = 1
                then '1900-01-01'::timestamp_ntz
            else dbt_valid_from
        end                                         as valid_from,
        coalesce(dbt_valid_to, '9999-12-31'::timestamp_ntz) as valid_to,
        dbt_valid_to is null                        as is_current,
        false                                       as is_inferred
    from {{ ref('snap_providers') }}
),

inferred as (
    select
        md5('inferred-provider|' || provider_id)    as provider_key,
        provider_id,
        null::varchar                               as npi,
        'Unknown (inferred)'                        as provider_name,
        'Unknown'                                   as provider_type,
        null::varchar                               as specialty,
        null::varchar                               as city,
        null::varchar                               as state,
        null::varchar                               as postal_code,
        'Unknown'                                   as network_status,
        null::boolean                               as is_in_network,
        '1900-01-01'::timestamp_ntz                 as valid_from,
        '9999-12-31'::timestamp_ntz                 as valid_to,
        true                                        as is_current,
        true                                        as is_inferred
    from {{ ref('int_inferred_providers') }}
)

select * from versions
union all
select * from inferred
