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
    dbt_valid_to is null                        as is_current
from {{ ref('snap_providers') }}