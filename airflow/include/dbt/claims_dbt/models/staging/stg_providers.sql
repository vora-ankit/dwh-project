select
    providerid          as provider_id,
    npi,
    providername        as provider_name,
    providertype        as provider_type,
    specialty,
    city,
    state,
    postalcode          as postal_code,
    networkstatus       as network_status,
    contractstartdate   as contract_start_date,
    isactive            as is_active,
    lastupdatedat       as last_updated_at,
    rowversionnumber    as row_version,
    _loaded_at
from {{ source('raw', 'providers') }}
qualify row_number() over (partition by providerid order by rowversionnumber desc) = 1