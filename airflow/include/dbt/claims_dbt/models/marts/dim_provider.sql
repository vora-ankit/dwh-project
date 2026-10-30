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
    last_updated_at
from {{ ref('stg_providers') }}