select
    claimid                     as claim_id,
    claimnumber                 as claim_number,
    formtype                    as form_type,
    claimtype                   as claim_type,
    servicedatefrom             as service_date_from,
    servicedateto               as service_date_to,
    datereceived                as date_received,
    datepaid                    as date_paid,
    memberid                    as member_id,
    subscriberid                as subscriber_id,
    groupid                     as group_id,
    providerid                  as provider_id,
    status,
    processingstatus            as processing_status,
    amtnegotiated               as amt_negotiated,
    amtmaximumallowable         as amt_maximum_allowable,
    lastupdatedat               as last_updated_at,
    lastactiverowversionnumber  as row_version,
    _loaded_at
from {{ source('raw', 'claims') }}
qualify row_number() over (
    partition by claimid
    order by lastactiverowversionnumber desc
) = 1