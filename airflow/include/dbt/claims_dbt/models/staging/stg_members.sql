select
    memberid            as member_id,
    firstname           as first_name,
    lastname            as last_name,
    dateofbirth         as date_of_birth,
    gender,
    state,
    postalcode          as postal_code,
    plantype            as plan_type,
    groupid             as group_id,
    effectivedate       as effective_date,
    terminationdate     as termination_date,
    lastupdatedat       as last_updated_at,
    rowversionnumber    as row_version,
    _loaded_at
from {{ source('raw', 'members') }}
qualify row_number() over (partition by memberid order by rowversionnumber desc) = 1