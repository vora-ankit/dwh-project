-- Provider IDs that claims refer to but that are not in the provider data (yet).
select distinct c.provider_id
from {{ ref('stg_claims') }} c
left join {{ ref('stg_providers') }} p
    on c.provider_id = p.provider_id
where c.provider_id is not null
  and p.provider_id is null
