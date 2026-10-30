with claims as (
    select * from {{ ref('stg_claims') }}
)

select
    c.claim_id,
    c.claim_number,
    to_number(to_char(c.date_received, 'YYYYMMDD'))     as date_received_key,
    to_number(to_char(c.service_date_from, 'YYYYMMDD')) as service_date_key,
    c.member_id,
    m.member_key,
    c.provider_id,
    p.provider_key,
    c.group_id,
    c.claim_type,
    c.form_type,
    c.status,
    c.processing_status,
    c.amt_maximum_allowable,
    c.amt_negotiated,
    c.amt_maximum_allowable - c.amt_negotiated          as amt_discount,
    datediff(day, c.date_received, c.date_paid)         as days_to_pay,
    c.status in ('PAID', 'CLOSED')                      as is_paid,
    c.last_updated_at,
	k.claim_id is null                                  as is_deleted
from claims c
left join {{ ref('stg_claim_keys') }} k
    on c.claim_id = k.claim_id
left join {{ ref('dim_member_history') }} m
    on c.member_id = m.member_id
   and c.date_received >= m.valid_from
   and c.date_received <  m.valid_to
left join {{ ref('dim_provider_history') }} p
    on c.provider_id = p.provider_id
   and c.date_received >= p.valid_from
   and c.date_received <  p.valid_to