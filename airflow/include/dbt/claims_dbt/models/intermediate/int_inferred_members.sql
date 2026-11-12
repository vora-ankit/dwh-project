-- Member IDs that claims refer to but that are not in the member data (yet).
-- Each becomes a placeholder ("inferred") row in dim_member and dim_member_history,
-- so the claim still joins to a member. When the real member arrives, it drops out of
-- this list and the next build uses the real member instead.
select distinct c.member_id
from {{ ref('stg_claims') }} c
left join {{ ref('stg_members') }} m
    on c.member_id = m.member_id
where c.member_id is not null
  and m.member_id is null
