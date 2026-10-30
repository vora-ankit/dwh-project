select member_id
from {{ ref('dim_member_history') }}
where is_current
group by member_id
having count(*) <> 1