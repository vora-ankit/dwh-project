select
    claimid       as claim_id,
    captured_at
from {{ source('raw', 'claim_keys') }}
qualify captured_at = max(captured_at) over ()