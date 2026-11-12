-- Lists every inferred (placeholder) member and provider. Severity is WARN: the build keeps
-- going, but the output shows how many claims are waiting for their real member or provider.
{{ config(severity='warn') }}

select 'member' as kind, member_id as id
from {{ ref('int_inferred_members') }}

union all

select 'provider' as kind, provider_id as id
from {{ ref('int_inferred_providers') }}
