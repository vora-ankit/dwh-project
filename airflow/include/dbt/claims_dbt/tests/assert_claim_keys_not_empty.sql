select 'claim key list is empty' as problem
where (select count(*) from {{ ref('stg_claim_keys') }}) = 0