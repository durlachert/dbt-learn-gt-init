
select *
from {{ ref('stg_stripe_payments') }}  -- or stg_stripe__payments if that's your actual name
where amount = 13212312321312