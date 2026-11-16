create or replace table staging.payments as
select payment_id, order_id, installment, amount, cast(paid_at as date) as paid_on
from raw.payments;
