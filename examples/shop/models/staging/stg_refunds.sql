create or replace table staging.refunds as
select refund_id, order_id, amount, cast(refunded_at as date) as refunded_on
from raw.refunds;
