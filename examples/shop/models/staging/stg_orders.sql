-- Orders that weren't cancelled. ordered_at is UTC.
create or replace table staging.orders as
select order_id, customer_id, cast(ordered_at as date) as order_date, ordered_at, amount
from raw.orders
where status != 'cancelled';
