-- Stand-in for the shop's production database: October 2026 orders, payments, refunds and customers.
create schema if not exists raw;

create or replace table raw.customers as
select i as customer_id, ['US', 'GB', 'DE', 'SG', 'AU'][1 + i % 5] as country, date '2026-01-01' + cast(i * 3 as integer) as signed_up_on
from range(1, 201) t(i);

create or replace table raw.orders as
select i as order_id,
       1 + (i * 7) % 200 as customer_id,
       timestamp '2026-10-01' + ((i * 893) % (31 * 86400)) * interval 1 second as ordered_at,
       cast(10 + (i * 37) % 190 + (i % 100) / 100 as decimal(10, 2)) as amount,
       case when i % 25 = 0 then 'cancelled' else 'complete' end as status
from range(1, 3001) t(i);

-- one payment per order; every tenth order is paid in two installments
create or replace table raw.payments as
select row_number() over (order by order_id, installment) as payment_id, order_id, installment, amount, paid_at
from (
  select order_id, 1 as installment, case when order_id % 10 = 0 then round(amount / 2, 2) else amount end as amount, ordered_at as paid_at
  from raw.orders where status = 'complete'
  union all
  select order_id, 2, amount - round(amount / 2, 2), ordered_at + interval 30 day
  from raw.orders where status = 'complete' and order_id % 10 = 0
);

create or replace table raw.refunds as
select order_id as refund_id, order_id, amount, ordered_at + (1 + order_id % 5) * interval 1 day as refunded_at
from raw.orders where status = 'complete' and order_id % 17 = 0;
