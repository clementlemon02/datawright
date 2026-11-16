-- Daily revenue for the finance dashboard: paid orders by order date.
create or replace table mart.revenue_daily as
select o.order_date,
       sum(o.amount) as revenue,
       count(*) as orders
from staging.orders o
join staging.payments p on p.order_id = o.order_id
group by o.order_date;
