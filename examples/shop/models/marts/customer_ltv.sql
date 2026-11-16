-- Lifetime value per customer, net of refunds.
create or replace table mart.customer_ltv as
select c.customer_id, c.country,
       count(o.order_id) as orders,
       coalesce(sum(o.amount), 0) - coalesce(sum(r.amount), 0) as net_revenue
from raw.customers c
left join staging.orders o on o.customer_id = c.customer_id
left join staging.refunds r on r.order_id = o.order_id
group by c.customer_id, c.country;
