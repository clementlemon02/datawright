"""Weekly KPI email: revenue and orders for the last 7 days."""
import duckdb

QUERY = """
select sum(revenue) as revenue, sum(orders) as orders
from mart.revenue_daily
where order_date > (select max(order_date) from mart.revenue_daily) - 7
"""


def main():
    print(duckdb.connect("shop.duckdb", read_only=True).execute(QUERY).fetchone())
