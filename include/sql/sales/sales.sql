select
    order_id,
    product_id,
    location_id,
    customer_id,
    order_date,
    quantity,
    price,
    created_at,
    updated_at
from public.sales
{where_cond}