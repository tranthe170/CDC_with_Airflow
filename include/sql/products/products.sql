select
    product_id,
    product_name,
    cost,
    original_sale_price,
    discount,
    current_price,
    taxes,
    created_at,
    updated_at
from public.products
{where_cond}