select
    location_id,
    name,
    county,
    state_code,
    state,
    type,
    latitude,
    longitude,
    created_at,
    updated_at
from public.locations
{where_cond}