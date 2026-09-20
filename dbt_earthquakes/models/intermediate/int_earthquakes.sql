with ranked as (
    select
        eq.*,
        r.region_name,
        row_number() over (
            partition by eq.id
            order by (r.max_lat - r.min_lat) * (r.max_long - r.min_long)
        ) as region_rank
    from {{ ref('stg_earthquakes') }} eq
    left join {{ ref('region_lookup') }} r
        on eq.latitude between r.min_lat and r.max_lat
       and eq.longitude between r.min_long and r.max_long
       and r.region_id != 15
)

select
    id,
    time,
    latitude,
    longitude,
    depth,
    mag,
    mag_type,
    place,
    event_type,
    status,
    net,
    updated,
    mag_category,
    depth_category,
    estimated_energy_joules,
    hour_of_day,
    day_of_week,
    coalesce(region_name, 'Other / Unclassified') as region_name
from ranked
where region_rank = 1 or region_name is null