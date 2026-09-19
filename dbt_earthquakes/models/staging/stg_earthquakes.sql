select
    id,
    time,
    latitude,
    longitude,
    depth,
    mag,
    "magType" as mag_type,
    place,
    type as event_type,
    status,
    net,
    updated,
    mag_category,
    depth_category,
    estimated_energy_joules,
    hour_of_day,
    day_of_week
from {{ source('staging', 'earthquakes') }}