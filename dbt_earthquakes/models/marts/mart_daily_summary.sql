select
    event_date,
    region_name,
    count(*) as earthquake_count,
    avg(mag) as avg_magnitude,
    max(mag) as max_magnitude,
    sum(estimated_energy_joules) as total_energy_joules
from {{ ref('fct_earthquakes') }}
group by event_date, region_name