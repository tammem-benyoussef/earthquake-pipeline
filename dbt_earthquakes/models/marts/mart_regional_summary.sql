select
    region_name,
    count(*) as earthquake_count,
    avg(mag) as avg_magnitude,
    max(mag) as max_magnitude,
    min(mag) as min_magnitude,
    avg(depth) as avg_depth,
    sum(estimated_energy_joules) as total_energy_joules,
    min(time) as first_recorded,
    max(time) as last_recorded
from {{ ref('fct_earthquakes') }}
group by region_name