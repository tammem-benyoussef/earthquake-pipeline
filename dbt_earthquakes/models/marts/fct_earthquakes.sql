{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge'
    )
}}

select
    id,
    time,
    date_trunc('day', time)::date as event_date,
    latitude,
    longitude,
    depth,
    mag,
    mag_type,
    place,
    event_type,
    status,
    net,
    mag_category,
    depth_category,
    estimated_energy_joules,
    hour_of_day,
    day_of_week,
    region_name
from {{ ref('int_earthquakes') }}

{% if is_incremental() %}
where time > (select max(time) from {{ this }})
{% endif %}