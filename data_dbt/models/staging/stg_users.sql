{{
    config(
        materialized='view'
    )
}}

with usersdate as (
    SELECT
    	*
    FROM
    	{{ source ('staging', 'users') }}
    WHERE LOWER(country) NOT IN('', 'n/a') AND LOWER(country) not like '%n/a%'
)

select
    user_id,
    age,
    {{ capitalize('city') }},
    {{ capitalize('state') }},
    {{ capitalize('country') }}
from usersdate
