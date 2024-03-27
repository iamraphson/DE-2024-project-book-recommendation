{{
    config(
        materialized='view'
    )
}}

with usersdata as (
    SELECT
    	*
    FROM
    	{{ source ('staging', 'users') }}
    WHERE LOWER(country) NOT IN('', 'n/a') AND LOWER(country) not like '%n/a%'
)

select
    user_id,
    age,
    {{ capitalize_replace('city') }} AS city,
    {{ capitalize_replace('state') }} AS state,
    CASE
        WHEN lower(country) IN ('usa', 'united states', 'united state', 'us', 'u.s.a.', 'america', 'u.s.a>', 'united staes', 'united states of america', 'Csa', 'San Franicsco', 'U.S. Of A.') THEN 'United states'
        WHEN lower(country) IN ('italia', 'l`italia', 'ferrara') THEN 'Italy'
        WHEN lower(country) IN ('u.a.e') THEN 'United Arab Emirates'
        WHEN lower(country) IN ('c.a.', 'canada') THEN 'Canada'
        WHEN lower(country) IN ('nz') THEN 'New Zealand'
        WHEN lower(country) IN ('urugua') THEN 'Uruguay'
        WHEN lower(country) IN ('p.r.china', 'china') THEN 'China'
        WHEN lower(country) IN ('trinidad and tobago', 'tobago') THEN 'Trinidad And Tobago'
        WHEN lower(country) IN ('united kingdom', 'u.k.', 'england', 'wales', 'united kindgonm') THEN 'United Kingdom'
        ELSE {{ capitalize_replace('country') }}
    END AS country
from usersdata
where (
    lower(country) not like '%far away%' AND
    lower(country) not in (
        'quit',
        'here and there',
        'everywhere and anywhere',
        'x',
        'k1c7b1',
        'we`re global!',
        '&#20013;&#22269;',
        'lkjlj',
        '',
        'Space',
        'Tdzimi',
        'Usa (Currently Living In England)',
        'Ua',
        'Universe'
    )
)
