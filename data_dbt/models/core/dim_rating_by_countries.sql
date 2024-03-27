{{ config(materialized='table') }}

with ratings as (
    select * from {{ ref('stg_ratings') }}
),
users as (
    select* from {{ ref('stg_users') }}
)

select
    users.country as country,
    sum(ratings.rating) as total_ratings
from ratings
join users on ratings.user_id = users.user_id
group by 1
