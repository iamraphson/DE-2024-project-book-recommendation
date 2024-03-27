{{
    config(
        materialized='view'
    )
}}

with ratingsdata as (
    SELECT
    	*
    FROM
    	{{ source ('staging', 'ratings') }}
)

select
    user_id,
    isbn,
    rating
from ratingsdata
