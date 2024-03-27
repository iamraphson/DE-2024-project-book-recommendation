{{
    config(
        materialized='table'
    )
}}

with ratings as (
    select * from {{ ref('stg_ratings') }}
),
books as(
    select* from {{ ref('stg_books') }}
),
users as (
    select* from {{ ref('stg_users') }}
)


select
    ratings.user_id as user_id,
    ratings.isbn as isbn,
    ratings.rating as rating,
    users.age as age,
    users.city as city,
    users.state as state,
    users.country as country,
    books.book_title as title,
    books.book_author as author,
    books.year_of_publication as year_of_publication,
    books.publisher as publisher
from ratings
join users on ratings.user_id = users.user_id
join books on ratings.isbn = books.isbn
