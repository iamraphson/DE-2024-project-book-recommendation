{{
    config(
        materialized='view'
    )
}}

with booksdata as (
    SELECT
    	*
    FROM
    	{{ source ('staging', 'books') }} where year_of_publication <> 0
)

select
    isbn,
    book_title,
    book_author,
    year_of_publication,
    publisher
from booksdata
