{{
    config(
        materialized='table'
    )
}}

WITH age_range_ratings AS (
    SELECT
        CASE
            WHEN age = 0 THEN 'Unknown age'
            WHEN age BETWEEN 1 AND 9 THEN '1-9'
            WHEN age BETWEEN 10 AND 19 THEN '10-19'
            WHEN age BETWEEN 20 AND 29 THEN '20-29'
            WHEN age BETWEEN 30 AND 39 THEN '30-39'
            WHEN age BETWEEN 40 AND 49 THEN '40-49'
            WHEN age BETWEEN 50 AND 59 THEN '50-59'
            WHEN age BETWEEN 60 AND 69 THEN '60-69'
            WHEN age BETWEEN 70 AND 79 THEN '70-79'
            WHEN age BETWEEN 80 AND 89 THEN '80-89'
            ELSE '90+'
        END AS age_range,
        age,
        rating
    FROM
        {{ ref('facts_full_ratings') }}
)

SELECT
    age_range,
    COUNT(rating) AS number_of_rating
FROM
    age_range_ratings
GROUP BY
    age_range
ORDER BY
    MIN(age)
