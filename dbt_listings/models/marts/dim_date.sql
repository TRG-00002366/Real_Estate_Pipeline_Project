SELECT 
    TO_NUMBER(TO_CHAR(d.date_value, 'YYYYMMDD')) AS date_key,
    d.date_value AS full_date,
    DAYOFWEEK(d.date_value) AS day_of_week,
    DAYNAME(d.date_value) AS day_name,
    MONTH(d.date_value) AS month_number,
    MONTHNAME(d.date_value) AS month_name,
    QUARTER(d.date_value) AS quarter,
    YEAR(d.date_value) AS year,
    DAYOFWEEK(d.date_value) IN (0, 6) AS is_weekend
FROM (
    SELECT DATEADD('day', SEQ4(), '2015-01-01')::DATE AS date_value
    FROM TABLE(GENERATOR(ROWCOUNT => 5000))  -- 10 years
) d