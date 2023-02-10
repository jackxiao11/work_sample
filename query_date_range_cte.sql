/*
sample data
dates
11/1
11/2
11/5
11/6
11/7
11/8
11/9
11/12
11/13

result
range_start range_end
11/1        11/2
11/5        11/9
*/

WITH RECURSIVE cte AS
(
    SELECT dates, next_date
    FROM
        (SELECT dates, LAG(date, 1) OVER (ORDER BY dates) prev_date,
            LEAD(date, 1) OVER (ORDER BY dates) next_date
        FROM table
        ) s1
    WHERE DATEDIFF(prev_date, dates) > 1
        OR prev_date IS NULL
    ORDER BY dates
    UNION ALL
    SELECT *
    FROM cte JOIN
        (SELECT cte.dates, s2.next_date
        FROM
            (SELECT dates, LEAD(date, 1) OVER (ORDER BY dates) next_date
            FROM table
            ) s2
        ON s2.dates = cte.next_date
    )
)
SELECT dates, MAX(next_date)
FROM cte
GROUP BY dates
