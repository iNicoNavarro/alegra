SELECT
  d.[year],
  d.month,
  l.city             AS region,
  SUM(f.total_amount) AS total_sales
FROM dwh_innova.fact_invoices AS f
  JOIN dwh_innova.dim_date     AS d
    ON f.id_date = d.date_id
  JOIN dwh_innova.dim_customer AS c
    ON f.id_customer = c.id_customer
  JOIN dwh_innova.dim_location AS l
    ON c.id_location = l.id_location
WHERE
  d.[year] = 2023
GROUP BY
  d.[year],
  d.month,
  l.city
ORDER BY
  d.month,
  l.city;
