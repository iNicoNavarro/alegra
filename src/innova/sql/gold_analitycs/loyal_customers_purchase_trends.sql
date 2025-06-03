WITH top_clients AS (
  SELECT TOP 5
    c.id_customer,
    c.customer_name,
    SUM(f.total_amount) AS annual_spent
  FROM dwh_innova.fact_invoices AS f
    JOIN dwh_innova.dim_date AS d
      ON f.id_date = d.date_id
    JOIN dwh_innova.dim_customer AS c
      ON f.id_customer = c.id_customer
  WHERE d.[year] = 2024
  GROUP BY
    c.id_customer,
    c.customer_name
  ORDER BY
    SUM(f.total_amount) DESC
)
SELECT
  d.[year],
  d.month,
  tc.customer_name,
  SUM(f.total_amount) AS monthly_spent
FROM dwh_innova.fact_invoices AS f
  JOIN dwh_innova.dim_date AS d
    ON f.id_date = d.date_id
  JOIN top_clients AS tc
    ON f.id_customer = tc.id_customer
GROUP BY
  d.[year],
  d.month,
  tc.customer_name
ORDER BY
  tc.customer_name,
  d.[year],
  d.month;
