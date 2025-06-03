MERGE INTO {target} AS tgt
USING {staging}     AS src
ON (tgt.customer_code = src.customer_code)
WHEN MATCHED THEN
  UPDATE SET
    customer_name = src.customer_name,
    id_segment    = src.id_segment,
    id_location   = src.id_location,
    updated_at    = GETDATE(),
    deleted_at    = NULL
WHEN NOT MATCHED THEN
  INSERT (customer_code, customer_name, id_segment, id_location, created_at)
  VALUES (src.customer_code, src.customer_name, src.id_segment, src.id_location, GETDATE())
WHEN NOT MATCHED BY SOURCE THEN
  UPDATE SET deleted_at = GETDATE();
