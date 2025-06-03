MERGE INTO {target} AS tgt
USING {staging}     AS src
ON (tgt.product_code = src.product_code)
WHEN MATCHED THEN
  UPDATE SET
    product_name = src.product_name,
    id_category  = src.id_category,
    price        = src.price,
    updated_at   = GETDATE(),
    deleted_at   = NULL
WHEN NOT MATCHED THEN
  INSERT (product_code, product_name, id_category, price, created_at)
  VALUES (src.product_code, src.product_name, src.id_category, src.price, GETDATE())
WHEN NOT MATCHED BY SOURCE THEN
  UPDATE SET deleted_at = GETDATE();
