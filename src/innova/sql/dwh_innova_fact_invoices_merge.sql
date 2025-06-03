MERGE INTO {target} AS tgt
USING {staging}     AS src
ON (tgt.invoice_id = src.invoice_id)
WHEN MATCHED THEN
  UPDATE SET
    id_customer = src.id_customer,
    id_product = src.id_product,
    id_date = src.id_date,
    quantity = src.quantity,
    total_amount = src.total_amount,
    updated_at = GETDATE(),
    deleted_at = NULL
WHEN NOT MATCHED THEN
  INSERT (
    invoice_id, 
    id_customer, 
    id_product, 
    id_date,
    quantity, 
    total_amount, 
    created_at
  )
  VALUES (
    src.invoice_id, 
    src.id_customer, 
    src.id_product, 
    src.id_date,
    src.quantity, 
    src.total_amount, 
    GETDATE()
  )
WHEN NOT MATCHED BY SOURCE THEN
  UPDATE SET deleted_at = GETDATE();