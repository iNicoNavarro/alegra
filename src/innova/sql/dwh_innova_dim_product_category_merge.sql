MERGE INTO {target} AS tgt
USING {staging}     AS src
ON (tgt.category_name = src.category_name)
WHEN MATCHED THEN
  UPDATE SET
    updated_at = GETDATE(),
    deleted_at = NULL
WHEN NOT MATCHED THEN
  INSERT (category_name, created_at)
  VALUES (src.category_name, GETDATE())
WHEN NOT MATCHED BY SOURCE THEN
  UPDATE SET deleted_at = GETDATE();