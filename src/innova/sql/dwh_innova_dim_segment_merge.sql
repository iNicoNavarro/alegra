MERGE INTO {target} AS tgt
USING {staging}     AS src
ON (tgt.segment_name = src.segment_name)
WHEN MATCHED THEN
  UPDATE SET
    updated_at = GETDATE(),
    deleted_at = NULL
WHEN NOT MATCHED THEN
  INSERT (segment_name, created_at)
  VALUES (src.segment_name, GETDATE())
WHEN NOT MATCHED BY SOURCE THEN
  UPDATE SET deleted_at = GETDATE();