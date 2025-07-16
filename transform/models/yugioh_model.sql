MODEL (
  name opensource.yugioh,
  table_format iceberg,
  dialect athena,
  kind FULL
);

SELECT
  *
FROM "sqlmesh"."yugioh"