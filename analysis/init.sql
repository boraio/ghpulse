-- init.sql — DuckDB session setup for reading ghpulse bronze from Ceph.
--
--   s3env && duckdb -init analysis/init.sql
--
-- Credentials come from AWS_* in the environment via credential_chain, so
-- nothing secret lives in this file and it is safe to commit.

INSTALL httpfs;
LOAD httpfs;

CREATE OR REPLACE SECRET radosgw (
    TYPE S3,
    PROVIDER credential_chain,
    ENDPOINT '10.0.20.252:80',
    URL_STYLE 'path',
    USE_SSL false
);

-- A TABLE, not a VIEW — and that is a workaround, not a preference.
--
-- DuckDB 1.5.5 crashes with
--     INTERNAL Error: Unsupported type for NumericValueUnionToValue
-- on any GROUP BY carrying more than one count(DISTINCT ...) when the source
-- is remote Parquet. Narrowed by elimination:
--
--   two count(DISTINCT), no GROUP BY, from S3     -> works
--   one count(DISTINCT), with GROUP BY, from S3   -> works
--   two count(DISTINCT), with GROUP BY, from S3   -> CRASHES
--   the same query against a materialised table   -> works
--
-- So it is the remote reader, not the schema, the structs, or the hive
-- partition columns. Materialising once per session sidesteps it entirely.
--
-- The cost is real and worth stating: this pulls the whole bronze layer into
-- memory. Fine at one hour (~15MB) or one day (~400MB). NOT fine at a month.
-- At that point aggregation belongs in Spark, and DuckDB goes back to
-- querying the much smaller gold layer interactively — which is the design
-- the README argues for anyway.
CREATE OR REPLACE TABLE events AS
SELECT * FROM read_parquet(
    's3://ghpulse/bronze/events/*/*/*.parquet',
    hive_partitioning = true
);

.print ghpulse bronze materialised. Try:  SELECT count(*) FROM events;
