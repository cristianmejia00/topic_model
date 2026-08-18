CREATE TABLE edges_query
WITH (
    format = 'PARQUET',
    external_location = 's3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/edges_query/'
) AS
SELECT
    e."from",
    e."to",
    e.weight
FROM
    edges_snapshot AS e
-- Join to ensure the starting node of the edge exists in our filtered node set.
JOIN
    nodes_query AS n_from ON e."from" = n_from.id
-- Join to ensure the ending node of the edge also exists in our filtered node set.
JOIN
    nodes_query AS n_to ON e."to" = n_to.id;