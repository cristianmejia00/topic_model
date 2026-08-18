CREATE TABLE nodes_query
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['publication_year'],
    external_location = 's3://openalex-results/snapshot_{SNAPSHOT}/queries/{QUERY}/nodes_query/'
) AS
SELECT
    id,
    doi,
    title,
    abstract,
    language,
    type_openalex,
    citations,
    publication_source,
    countries,
    institutions,
    authors,
    publication_year
FROM
    nodes_snapshot
WHERE
    publication_year >= 1990
    AND publication_year <= 2026
    AND type_openalex = 'article'
    AND language = 'en'
    AND REGEXP_LIKE(
        LOWER(COALESCE(title, '') || ' ' || COALESCE(abstract, '')),
        '\bquantum[-\s](comput|bit|logic|gate|processor|circuit|photonic|optic|algorithm|inspired|machine[-\s]learning|strateg|behaved[-\s]particle[-\s]swarm|neural[-\s]network|convolutional[-\s]neural[-\s]network|annealing|federated[-\s]learning|enhanced[-\s]feature|information|communication|network|internet|teleportation|repeater|key[-\s]distribution|memor|sensing|sensor|metrolog|error|cryptograph|secret[-\s]sharing)|\bqubit|\bintegrated[-\s]photonic|\bremote[-\s]state[-\s]preparation|\benvironment[-\s]induced[-\s]decoherence'
    )