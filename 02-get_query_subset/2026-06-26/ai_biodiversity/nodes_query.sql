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
    AND REGEXP_LIKE(LOWER(title || ' ' || COALESCE(abstract, '')),
        'artificial intelligence|machine learning|neural networks?|large language models?|deep learning|natural language processing|generative ai|transformer models?|retrieval-augmented generation|foundation models?')
    AND REGEXP_LIKE(LOWER(title || ' ' || COALESCE(abstract, '')), '\bbiodivers')