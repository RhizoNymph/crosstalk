//! The SQL that scores candidates, one statement per [`Mode`].
//!
//! Every statement takes the same parameters and returns the same columns,
//! ranked by descending (score, transmission) after an optional keyset:
//!
//! | Parameter | Type | Meaning |
//! | --- | --- | --- |
//! | `$1` | `text` | the query text (text and hybrid modes) |
//! | `$2` | `real[]` | the query embedding (semantic and hybrid modes) |
//! | `$3`, `$4` | `text`, `integer` | the query embedding's model |
//! | `$5`, `$6` | `bigint` | the window's start and end, or NULL |
//! | `$7`, `$8` | `real`, `text` | the last hit served (score, transmission), or NULL |
//! | `$9` | `bigint` | how many candidates to return |
//!
//! Columns: transmission, sender, reader, route JSON, confirmation time,
//! verdict JSON (the index's copy), snippet, score.
//!
//! **Text terms.** The query's lexemes are `to_tsvector('simple', $1)`'s,
//! ORed into a `tsquery` (each lexeme quoted, its quotes and backslashes
//! escaped) so the GIN index finds every document sharing one; the text
//! score is the fraction of the query's lexemes the document's `terms`
//! hold. A query with no lexeme matches nothing.
//!
//! **Cosine.** `sum(a * b ORDER BY i)` over the two vectors as `real[]`:
//! `real` products added in dimension order, the reference's `f32` sum,
//! then clamped into `0..=1`.

/// How a query scores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Text,
    Semantic,
    Hybrid,
}

const QUERY_TERMS: &str = "\
q AS (
    SELECT lexemes,
           cardinality(lexemes) AS n,
           (SELECT string_agg('''' || replace(replace(l, '\\', '\\\\'), '''', '''''') || '''', ' | ')
              FROM unnest(lexemes) AS l)::tsquery AS terms
      FROM (SELECT tsvector_to_array(to_tsvector('simple'::regconfig, coalesce($1::text, ''))) AS lexemes) AS t
)";

const TEXT_SCORE: &str = "\
CASE WHEN q.n > 0 AND d.terms @@ q.terms
     THEN (SELECT count(*) FROM unnest(tsvector_to_array(d.terms)) AS l WHERE l = ANY (q.lexemes))::real / q.n::real
     ELSE 0::real END";

const COSINE: &str = "\
(SELECT CASE WHEN x.dot > 0 THEN least(x.dot, 1::real) ELSE 0::real END
   FROM (SELECT sum(u.a * u.b ORDER BY u.i) AS dot
           FROM unnest(e.embedding::real[], $2::real[]) WITH ORDINALITY AS u(a, b, i)) AS x)";

const EMBEDDING_JOIN: &str = "\
JOIN analysis.search_embeddings AS e
  ON e.transmission = d.transmission AND e.model_name = $3::text AND e.model_dimension = $4::integer";

/// The statement for `mode`.
pub fn statement(mode: Mode) -> String {
    let (join, filter, score) = match mode {
        Mode::Text => (
            String::new(),
            "q.n > 0 AND d.terms @@ q.terms".to_owned(),
            TEXT_SCORE.to_owned(),
        ),
        Mode::Semantic => (
            EMBEDDING_JOIN.to_owned(),
            "true".to_owned(),
            COSINE.to_owned(),
        ),
        Mode::Hybrid => (
            EMBEDDING_JOIN.to_owned(),
            "true".to_owned(),
            format!(
                "least(greatest((((({TEXT_SCORE})::float8 + ({COSINE})::float8) / 2))::real, 0::real), 1::real)"
            ),
        ),
    };
    format!(
        "WITH {QUERY_TERMS}
SELECT * FROM (
    SELECT d.transmission, d.from_agent, d.to_agent, d.route, d.confirmed_at, v.verdict,
           left(d.body, {snippet}) AS snippet,
           ({score})::real AS score
      FROM analysis.search_docs AS d
      CROSS JOIN q
      {join}
      LEFT JOIN analysis.search_verdicts AS v ON v.transmission = d.transmission
     WHERE {filter}
       AND ($5::bigint IS NULL OR d.confirmed_at >= $5::bigint)
       AND ($6::bigint IS NULL OR d.confirmed_at < $6::bigint)
) AS scored
 WHERE $7::real IS NULL OR scored.score < $7::real OR (scored.score = $7::real AND scored.transmission < $8::text)
 ORDER BY scored.score DESC, scored.transmission DESC
 LIMIT $9::bigint",
        snippet = super::SNIPPET_CHARS,
    )
}
