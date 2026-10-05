//! The layers that own a Postgres schema.
//!
//! Every layer crate keeps its tables in a schema named after the layer and
//! records its applied migrations in a table inside that schema, so two
//! layers can both have a migration numbered `1` without colliding.

use std::fmt;

/// A layer of the abstraction stack that owns a Postgres schema.
///
/// The schema name is the layer crate's directory name (`crates/<layer>`),
/// so `crosstalk-ingress` owns schema `ingress`. The names are fixed
/// lowercase ASCII identifiers, which is what makes it safe to splice them
/// into DDL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Layer {
    /// L0, `crosstalk-ingress`.
    Ingress,
    /// L1, `crosstalk-canonical`.
    Canonical,
    /// L2, `crosstalk-transport`.
    Transport,
    /// L3, `crosstalk-reconstruct`.
    Reconstruct,
    /// L4, `crosstalk-provenance`.
    Provenance,
    /// L5, `crosstalk-flow`.
    Flow,
    /// L6, `crosstalk-analysis`.
    Analysis,
    /// L7, `crosstalk-topology`.
    Topology,
    /// L8, `crosstalk-surface`.
    Surface,
}

/// The name of the per-schema table that records applied migrations.
pub const MIGRATIONS_TABLE: &str = "_sqlx_migrations";

impl Layer {
    /// Every layer, in stack order.
    pub const ALL: [Layer; 9] = [
        Layer::Ingress,
        Layer::Canonical,
        Layer::Transport,
        Layer::Reconstruct,
        Layer::Provenance,
        Layer::Flow,
        Layer::Analysis,
        Layer::Topology,
        Layer::Surface,
    ];

    /// The layer's Postgres schema name (unquoted).
    pub const fn schema(self) -> &'static str {
        match self {
            Layer::Ingress => "ingress",
            Layer::Canonical => "canonical",
            Layer::Transport => "transport",
            Layer::Reconstruct => "reconstruct",
            Layer::Provenance => "provenance",
            Layer::Flow => "flow",
            Layer::Analysis => "analysis",
            Layer::Topology => "topology",
            Layer::Surface => "surface",
        }
    }

    /// The schema as a quoted SQL identifier, for example `"ingress"`.
    pub fn quoted_schema(self) -> String {
        format!("\"{}\"", self.schema())
    }

    /// The schema-qualified, quoted name of the layer's migrations table,
    /// for example `"ingress"."_sqlx_migrations"`.
    pub fn migrations_table(self) -> String {
        format!("\"{}\".\"{MIGRATIONS_TABLE}\"", self.schema())
    }

    /// The `search_path` a layer's migrations run under: the layer's schema
    /// first, so unqualified DDL lands there, then `public`, where the shared
    /// extensions (`vector`, `pg_trgm`) live.
    pub fn search_path(self) -> String {
        format!("\"{}\", public", self.schema())
    }
}

impl fmt::Display for Layer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.schema())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn schemas_are_distinct_lowercase_identifiers() {
        let names: BTreeSet<&str> = Layer::ALL.into_iter().map(Layer::schema).collect();
        assert_eq!(names.len(), Layer::ALL.len());
        for name in names {
            assert!(!name.is_empty());
            assert!(
                name.bytes().all(|b| b.is_ascii_lowercase()),
                "{name} is not a plain lowercase identifier"
            );
        }
    }

    #[test]
    fn migrations_table_is_qualified_by_the_layer_schema() {
        assert_eq!(
            Layer::Ingress.migrations_table(),
            "\"ingress\".\"_sqlx_migrations\""
        );
        assert_eq!(Layer::Flow.quoted_schema(), "\"flow\"");
        assert_eq!(Layer::Surface.search_path(), "\"surface\", public");
        assert_eq!(Layer::Canonical.to_string(), "canonical");
    }

    #[test]
    fn every_layer_table_is_distinct() {
        let tables: BTreeSet<String> = Layer::ALL
            .into_iter()
            .map(Layer::migrations_table)
            .collect();
        assert_eq!(tables.len(), Layer::ALL.len());
    }
}
