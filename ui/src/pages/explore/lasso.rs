//! The `<ct-projection>` selection value and the server side of a lasso.
//!
//! Grammar (`ui/elements/src/shared/selection.ts`): `lasso:<x>,<y>;…`
//! (3 to 48 vertices in projection coordinates, decimals with at most four
//! fractional digits) | `point:<transmission ulid>` | `` (nothing). The
//! element selects exactly the points inside the rounded polygon by the
//! even-odd rule; [`Polygon::select`] repeats that computation in `f64`
//! over the stored `f32` coordinates, with the same bounding-box test
//! first, so both sides pick the same points.

use crosstalk_spec::ids::TransmissionId;

use crate::url::ulid::{InvalidUlid, UlidId};
use crosstalk_spec::aggregates::projection::Projection;

pub const MIN_VERTICES: usize = 3;
pub const MAX_VERTICES: usize = 48;
/// Fractional digits a coordinate may carry.
pub const MAX_DECIMALS: usize = 4;
/// Longer values are rejected before parsing: 48 vertices of two
/// coordinates each fit well within this.
pub const MAX_LEN: usize = 2048;

/// A lasso polygon in projection coordinates. Built only through
/// [`Polygon::parse`]: 3 to 48 finite vertices.
#[derive(Debug, Clone, PartialEq)]
pub struct Polygon(Vec<(f64, f64)>);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidSelection {
    #[error("longer than {MAX_LEN} bytes")]
    TooLong,
    #[error("expected lasso: or point:")]
    Kind,
    #[error("point: {0}")]
    Point(InvalidUlid),
    #[error("lasso: expected x,y pairs")]
    Pair,
    #[error("lasso: {0:?} is not a decimal with at most {MAX_DECIMALS} fractional digits")]
    Coordinate(String),
    #[error("lasso: needs {MIN_VERTICES} to {MAX_VERTICES} vertices, got {0}")]
    Vertices(usize),
}

/// `-?\d+(\.\d{1,4})?`, parsed. Rejects exponents, `inf`, `NaN` and
/// leading `+`, which `f64::from_str` would accept.
fn coordinate(text: &str) -> Result<f64, InvalidSelection> {
    let bad = || InvalidSelection::Coordinate(text.to_owned());
    let digits = text.strip_prefix('-').unwrap_or(text);
    let (whole, fraction) = match digits.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (digits, None),
    };
    let all_digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if !all_digits(whole) || fraction.is_some_and(|f| !all_digits(f) || f.len() > MAX_DECIMALS) {
        return Err(bad());
    }
    text.parse::<f64>()
        .ok()
        .filter(|v| v.is_finite())
        .ok_or_else(bad)
}

impl Polygon {
    /// Parses the part after `lasso:`.
    pub fn parse(text: &str) -> Result<Self, InvalidSelection> {
        let mut vertices = Vec::new();
        for pair in text.split(';') {
            let mut parts = pair.split(',');
            let (Some(x), Some(y), None) = (parts.next(), parts.next(), parts.next()) else {
                return Err(InvalidSelection::Pair);
            };
            vertices.push((coordinate(x)?, coordinate(y)?));
            if vertices.len() > MAX_VERTICES {
                return Err(InvalidSelection::Vertices(text.split(';').count()));
            }
        }
        if vertices.len() < MIN_VERTICES {
            return Err(InvalidSelection::Vertices(vertices.len()));
        }
        Ok(Self(vertices))
    }

    #[cfg(test)]
    pub fn vertices(&self) -> &[(f64, f64)] {
        &self.0
    }

    /// Even-odd ray casting, as the element computes it. Points exactly on
    /// an edge may fall either way, identically on both sides.
    pub fn contains(&self, x: f64, y: f64) -> bool {
        let vertices = &self.0;
        let mut inside = false;
        let mut j = vertices.len() - 1;
        for (i, &(xi, yi)) in vertices.iter().enumerate() {
            let (xj, yj) = vertices[j];
            if (yi > y) != (yj > y) && x < (xj - xi) * (y - yi) / (yj - yi) + xi {
                inside = !inside;
            }
            j = i;
        }
        inside
    }

    /// Indexes of the points inside the polygon, in point order.
    pub fn select(&self, xs: &[f32], ys: &[f32]) -> Vec<usize> {
        let (mut min_x, mut max_x) = (f64::INFINITY, f64::NEG_INFINITY);
        let (mut min_y, mut max_y) = (f64::INFINITY, f64::NEG_INFINITY);
        for &(x, y) in &self.0 {
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
        xs.iter()
            .zip(ys)
            .enumerate()
            .filter_map(|(i, (&x, &y))| {
                let (x, y) = (f64::from(x), f64::from(y));
                let outside_box = x < min_x || x > max_x || y < min_y || y > max_y;
                (!outside_box && self.contains(x, y)).then_some(i)
            })
            .collect()
    }

    /// The transmissions of a stored projection inside the polygon, in
    /// sample order.
    pub fn transmissions(&self, projection: &Projection) -> Vec<TransmissionId> {
        let columns = projection.frame().columns();
        let (xs, ys): (Vec<f32>, Vec<f32>) = columns.xy.iter().map(|[x, y]| (*x, *y)).unzip();
        self.select(&xs, &ys)
            .into_iter()
            .filter_map(|i| columns.transmissions.get(i).copied())
            .collect()
    }
}

/// A projection selection.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ProjectionSelection {
    #[default]
    None,
    Point(TransmissionId),
    Lasso(Polygon),
}

impl ProjectionSelection {
    pub fn parse(text: &str) -> Result<Self, InvalidSelection> {
        if text.len() > MAX_LEN {
            return Err(InvalidSelection::TooLong);
        }
        if text.is_empty() {
            return Ok(Self::None);
        }
        if let Some(id) = text.strip_prefix("point:") {
            return TransmissionId::parse_ulid(id)
                .map(Self::Point)
                .map_err(InvalidSelection::Point);
        }
        if let Some(polygon) = text.strip_prefix("lasso:") {
            return Polygon::parse(polygon).map(Self::Lasso);
        }
        Err(InvalidSelection::Kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn square() -> Polygon {
        Polygon::parse("0,0;2,0;2,2;0,2").expect("square")
    }

    #[test]
    fn parses_the_element_grammar() {
        let polygon = Polygon::parse("-1.25,0.5;3,-2.0001;0.1,7").expect("polygon");
        assert_eq!(
            polygon.vertices(),
            &[(-1.25, 0.5), (3.0, -2.0001), (0.1, 7.0)]
        );
        assert_eq!(
            ProjectionSelection::parse("point:01J9ZQ3W8D0000000000000001"),
            Ok(ProjectionSelection::Point(
                TransmissionId::parse_ulid("01J9ZQ3W8D0000000000000001").expect("id")
            ))
        );
        assert_eq!(
            ProjectionSelection::parse(""),
            Ok(ProjectionSelection::None)
        );
        assert!(matches!(
            ProjectionSelection::parse("lasso:0,0;1,0;1,1"),
            Ok(ProjectionSelection::Lasso(_))
        ));
    }

    #[test]
    fn rejects_what_the_element_never_sends() {
        for bad in [
            "1e3,0;1,0;1,1",
            "inf,0;1,0;1,1",
            "NaN,0;1,0;1,1",
            "+1,0;1,0;1,1",
            ".5,0;1,0;1,1",
            "1.,0;1,0;1,1",
            "0.12345,0;1,0;1,1",
        ] {
            assert!(
                matches!(Polygon::parse(bad), Err(InvalidSelection::Coordinate(_))),
                "{bad}"
            );
        }
        assert_eq!(
            Polygon::parse("0,0;1,1"),
            Err(InvalidSelection::Vertices(2))
        );
        assert_eq!(Polygon::parse("0,0;1;1,1"), Err(InvalidSelection::Pair));
        assert_eq!(Polygon::parse("0,0,0;1,1;1,0"), Err(InvalidSelection::Pair));
        let many = vec!["0,0"; MAX_VERTICES + 1].join(";");
        assert_eq!(
            Polygon::parse(&many),
            Err(InvalidSelection::Vertices(MAX_VERTICES + 1))
        );
        assert_eq!(
            ProjectionSelection::parse("box:0,0"),
            Err(InvalidSelection::Kind)
        );
        assert!(matches!(
            ProjectionSelection::parse("point:nope"),
            Err(InvalidSelection::Point(_))
        ));
        assert_eq!(
            ProjectionSelection::parse(&"x".repeat(MAX_LEN + 1)),
            Err(InvalidSelection::TooLong)
        );
    }

    #[test]
    fn even_odd_membership() {
        let square = square();
        assert!(square.contains(1.0, 1.0));
        assert!(!square.contains(3.0, 1.0));
        assert!(!square.contains(-0.5, 1.0));
        // A bow tie: the crossing point's neighbourhood alternates.
        let bow = Polygon::parse("0,0;2,2;2,0;0,2").expect("bow tie");
        assert!(bow.contains(0.2, 1.0));
        assert!(!bow.contains(1.0, 0.2));
    }

    #[test]
    fn concave_polygons_exclude_their_notch() {
        // A U shape opening upwards: the notch between the arms is outside.
        let u = Polygon::parse("0,0;3,0;3,3;2,3;2,1;1,1;1,3;0,3").expect("u");
        assert!(u.contains(0.5, 2.0));
        assert!(u.contains(2.5, 2.0));
        assert!(!u.contains(1.5, 2.0));
        assert!(u.contains(1.5, 0.5));
    }

    #[test]
    fn selects_points_in_order_over_f32_columns() {
        let xs = [1.0_f32, 5.0, 0.5, 1.999_9, -1.0];
        let ys = [1.0_f32, 1.0, 0.5, 1.999_9, 1.0];
        assert_eq!(square().select(&xs, &ys), vec![0, 2, 3]);
        assert!(square().select(&[], &[]).is_empty());
    }
}
