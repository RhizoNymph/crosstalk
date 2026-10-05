//! The alignment rule: when a prediction is the transmission a label
//! expects. This is the heart of the eval; every count derives from it.

use crate::location::SpanLocationExt;
use crate::predict::{PredictedRoute, Prediction};
use crate::truth::{Exemption, ExpectedTransmission, NegativeControl, RouteExpectation};

/// Whether `prediction` reports the transmission `expected` labels.
///
/// All of:
///
/// 1. the same sender (`from`) and reader (`to`) agent;
/// 2. the same reader exchange: the content is found where it first arrived,
///    not in a later exchange that merely still carries it;
/// 3. overlapping content locations: the matched reader text shares at least
///    one byte with the labelled text (same message, same part);
/// 4. for a label routed through a channel, a predicted channel holding the
///    same canonical resource. Other route kinds do not have to agree: a
///    detector that finds the content but routes it differently is still
///    credited, and the per-route breakdown shows the disagreement.
///
/// Match class and carrier never decide alignment; they only choose the row
/// a prediction and a label are counted in.
pub fn aligns(prediction: &Prediction, expected: &ExpectedTransmission) -> bool {
    let label = expected.label();
    prediction.from == label.from
        && prediction.to == label.to
        && prediction.reader_exchange == label.reader_exchange
        && prediction.read_at.overlaps(&label.content.at)
        && same_channel(&label.route, &prediction.route)
}

fn same_channel(expected: &RouteExpectation, predicted: &PredictedRoute) -> bool {
    match (expected, predicted) {
        (RouteExpectation::Channel { resource }, PredictedRoute::Channel { resources }) => {
            resources.contains(resource)
        }
        (RouteExpectation::Channel { .. }, _) => false,
        (
            RouteExpectation::Delegation { .. }
            | RouteExpectation::Direct
            | RouteExpectation::Unobserved,
            _,
        ) => true,
    }
}

/// Whether a prediction that aligns with no label falls under `control`:
/// same sender and reader, the control's reader exchange (when it names
/// one), a read location overlapping the control's (when it names one), and
/// a matched span overlapping the control's origin (when it names one; a
/// prediction whose span location is unknown never falls under such a
/// control).
pub fn violates(prediction: &Prediction, control: &NegativeControl) -> bool {
    let label = control.label();
    prediction.from == label.from
        && prediction.to == label.to
        && label
            .reader_exchange
            .is_none_or(|exchange| exchange == prediction.reader_exchange)
        && label.at.is_none_or(|at| at.overlaps(&prediction.read_at))
        && label.origin.is_none_or(|origin| {
            prediction
                .origin_at
                .is_some_and(|span| span.overlaps(&origin))
        })
}

/// How specific a control is: one naming a read location is checked first,
/// then one naming an origin, then one naming only an exchange, so a
/// prediction is charged to the most specific control it falls under.
pub fn specificity(control: &NegativeControl) -> u8 {
    let label = control.label();
    let named = [
        label.at.is_some(),
        label.origin.is_some(),
        label.reader_exchange.is_some(),
    ];
    match named {
        [true, _, _] => 0,
        [false, true, _] => 1,
        [false, false, true] => 2,
        [false, false, false] => 3,
    }
}

/// Whether a prediction that aligns with no label falls under `exemption`:
/// the same reader and reader exchange, and an overlapping read location.
/// The sender is not compared: the exemption is for content whose sender
/// is unknown.
pub fn exempts(prediction: &Prediction, exemption: &Exemption) -> bool {
    prediction.to == exemption.to
        && prediction.reader_exchange == exemption.reader_exchange
        && prediction.read_at.overlaps(&exemption.at)
}
