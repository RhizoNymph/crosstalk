//! The surface's own cursors across a restart: a key derived from the
//! deployment secret resolves them in a new process; a drawn key or
//! another secret does not.

use std::sync::Arc;

use crosstalk_spec::aggregates::filter::TopicVersionSelector;
use crosstalk_spec::ids::{DeploymentSecret, KeyedHasher, SecretVersion, SeededRandom};
use crosstalk_spec::interfaces::l8_surface::summary::TransmissionSelection;
use crosstalk_spec::interfaces::l8_surface::{QueryApi, QueryError};
use crosstalk_spec::paging::{PageRequest, TransmissionList};
use crosstalk_testkit::build::TransmissionBuilder;

use super::page;
use super::world::{Fixture, Who, World, config};
use crate::Surface;

fn secret(key: u8) -> KeyedHasher {
    KeyedHasher::new(DeploymentSecret::new(SecretVersion(1), [key; 32]))
}

/// A surface process over `fixture`'s stores, its ids drawn from OS
/// entropy as a gateway's are.
fn process(fixture: &Fixture, secret: Option<&KeyedHasher>) -> Surface<World> {
    let clock = Arc::new(fixture.clock.clone());
    match secret {
        Some(secret) => Surface::with_secret(
            fixture.world.clone(),
            clock,
            config(),
            SeededRandom::from_entropy(),
            fixture.feed.clone(),
            secret,
        ),
        None => Surface::new(
            fixture.world.clone(),
            clock,
            config(),
            SeededRandom::from_entropy(),
            fixture.feed.clone(),
        ),
    }
}

/// INV-1219: a cursor the surface issued resolves to the same page after
/// a restart with the same deployment secret.
#[tokio::test]
async fn cursor_keyed_from_the_secret_resolves_after_restart() {
    let fixture = Fixture::new().await;
    let mut scene = fixture.scene().await;
    let caller = fixture.caller(Who::Viewer).await;
    let mut ids = vec![scene.t1.transmission.id];
    for _ in 0..2 {
        let transmission = match TransmissionBuilder::new(&mut scene.ids).detected().build() {
            Ok(transmission) => transmission,
            Err(error) => panic!("{error:?}"),
        };
        fixture.transmission(&transmission).await;
        ids.push(transmission.id);
    }
    let Ok(selection) = TransmissionSelection::new(ids) else {
        panic!("selection");
    };
    let rows = async |surface: &Surface<World>, request: PageRequest<TransmissionList>| {
        surface
            .transmissions_by_id(&caller, &selection, TopicVersionSelector::Current, &request)
            .await
    };

    let before = process(&fixture, Some(&secret(1)));
    let first = match rows(&before, page(1)).await {
        Ok(first) => first,
        Err(error) => panic!("first page: {error:?}"),
    };
    let Some(next) = first.page.next().cloned() else {
        panic!("no second page");
    };
    let resumed = PageRequest {
        size: page::<TransmissionList>(1).size,
        after: Some(next),
    };
    let expected = rows(&before, resumed.clone()).await;
    assert!(expected.is_ok());
    drop(before);

    // A restart with the same secret: same page.
    let after = process(&fixture, Some(&secret(1)));
    assert_eq!(rows(&after, resumed.clone()).await, expected);
    // Another secret (a rotation), or a key drawn at start, refuses it.
    let rotated = process(&fixture, Some(&secret(2)));
    assert_eq!(
        rows(&rotated, resumed.clone()).await,
        Err(QueryError::InvalidCursor)
    );
    let drawn = process(&fixture, None);
    assert_eq!(rows(&drawn, resumed).await, Err(QueryError::InvalidCursor));
}
