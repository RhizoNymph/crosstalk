//! `QueryApi::me` over HTTP (INV-1077): `HttpClient` against
//! `crosstalk-api`'s server over the in-process surface seeded with the
//! synthetic world. Each operator's token reads that operator, as the
//! world's directory defines it, whatever permissions it holds.

use crosstalk_api::http::BearerToken as ServerToken;
use crosstalk_api::world::{WorldOptions, seed_world, serve_world};
use crosstalk_client::{BaseUrl, BearerToken, ClientConfig, HttpClient};
use crosstalk_conformance::harness::caller;
use crosstalk_spec::ids::OperatorId;
use crosstalk_spec::interfaces::l8_surface::operators::{
    OperatorConfig, OperatorDirectory, OperatorName,
};
use crosstalk_spec::interfaces::l8_surface::{Permission, PermissionSet, QueryApi, QueryError};
use crosstalk_world::clock::UI_ANCHOR;
use crosstalk_world::config::{OPERATOR_ONCALL, OPERATOR_RESEARCHER};

const AUDITOR: OperatorId = OperatorId::from_ulid(0x0d0e_0001);
const VIEWER: OperatorId = OperatorId::from_ulid(0x0d0e_0002);

fn token_text(operator: OperatorId) -> String {
    format!("me-test-{:032x}", operator.as_ulid())
}

fn configured(id: OperatorId, name: &str, permissions: &[Permission]) -> OperatorConfig {
    OperatorConfig {
        id,
        name: OperatorName::new(name).unwrap_or_else(|e| panic!("{e:?}")),
        permissions: PermissionSet::of(permissions.iter().copied()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_token_reads_its_own_operator() {
    let mut options =
        WorldOptions::new(7, UI_ANCHOR).unwrap_or_else(|e| panic!("world options: {e}"));
    options.operators = vec![
        configured(AUDITOR, "auditor", &[Permission::Audit]),
        configured(VIEWER, "viewer", &[Permission::View]),
    ];
    let world = seed_world(options)
        .await
        .unwrap_or_else(|e| panic!("seed: {e}"));
    let (directory, _) =
        OperatorDirectory::load(None, &world.access).unwrap_or_else(|e| panic!("{e:?}"));
    let operators = [OPERATOR_RESEARCHER, OPERATOR_ONCALL, AUDITOR, VIEWER];
    let tokens = operators
        .iter()
        .map(|id| {
            let token = ServerToken::new(&token_text(*id)).unwrap_or_else(|e| panic!("{e}"));
            (token, *id)
        })
        .collect();
    let served = serve_world(world, tokens)
        .await
        .unwrap_or_else(|e| panic!("serve: {e}"));
    let base = BaseUrl::parse(&served.base_url()).unwrap_or_else(|e| panic!("{e:?}"));
    let anonymous: HttpClient = HttpClient::new(base, ClientConfig::default());
    // The client sends no caller: the server derives it from the token.
    let ignored =
        caller(OPERATOR_RESEARCHER, PermissionSet::ALL).unwrap_or_else(|e| panic!("caller: {e}"));
    for id in operators {
        let token = BearerToken::new(&token_text(id)).unwrap_or_else(|e| panic!("{e:?}"));
        let client = anonymous.with_token(token);
        let me = client.me(&ignored).await;
        let Ok(me) = me else {
            panic!("me as {id:?}: {me:?}");
        };
        let Some(expected) = directory.get(id) else {
            panic!("{id:?} is not in the directory");
        };
        assert_eq!(&me, expected, "{id:?}");
        assert!(!me.permissions.is_empty(), "{id:?}");
    }
    // The auditor holds no View: `operators` refuses it, `me` did not.
    let auditor = anonymous
        .with_token(BearerToken::new(&token_text(AUDITOR)).unwrap_or_else(|e| panic!("{e:?}")));
    assert_eq!(
        auditor.operators(&ignored).await.err(),
        Some(QueryError::Forbidden {
            missing: Permission::View
        })
    );
    served.shutdown().await;
}
