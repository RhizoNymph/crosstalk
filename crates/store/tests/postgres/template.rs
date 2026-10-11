//! Template databases: built once, frozen, cloned per test, dropped.

use crosstalk_store::{TEST_DB_PREFIX, TEST_TEMPLATE_PREFIX, TestDb, test_server};

use crate::{Failure, TestResult, database_exists, db};

/// A frozen template's contents reach every clone, clones are independent
/// of each other and of the template, and closing drops each.
#[tokio::test(flavor = "multi_thread")]
async fn clones_carry_the_template_contents_and_are_independent() -> TestResult {
    let Some(observer) = db("clones_carry_the_template_contents_and_are_independent").await? else {
        return Ok(());
    };
    let admin = test_server()?
        .ok_or_else(|| Failure::Unexpected("a configured server has an admin URL".into()))?;
    let building = TestDb::create_template(admin, TestDb::default_pool_settings()).await?;
    let template_name = building
        .name()
        .map(|n| n.as_str().to_owned())
        .ok_or_else(|| Failure::Unexpected("a live TestDb has a name".into()))?;
    assert!(template_name.starts_with(TEST_TEMPLATE_PREFIX));
    sqlx::query("CREATE TABLE public.seeded (id int)")
        .execute(building.pool())
        .await?;
    sqlx::query("INSERT INTO public.seeded VALUES (1), (2)")
        .execute(building.pool())
        .await?;
    // A connection outside the pool, as a bus's LISTEN connection is:
    // freezing ends it.
    let stray = building.pool().acquire().await?.detach();
    let template = building.freeze().await?;
    drop(stray);

    let (a, b) = (template.instantiate().await?, template.instantiate().await?);
    for clone in [&a, &b] {
        let name = clone
            .name()
            .map(|n| n.as_str().to_owned())
            .unwrap_or_default();
        assert!(name.starts_with(TEST_DB_PREFIX) && !name.starts_with(TEST_TEMPLATE_PREFIX));
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM public.seeded")
            .fetch_one(clone.pool())
            .await?;
        assert_eq!(rows, 2);
    }
    sqlx::query("INSERT INTO public.seeded VALUES (3)")
        .execute(a.pool())
        .await?;
    let in_b: i64 = sqlx::query_scalar("SELECT count(*) FROM public.seeded")
        .fetch_one(b.pool())
        .await?;
    assert_eq!(in_b, 2, "a write to one clone is not in another");
    let c = template.instantiate().await?;
    let in_c: i64 = sqlx::query_scalar("SELECT count(*) FROM public.seeded")
        .fetch_one(c.pool())
        .await?;
    assert_eq!(in_c, 2, "a write to a clone is not in the template");

    let clone_name = a.name().map(|n| n.as_str().to_owned()).unwrap_or_default();
    a.close().await?;
    b.close().await?;
    c.close().await?;
    assert!(!database_exists(observer.pool(), &clone_name).await?);
    assert!(database_exists(observer.pool(), &template_name).await?);
    template.close().await?;
    assert!(!database_exists(observer.pool(), &template_name).await?);
    observer.close().await?;
    Ok(())
}

/// A frozen template refuses connections.
#[tokio::test(flavor = "multi_thread")]
async fn a_frozen_template_refuses_connections() -> TestResult {
    let Some(observer) = db("a_frozen_template_refuses_connections").await? else {
        return Ok(());
    };
    let admin = test_server()?
        .ok_or_else(|| Failure::Unexpected("a configured server has an admin URL".into()))?;
    let template = TestDb::create_template(admin.clone(), TestDb::default_pool_settings())
        .await?
        .freeze()
        .await?;
    let name = template
        .name()
        .map(|n| n.as_str().to_owned())
        .ok_or_else(|| Failure::Unexpected("a live template has a name".into()))?;
    let allowed: bool =
        sqlx::query_scalar("SELECT datallowconn FROM pg_database WHERE datname = $1")
            .bind(&name)
            .fetch_one(observer.pool())
            .await?;
    assert!(!allowed);
    template.close().await?;
    observer.close().await?;
    Ok(())
}
