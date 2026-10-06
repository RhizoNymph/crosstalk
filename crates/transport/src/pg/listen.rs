//! The `LISTEN` task: turns `NOTIFY transport_events` from any connection
//! into a wake-up of every waiting `next` in the process. Notifications are
//! hints, never the source of truth: a waiting `next` also re-reads the
//! database every poll interval, and the task wakes everyone after each
//! (re)connect, since notifications sent while it was away are lost.

use std::sync::Arc;

use sqlx::postgres::PgListener;

use super::Shared;
use super::publish::CHANNEL;
use super::row::failure;

pub(crate) async fn run(shared: Arc<Shared>) {
    let poll = shared.config.poll.get();
    loop {
        match PgListener::connect_with(&shared.pool).await {
            Ok(mut listener) => match listener.listen(CHANNEL).await {
                Ok(()) => {
                    tracing::debug!(channel = CHANNEL, "listening for bus notifications");
                    shared.wake_all();
                    loop {
                        match listener.recv().await {
                            Ok(_) => shared.wake_all(),
                            Err(error) => {
                                tracing::debug!(failure = %failure(&error), "bus listener lost its connection");
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    tracing::debug!(failure = %failure(&error), "LISTEN failed");
                }
            },
            Err(error) => {
                tracing::debug!(failure = %failure(&error), "bus listener could not connect");
            }
        }
        tokio::time::sleep(poll).await;
    }
}
