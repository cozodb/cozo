use anyhow::Result;
use rand::Rng;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::v1::{Context, Timestamp};
use uuid::Uuid;

pub(crate) fn random_uuid_v1() -> Result<Uuid> {
    let mut rng = rand::thread_rng();
    let uuid_ctx = Context::new(rng.gen());
    let now = SystemTime::now();
    let since_epoch = now.duration_since(UNIX_EPOCH)?;

    let ts = Timestamp::from_unix(uuid_ctx, since_epoch.as_secs(), since_epoch.subsec_nanos());

    let mut rand_vals = [0u8; 6];
    rng.fill(&mut rand_vals);
    let id = Uuid::new_v1(ts, &rand_vals)?;
    Ok(id)
}
