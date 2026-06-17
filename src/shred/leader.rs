//! Slot→leader lookup, backed by a Solana JSON-RPC endpoint and cached per epoch.
//!
//! One `getLeaderSchedule` call returns the whole epoch's schedule (leader pubkey → relative slot
//! indices) in a single response, so the cache is rebuilt at most once per epoch. `getEpochInfo`
//! tells us the current epoch and its first absolute slot, and lets a background refresher notice an
//! epoch rollover. The forwarder only ever does a lock-read of the cache on its hot path; all RPC
//! happens off-path in the refresher task.
//!
//! Minimal hand-rolled JSON-RPC over `reqwest` (the issue's "prefer minimal" option) — no
//! `solana-client`.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tracing::{info, warn};

/// How often the refresher polls `getEpochInfo` to detect an epoch rollover.
const REFRESH_INTERVAL: Duration = Duration::from_secs(30);
/// Shorter retry after an RPC error, so a transient failure doesn't leave us blind for a full cycle.
const RETRY_INTERVAL: Duration = Duration::from_secs(5);

/// The cached leader pubkeys for one epoch, indexed by `slot - first_slot`.
struct EpochLeaders {
    epoch: u64,
    first_slot: u64,
    leaders: Vec<[u8; 32]>,
}

pub struct LeaderSchedule {
    rpc_url: String,
    client: reqwest::Client,
    inner: RwLock<Option<EpochLeaders>>,
}

impl LeaderSchedule {
    pub fn new(rpc_url: String) -> Self {
        Self {
            rpc_url,
            client: reqwest::Client::new(),
            inner: RwLock::new(None),
        }
    }

    /// The slot's leader pubkey, or `None` if the schedule isn't loaded yet or the slot lies outside
    /// the cached epoch. `None` makes the forwarder fail open (forward, don't dedup) for that slot.
    pub async fn leader(&self, slot: u64) -> Option<[u8; 32]> {
        let guard = self.inner.read().await;
        let e = guard.as_ref()?;
        let rel = slot.checked_sub(e.first_slot)? as usize;
        e.leaders.get(rel).copied()
    }

    /// Rebuild the cache from RPC if the current epoch differs from what's cached (or nothing is
    /// cached yet). A no-op when the cached epoch is still current.
    pub async fn refresh(&self) -> Result<()> {
        let info = self.fetch_epoch_info().await?;
        let first_slot = info.absolute_slot - info.slot_index;
        if self
            .inner
            .read()
            .await
            .as_ref()
            .is_some_and(|e| e.epoch == info.epoch)
        {
            return Ok(());
        }
        let schedule = self.fetch_leader_schedule().await?;
        let leaders = build_leaders(&schedule)?;
        info!(
            epoch = info.epoch,
            first_slot,
            slots = leaders.len(),
            "loaded leader schedule"
        );
        *self.inner.write().await = Some(EpochLeaders {
            epoch: info.epoch,
            first_slot,
            leaders,
        });
        Ok(())
    }

    /// Loop forever refreshing the schedule, polling for epoch rollovers. Never returns (so it is
    /// not the terminal task in the forwarder's `JoinSet`); RPC errors are logged and retried.
    pub async fn run_refresher(&self) {
        loop {
            match self.refresh().await {
                Ok(()) => tokio::time::sleep(REFRESH_INTERVAL).await,
                Err(e) => {
                    warn!(%e, "leader schedule refresh failed; retrying");
                    tokio::time::sleep(RETRY_INTERVAL).await;
                }
            }
        }
    }

    async fn fetch_epoch_info(&self) -> Result<EpochInfo> {
        self.rpc("getEpochInfo", json!([])).await
    }

    async fn fetch_leader_schedule(&self) -> Result<std::collections::HashMap<String, Vec<usize>>> {
        // `null` slot = current epoch; default config returns relative slot indices per pubkey.
        self.rpc("getLeaderSchedule", json!([null])).await
    }

    /// One JSON-RPC call, returning the decoded `result`.
    async fn rpc<T: DeserializeOwned>(&self, method: &str, params: Value) -> Result<T> {
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
        let resp: RpcResponse<T> = self
            .client
            .post(&self.rpc_url)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("{method} request"))?
            .json()
            .await
            .with_context(|| format!("{method} decode"))?;
        if let Some(err) = resp.error {
            return Err(anyhow!("{method} rpc error {}: {}", err.code, err.message));
        }
        resp.result
            .ok_or_else(|| anyhow!("{method} returned no result"))
    }
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct EpochInfo {
    epoch: u64,
    absolute_slot: u64,
    slot_index: u64,
}

/// Turn a `getLeaderSchedule` response (base58 pubkey → relative slot indices) into a dense vector
/// indexed by relative slot. Every slot in an epoch has an assigned leader, so the vector is fully
/// populated; an undecodable pubkey is skipped (its slots stay zero → verification fails closed for
/// just those slots, which is the safe direction).
fn build_leaders(
    schedule: &std::collections::HashMap<String, Vec<usize>>,
) -> Result<Vec<[u8; 32]>> {
    let max = schedule
        .values()
        .flatten()
        .copied()
        .max()
        .ok_or_else(|| anyhow!("empty leader schedule"))?;
    let mut leaders = vec![[0u8; 32]; max + 1];
    for (pubkey_b58, indices) in schedule {
        let Some(pubkey) = decode_pubkey(pubkey_b58) else {
            warn!(pubkey = %pubkey_b58, "undecodable leader pubkey; skipping its slots");
            continue;
        };
        for &i in indices {
            if let Some(slot) = leaders.get_mut(i) {
                *slot = pubkey;
            }
        }
    }
    Ok(leaders)
}

fn decode_pubkey(b58: &str) -> Option<[u8; 32]> {
    let bytes = bs58::decode(b58).into_vec().ok()?;
    <[u8; 32]>::try_from(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn b58(bytes: &[u8; 32]) -> String {
        bs58::encode(bytes).into_string()
    }

    #[test]
    fn build_leaders_is_dense_and_indexed_by_relative_slot() {
        let a = [1u8; 32];
        let b = [2u8; 32];
        let mut sched = HashMap::new();
        sched.insert(b58(&a), vec![0, 2]);
        sched.insert(b58(&b), vec![1, 3]);
        let leaders = build_leaders(&sched).unwrap();
        assert_eq!(leaders, vec![a, b, a, b]);
    }

    #[test]
    fn build_leaders_skips_undecodable_pubkeys() {
        let a = [3u8; 32];
        let mut sched = HashMap::new();
        sched.insert(b58(&a), vec![1]);
        sched.insert("not-base58-!!!".to_string(), vec![0]);
        let leaders = build_leaders(&sched).unwrap();
        assert_eq!(leaders[1], a);
        assert_eq!(
            leaders[0], [0u8; 32],
            "undecodable pubkey leaves its slot zeroed"
        );
    }

    #[test]
    fn empty_schedule_is_an_error() {
        assert!(build_leaders(&HashMap::new()).is_err());
    }

    #[test]
    fn decode_pubkey_round_trips_and_rejects_wrong_length() {
        let pk = [9u8; 32];
        assert_eq!(decode_pubkey(&b58(&pk)), Some(pk));
        assert_eq!(decode_pubkey(&bs58::encode([0u8; 16]).into_string()), None);
    }
}
