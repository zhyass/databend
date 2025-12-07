// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use backoff::backoff::Backoff;
use chrono::Utc;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableStatistics;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_types::MatchSeq;
use databend_storages_common_cache::Table;
use databend_storages_common_cache::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::decode_column_hll;
use databend_storages_common_table_meta::meta::encode_column_hll;
use databend_storages_common_table_meta::meta::merge_column_hll;
use databend_storages_common_table_meta::readers::snapshot_reader::TableSnapshotAccessor;
use databend_storages_common_table_meta::table::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use log::info;
use tokio::time::sleep;

use super::diff::SegmentsDiff;
use crate::FuseTable;
use crate::operations::set_backoff;
use crate::statistics::merge_statistics;
use crate::statistics::reducers::deduct_statistics;

const FUSE_ENGINE: &str = "FUSE";

pub async fn commit_with_backoff(
    ctx: Arc<dyn TableContext>,
    mut req: UpdateMultiTableMetaReq,
) -> Result<()> {
    let catalog = ctx.get_default_catalog()?;
    let mut backoff = set_backoff(None, None, None);
    let mut retries = 0;

    // Compute segments diff for all tables before entering the retry loop.
    // This diff represents the actual changes made by the transaction (base -> txn_generated),
    // and remains constant across all retries.
    // Also cache the original snapshots for statistics merging.
    let (table_segments_diffs, table_original_snapshots) =
        compute_table_segments_diffs(ctx.clone(), &req).await?;

    loop {
        let ret = catalog
            .retryable_update_multi_table_meta(req.clone())
            .await?;
        let Err(update_failed_tbls) = ret else {
            return Ok(());
        };
        let Some(duration) = backoff.next_backoff() else {
            return Err(ErrorCode::OCCRetryFailure(retry_too_many_msg(
                retries,
                backoff.start_time,
                update_failed_tbls,
            )));
        };
        sleep(duration).await;
        retries += 1;
        try_rebuild_req(
            ctx.clone(),
            &mut req,
            update_failed_tbls,
            &table_segments_diffs,
            &table_original_snapshots,
        )
        .await?;
    }
}

async fn compute_table_segments_diffs(
    ctx: Arc<dyn TableContext>,
    req: &UpdateMultiTableMetaReq,
) -> Result<(
    HashMap<u64, HashMap<Option<u64>, SegmentsDiff>>,
    HashMap<u64, HashMap<Option<u64>, Option<Arc<TableSnapshot>>>>,
)> {
    let txn_mgr = ctx.txn_mgr();
    let storage_class = ctx.get_settings().get_s3_storage_class()?;
    let mut table_segments_diffs = HashMap::new();
    let mut table_original_snapshots = HashMap::new();

    for (update_table_meta_req, _) in &req.update_table_metas {
        let tid = update_table_meta_req.table_id;
        let engine = update_table_meta_req.new_table_meta.engine.as_str();

        if engine != FUSE_ENGINE {
            info!(
                "Skipping segments diff pre-compute for table {} with engine {}",
                tid, engine
            );
            continue;
        }

        // Read the base snapshot (snapshot at transaction begin)
        let base_snapshot_locations = txn_mgr.lock().get_base_snapshot_locations(tid);

        // Read the transaction-generated snapshot (original snapshot before any merge)
        let new_table = FuseTable::from_table_meta(
            update_table_meta_req.table_id,
            0,
            update_table_meta_req.new_table_meta.clone(),
            storage_class,
        )?;

        let mut branch_diffs = HashMap::new();
        let mut branch_snapshots = HashMap::new();
        for (branch_id, base_location) in base_snapshot_locations {
            let base_snapshot = new_table
                .read_table_snapshot_with_location(base_location.clone())
                .await?;
            let new_snapshot_location =
                get_branch_snapshot_location(new_table.as_ref(), branch_id)?;
            let new_snapshot = new_table
                .read_table_snapshot_with_location(new_snapshot_location)
                .await?;

            let base_segments = base_snapshot
                .as_ref()
                .map(|s| s.segments.as_slice())
                .unwrap_or(&[]);
            let new_segments = new_snapshot
                .as_ref()
                .map(|s| s.segments.as_slice())
                .unwrap_or(&[]);

            info!(
                "Computing segments diff for table {} branch {:?} (base: {} segments, txn: {} segments)",
                tid,
                branch_id,
                base_segments.len(),
                new_segments.len()
            );

            let diff = SegmentsDiff::new(base_segments, new_segments);
            branch_diffs.insert(branch_id, diff);
            branch_snapshots.insert(branch_id, new_snapshot);
        }

        table_segments_diffs.insert(tid, branch_diffs);
        table_original_snapshots.insert(tid, branch_snapshots);
    }

    Ok((table_segments_diffs, table_original_snapshots))
}

async fn try_rebuild_req(
    ctx: Arc<dyn TableContext>,
    req: &mut UpdateMultiTableMetaReq,
    update_failed_tbls: Vec<(u64, u64, TableMeta)>,
    table_segments_diffs: &HashMap<u64, HashMap<Option<u64>, SegmentsDiff>>,
    table_original_snapshots: &HashMap<u64, HashMap<Option<u64>, Option<Arc<TableSnapshot>>>>,
) -> Result<()> {
    info!(
        "try_rebuild_req: update_failed_tbls={:?}",
        update_failed_tbls
    );
    let insert_rows = {
        let stats = ctx.get_multi_table_insert_status();
        let status = stats.lock();
        status.insert_rows.clone()
    };
    let txn_mgr = ctx.txn_mgr();
    for (tid, seq, table_meta) in update_failed_tbls {
        if table_meta.engine == "STREAM" {
            return Err(ErrorCode::UnresolvableConflict(format!(
                "Concurrent transaction commit failed. Stream table {} has unresolvable conflicts.",
                tid
            )));
        }
        let storage_class = ctx.get_settings().get_s3_storage_class()?;
        let latest_table = FuseTable::from_table_meta(tid, seq, table_meta, storage_class)?;
        let default_cluster_key_id = latest_table.cluster_key_id();
        let (update_table_meta_req, _) = req
            .update_table_metas
            .iter_mut()
            .find(|(meta, _)| meta.table_id == tid)
            .unwrap();

        let base_snapshot_locations = txn_mgr.lock().get_base_snapshot_locations(tid);

        // Get the pre-computed segments diff for this table (computed before retry loop)
        let branch_segments_diffs = table_segments_diffs.get(&tid).ok_or_else(|| {
            ErrorCode::Internal(format!("Missing segments diff for table {}", tid))
        })?;
        let branch_original_snapshots = table_original_snapshots.get(&tid).ok_or_else(|| {
            ErrorCode::Internal(format!("Missing original snapshot for table {}", tid))
        })?;

        // Read the original transaction-generated snapshot from cache for statistics merging
        let dal = latest_table.get_operator();
        let location_generator = &latest_table.meta_location_generator;
        let table_id = latest_table.table_info.ident.table_id;
        let table_version = latest_table.table_info.ident.seq;
        let mut new_table_meta = latest_table.table_info.meta.clone();
        let mut new_base_snapshot_locations = HashMap::with_capacity(base_snapshot_locations.len());

        for (branch_id, base_location) in base_snapshot_locations {
            let segments_diff = branch_segments_diffs.get(&branch_id).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "Missing segments diff for table {}, branch {:?}",
                    tid, branch_id
                ))
            })?;
            let new_snapshot = branch_original_snapshots
                .get(&branch_id)
                .cloned()
                .ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "Missing original snapshot for table {} branch {:?}",
                        tid, branch_id
                    ))
                })?;
            let latest_snapshot_location =
                get_branch_snapshot_location(latest_table.as_ref(), branch_id)?;
            new_base_snapshot_locations.insert(branch_id, latest_snapshot_location.clone());
            let latest_snapshot = latest_table
                .read_table_snapshot_with_location(latest_snapshot_location)
                .await?;
            let base_snapshot = latest_table
                .read_table_snapshot_with_location(base_location.clone())
                .await?;

            let Some(merged_segments) = segments_diff
                .clone()
                .apply(latest_snapshot.segments().to_vec())
            else {
                return Err(ErrorCode::UnresolvableConflict(format!(
                    "Unresolvable conflict detected for table {} branch {:?}",
                    tid, branch_id
                )));
            };

            let s = merge_statistics(
                new_snapshot.summary(),
                &latest_snapshot.summary(),
                default_cluster_key_id,
            );
            let mut merged_summary = deduct_statistics(&s, &base_snapshot.summary());
            let mut additional_stats_meta = latest_snapshot.additional_stats_meta();
            let table_target_id = branch_id.unwrap_or(tid);
            let insert_row = insert_rows.get(&table_target_id).cloned().unwrap_or(0);
            let new_hll = new_snapshot
                .as_ref()
                .and_then(|v| v.summary.additional_stats_meta.as_ref())
                .and_then(|m| m.hll.as_ref());
            if insert_row > 0 && new_hll.is_some_and(|v| !v.is_empty()) {
                if let Some(ref mut latest_metas) = additional_stats_meta {
                    let new_hll = decode_column_hll(new_hll.unwrap())?.unwrap();
                    let latest_hll = latest_metas
                        .hll
                        .as_ref()
                        .map(decode_column_hll)
                        .transpose()?
                        .flatten()
                        .unwrap_or_default();
                    let merged = merge_column_hll(new_hll, latest_hll);
                    if !merged.is_empty() {
                        latest_metas.hll = Some(encode_column_hll(&merged)?);
                        latest_metas.row_count += insert_row;
                    }
                }
            }
            merged_summary.additional_stats_meta = additional_stats_meta;

            {
                let txn_mgr_ref = ctx.txn_mgr();
                let txn_mgr = txn_mgr_ref.lock();
                if let Some(txn_begin_timestamp) =
                    txn_mgr.get_table_txn_begin_timestamp(latest_table.get_unique_id())
                {
                    if let Some(latest_snapshot) = latest_snapshot.as_ref() {
                        let Some(latest_snapshot_timestamp) = latest_snapshot.timestamp else {
                            return Err(ErrorCode::UnresolvableConflict(format!(
                                "Table {} snapshot lacks required timestamp. This table was created with a significantly outdated version that is no longer directly supported by the current version and requires migration.
                                 Please contact us at https://www.databend.com/contact-us/ or email hi@databend.com",
                                tid
                            )));
                        };

                        // By enforcing txn_begin_timestamp >= latest_snapshot_timestamp, we ensure that
                        // vacuum operations won't remove table data (segment, blocks, etc.) that newly
                        // created in the current active transaction.

                        // In the current transaction, all the newly created table data (segments, blocks, etc.)
                        // has timestamps that are greater than or equal to txn_begin_timestamp, but the
                        // final snapshot which contains those data (and is yet to be committed) may have a timestamp
                        // that is larger than txn_begin_timestamp.

                        // To maintain vacuum safety, we must ensure that if the latest snapshot's timestamp
                        // (latest_snapshot_timestamp) is larger than txn_begin_timestamp, we abort the transaction
                        // to prevent potential data loss during vacuum operations.

                        // Example:
                        // session1:                                      session2:                    session3:
                        // begin;
                        // -- newly created table data
                        // -- timestamped as A
                        // insert into t values (1);
                        //                                              -- new snapshot S's ts is B
                        //                                              insert into t values (2);
                        //                                                                             -- using S as gc root
                        //                                                                             -- if B > A, then newly created table data
                        //                                                                             -- in session1 will be purged
                        //                                                                             call fuse_vacuum2('db', 't');
                        // -- while merging with S
                        // -- if A < B, this txn should abort
                        // commit;

                        if txn_begin_timestamp < latest_snapshot_timestamp {
                            return Err(ErrorCode::UnresolvableConflict(format!(
                                "Unresolvable conflict detected for table {} while resolving conflicts: txn started with logical timestamp {}, which is less than the latest table timestamp {}. Transaction must be aborted.",
                                tid, txn_begin_timestamp, latest_snapshot_timestamp
                            )));
                        }
                    }
                }
            }

            let table_meta_timestamps =
                ctx.get_table_meta_timestamps(latest_table.as_ref(), latest_snapshot.clone())?;
            let merged_snapshot = TableSnapshot::try_new(
                Some(seq),
                latest_snapshot.clone(),
                latest_table.schema().as_ref().clone(),
                merged_summary,
                merged_segments,
                latest_table.cluster_key_meta(),
                latest_snapshot.table_statistics_location(),
                table_meta_timestamps,
            )?;
            merged_snapshot.ensure_segments_unique()?;

            let location = location_generator.gen_snapshot_location(
                branch_id,
                &merged_snapshot.snapshot_id,
                TableSnapshot::VERSION,
            )?;
            dal.write(&location, merged_snapshot.to_bytes()?).await?;

            match branch_id {
                None => {
                    new_table_meta
                        .options
                        .insert(OPT_KEY_SNAPSHOT_LOCATION.to_owned(), location.to_owned());
                    new_table_meta.options.remove(OPT_KEY_LEGACY_SNAPSHOT_LOC);

                    let stats = &merged_snapshot.summary;
                    new_table_meta.statistics = TableStatistics {
                        number_of_rows: stats.row_count,
                        data_bytes: stats.uncompressed_byte_size,
                        compressed_data_bytes: stats.compressed_byte_size,
                        index_data_bytes: stats.index_size,
                        bloom_index_size: stats.bloom_index_size,
                        ngram_index_size: stats.ngram_index_size,
                        inverted_index_size: stats.inverted_index_size,
                        vector_index_size: stats.vector_index_size,
                        virtual_column_size: stats.virtual_column_size,
                        number_of_segments: Some(merged_snapshot.segments.len() as u64),
                        number_of_blocks: Some(stats.block_count),
                    };
                }
                Some(branch_id) => {
                    let Some(branch_ref) = new_table_meta
                        .refs
                        .iter_mut()
                        .find(|(_, r)| r.id == branch_id)
                    else {
                        return Err(ErrorCode::UnknownReference(format!(
                            "Unknown branch '{}' for table {}",
                            branch_id, tid
                        )));
                    };
                    branch_ref.1.loc = location;
                }
            }
        }

        new_table_meta.updated_on = Utc::now();

        let req = UpdateTableMetaReq {
            table_id,
            seq: MatchSeq::Exact(table_version),
            new_table_meta,
            base_snapshot_locations: new_base_snapshot_locations,
            lvt_check: None,
        };
        *update_table_meta_req = req;
    }
    Ok(())
}

fn get_branch_snapshot_location(
    table: &FuseTable,
    branch_id: Option<u64>,
) -> Result<Option<String>> {
    if let Some(branch_id) = branch_id {
        let table_info = table.get_table_info();
        let location = table_info
            .meta
            .refs
            .values()
            .find(|r| r.id == branch_id)
            .map(|info| info.loc.clone())
            .ok_or_else(|| {
                ErrorCode::UnknownReference(format!(
                    "Unknown reference '{}' in table {}",
                    branch_id, table_info.desc
                ))
            })?;
        Ok(Some(location))
    } else {
        let options = table.get_table_info().options();
        Ok(options
            .get(OPT_KEY_SNAPSHOT_LOCATION)
            .or_else(|| options.get(OPT_KEY_LEGACY_SNAPSHOT_LOC))
            .cloned())
    }
}

fn retry_too_many_msg(
    retries: u32,
    start_time: Instant,
    update_failed_tbls: Vec<(u64, u64, TableMeta)>,
) -> String {
    format!(
        "Transaction aborted after retries({} times, {} ms). The table_ids that failed to update: {:?}",
        retries,
        Instant::now().duration_since(start_time).as_millis(),
        update_failed_tbls
            .into_iter()
            .map(|(tid, _, _)| tid)
            .collect::<Vec<_>>()
    )
}
