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

use databend_common_expression::DataBlock;
use databend_query::sessions::TableContextTableAccess;
use databend_query::storages::fuse::FuseTable;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::table::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG;
use futures::TryStreamExt;

async fn assert_ids(
    fixture: &TestFixture,
    database: &str,
    table: &str,
    expected: &[&str],
) -> anyhow::Result<()> {
    let stream = fixture
        .execute_query(&format!("SELECT id FROM {database}.{table} ORDER BY id"))
        .await?;
    let blocks = stream.try_collect::<Vec<DataBlock>>().await?;
    databend_common_expression::block_debug::assert_blocks_sorted_eq(expected.to_vec(), &blocks);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_table_clone_empty_source() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let database = fixture.default_db_name();
    let source_name = fixture.default_table_name();
    let clone_name = format!("{}_clone_empty", source_name);

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{clone_name} CLONE {database}.{source_name}"
        ))
        .await?;

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
    let source = catalog
        .get_table(&ctx.get_tenant(), &database, &source_name)
        .await?;
    let cloned = catalog
        .get_table(&ctx.get_tenant(), &database, &clone_name)
        .await?;
    let source_fuse = FuseTable::try_from_table(source.as_ref())?;
    let cloned_fuse = FuseTable::try_from_table(cloned.as_ref())?;

    assert!(source_fuse.snapshot_loc().is_none());
    assert!(cloned_fuse.snapshot_loc().is_none());
    assert_eq!(cloned.schema(), source.schema());
    assert_eq!(cloned_fuse.clone_group_id()?, source_fuse.clone_group_id()?);

    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{clone_name} VALUES (1, (2, 3))"
        ))
        .await?;
    let one = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 1        |",
        "+----------+",
    ];
    assert_ids(&fixture, &database, &clone_name, &one).await?;
    let source_blocks = fixture
        .execute_query(&format!(
            "SELECT id FROM {database}.{source_name} ORDER BY id"
        ))
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    assert_eq!(
        source_blocks.iter().map(DataBlock::num_rows).sum::<usize>(),
        0
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_table_clone_across_databases() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let source_database = fixture.default_db_name();
    let source_name = fixture.default_table_name();
    let target_database = "clone_target_db";
    let clone_name = "cross_database_clone";

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {source_database}.{source_name} VALUES (1, (2, 3))"
        ))
        .await?;
    fixture
        .execute_command(&format!("CREATE DATABASE {target_database}"))
        .await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {target_database}.{clone_name} CLONE {source_database}.{source_name}"
        ))
        .await?;

    let one = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 1        |",
        "+----------+",
    ];
    assert_ids(&fixture, &source_database, &source_name, &one).await?;
    assert_ids(&fixture, target_database, clone_name, &one).await?;

    fixture
        .execute_command(&format!(
            "INSERT INTO {target_database}.{clone_name} VALUES (2, (4, 6))"
        ))
        .await?;
    let cloned = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 1        |",
        "| 2        |",
        "+----------+",
    ];
    assert_ids(&fixture, target_database, clone_name, &cloned).await?;
    assert_ids(&fixture, &source_database, &source_name, &one).await?;

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
    let source = catalog
        .get_table(&ctx.get_tenant(), &source_database, &source_name)
        .await?;
    let cloned = catalog
        .get_table(&ctx.get_tenant(), target_database, clone_name)
        .await?;
    assert_ne!(
        FuseTable::parse_storage_prefix_from_table_info(source.get_table_info())?,
        FuseTable::parse_storage_prefix_from_table_info(cloned.get_table_info())?
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_table_clone_copies_auto_increment_counter() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let database = fixture.default_db_name();
    let source_name = "clone_auto_increment_source";
    let clone_name = "clone_auto_increment_target";

    fixture.create_default_database().await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{source_name} (id INT AUTOINCREMENT (5, 1) ORDER, payload INT)"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{source_name} (payload) VALUES (10), (20)"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{clone_name} CLONE {database}.{source_name}"
        ))
        .await?;

    let initial = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 5        |",
        "| 6        |",
        "+----------+",
    ];
    assert_ids(&fixture, &database, source_name, &initial).await?;
    assert_ids(&fixture, &database, clone_name, &initial).await?;

    // Both counters continue from the value captured at clone time and advance independently.
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{source_name} (payload) VALUES (30)"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{clone_name} (payload) VALUES (40), (50)"
        ))
        .await?;
    let source_expected = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 5        |",
        "| 6        |",
        "| 7        |",
        "+----------+",
    ];
    let clone_expected = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 5        |",
        "| 6        |",
        "| 7        |",
        "| 8        |",
        "+----------+",
    ];
    assert_ids(&fixture, &database, source_name, &source_expected).await?;
    assert_ids(&fixture, &database, clone_name, &clone_expected).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_table_clone_survives_purge() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture
        .default_session()
        .get_settings()
        .set_data_retention_time_in_days(0)?;

    let database = fixture.default_db_name();
    let source_name = fixture.default_table_name();
    let clone_name = format!("{}_purge_clone", source_name);
    let chained_clone_name = format!("{}_purge_chain", source_name);

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{source_name} VALUES (1, (2, 3))"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{clone_name} CLONE {database}.{source_name}"
        ))
        .await?;

    // Make the original segment unreachable from the source head but still reachable from the
    // clone. Purging the source must retain that segment and its blocks for the clone.
    fixture
        .execute_command(&format!("TRUNCATE TABLE {database}.{source_name}"))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{source_name} VALUES (2, (4, 6))"
        ))
        .await?;
    fixture
        .execute_command(&format!("OPTIMIZE TABLE {database}.{source_name} PURGE"))
        .await?;

    let one = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 1        |",
        "+----------+",
    ];
    let two = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 2        |",
        "+----------+",
    ];
    assert_ids(&fixture, &database, &clone_name, &one).await?;
    assert_ids(&fixture, &database, &source_name, &two).await?;

    // Create a descendant after an independent clone write, then make those old clone segments
    // unreachable from the clone head. Purging the clone must retain them for the descendant.
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{clone_name} VALUES (3, (6, 9))"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{chained_clone_name} CLONE {database}.{clone_name}"
        ))
        .await?;
    fixture
        .execute_command(&format!("TRUNCATE TABLE {database}.{clone_name}"))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{clone_name} VALUES (4, (8, 12))"
        ))
        .await?;
    fixture
        .execute_command(&format!("OPTIMIZE TABLE {database}.{clone_name} PURGE"))
        .await?;

    let one_three = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 1        |",
        "| 3        |",
        "+----------+",
    ];
    let four = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 4        |",
        "+----------+",
    ];
    assert_ids(&fixture, &database, &chained_clone_name, &one_three).await?;
    assert_ids(&fixture, &database, &clone_name, &four).await?;
    assert_ids(&fixture, &database, &source_name, &two).await?;

    // All members remain independently writable after both purge directions.
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{source_name} VALUES (5, (10, 15))"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{chained_clone_name} VALUES (6, (12, 18))"
        ))
        .await?;

    let two_five = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 2        |",
        "| 5        |",
        "+----------+",
    ];
    let one_three_six = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 1        |",
        "| 3        |",
        "| 6        |",
        "+----------+",
    ];
    assert_ids(&fixture, &database, &source_name, &two_five).await?;
    assert_ids(&fixture, &database, &chained_clone_name, &one_three_six).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clone_purge_uses_descendant_retention_roots_and_file_owners() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.default_session().get_settings().set_setting(
        "data_retention_num_snapshots_to_keep".to_string(),
        "1".to_string(),
    )?;

    let database = fixture.default_db_name();
    let source_name = fixture.default_table_name();
    let left_name = format!("{}_left", source_name);
    let right_name = format!("{}_right", source_name);

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{source_name} VALUES (1, (2, 3))"
        ))
        .await?;

    let source = fixture.latest_default_table().await?;
    let source_fuse = FuseTable::try_from_table(source.as_ref())?;
    let inherited_segment = source_fuse.read_table_snapshot().await?.unwrap().segments[0].clone();

    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{left_name} CLONE {database}.{source_name}"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{right_name} CLONE {database}.{source_name}"
        ))
        .await?;

    // Move every member's retained root past the inherited source segment. Their old clone anchors
    // still reference it physically, but retention=1 makes those anchors unreachable.
    for (table, id) in [(&source_name, 4), (&left_name, 2), (&right_name, 3)] {
        fixture
            .execute_command(&format!("TRUNCATE TABLE {database}.{table}"))
            .await?;
        fixture
            .execute_command(&format!(
                "INSERT INTO {database}.{table} VALUES ({id}, ({id}, {id}))"
            ))
            .await?;
    }

    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
    let source = catalog
        .get_table(&ctx.get_tenant(), &database, &source_name)
        .await?;
    let source_fuse = FuseTable::try_from_table(source.as_ref())?;
    let protected = source_fuse
        .get_snapshot_referenced_segments(ctx.clone(), |_| {})
        .await?
        .unwrap();
    assert!(
        !protected.contains(&inherited_segment),
        "snapshots older than every member's retention root must not remain protected"
    );

    // The left clone has no descendants. Its purge must neither scan/protect the sibling nor
    // delete the inherited segment, because that physical file belongs to the source prefix.
    fixture
        .execute_command(&format!("OPTIMIZE TABLE {database}.{left_name} PURGE"))
        .await?;
    assert!(
        source_fuse
            .get_operator_ref()
            .exists(&inherited_segment.0)
            .await?
    );

    // The source owns the inherited segment. Since neither source nor descendants retain it now,
    // source purge can reclaim it instead of preserving the immutable clone anchors forever.
    fixture
        .execute_command(&format!("OPTIMIZE TABLE {database}.{source_name} PURGE"))
        .await?;
    assert!(
        !source_fuse
            .get_operator_ref()
            .exists(&inherited_segment.0)
            .await?
    );

    let two = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 2        |",
        "+----------+",
    ];
    let three = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 3        |",
        "+----------+",
    ];
    let four = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 4        |",
        "+----------+",
    ];
    assert_ids(&fixture, &database, &left_name, &two).await?;
    assert_ids(&fixture, &database, &right_name, &three).await?;
    assert_ids(&fixture, &database, &source_name, &four).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_create_table_clone_snapshot_and_independent_writes() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    let database = fixture.default_db_name();
    let source_name = fixture.default_table_name();
    let head_clone = format!("{}_clone_head", source_name);
    let historical_clone = format!("{}_clone_historical", source_name);
    let chained_clone = format!("{}_clone_chain", source_name);

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{source_name} VALUES (1, (2, 3))"
        ))
        .await?;

    let source = fixture.latest_default_table().await?;
    let source_fuse = FuseTable::try_from_table(source.as_ref())?;
    let first_snapshot_location = source_fuse.snapshot_loc().unwrap();
    let first_snapshot_id = source_fuse
        .read_table_snapshot()
        .await?
        .unwrap()
        .snapshot_id
        .simple()
        .to_string();

    // Evolve metadata after the captured snapshot. A historical clone must restore the selected
    // snapshot's schema and clustering rather than inheriting these current-head values.
    fixture
        .execute_command(&format!(
            "ALTER TABLE {database}.{source_name} ADD COLUMN evolved STRING"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "ALTER TABLE {database}.{source_name} CLUSTER BY (id)"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{source_name} (id, t) VALUES (2, (4, 6))"
        ))
        .await?;
    let source = fixture.latest_default_table().await?;
    let source_fuse = FuseTable::try_from_table(source.as_ref())?;
    let source_head_location = source_fuse.snapshot_loc().unwrap();
    let source_head = source_fuse.read_table_snapshot().await?.unwrap();

    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{head_clone} CLONE {database}.{source_name}"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{historical_clone} CLONE {database}.{source_name} \
             AT (SNAPSHOT => '{first_snapshot_id}')"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {database}.{chained_clone} CLONE {database}.{head_clone}"
        ))
        .await?;

    // The clone owns a fresh root snapshot. It shares immutable data pointers but does not point
    // into the source snapshot chain.
    let ctx = fixture.new_query_ctx().await?;
    let catalog = ctx.get_catalog(&fixture.default_catalog_name()).await?;
    let head_table = catalog
        .get_table(&ctx.get_tenant(), &database, &head_clone)
        .await?;
    let head_fuse = FuseTable::try_from_table(head_table.as_ref())?;
    let anchor_location = head_fuse.snapshot_loc().unwrap();
    let anchor = head_fuse.read_table_snapshot().await?.unwrap();
    assert!(
        !head_table
            .get_table_info()
            .meta
            .options
            .contains_key(OPT_KEY_LEGACY_SNAPSHOT_LOC)
    );
    assert!(
        !head_table
            .get_table_info()
            .meta
            .options
            .contains_key(OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG)
    );
    assert_ne!(anchor_location, source_head_location);
    assert_ne!(anchor_location, first_snapshot_location);
    assert_eq!(anchor.prev_snapshot_id, None);
    assert_eq!(anchor.segments, source_head.segments);

    let source = catalog
        .get_table(&ctx.get_tenant(), &database, &source_name)
        .await?;
    let historical = catalog
        .get_table(&ctx.get_tenant(), &database, &historical_clone)
        .await?;
    let chained = catalog
        .get_table(&ctx.get_tenant(), &database, &chained_clone)
        .await?;
    assert_eq!(
        FuseTable::try_from_table(source.as_ref())?.clone_group_id()?,
        head_fuse.clone_group_id()?
    );
    assert_eq!(
        FuseTable::try_from_table(chained.as_ref())?.clone_group_id()?,
        head_fuse.clone_group_id()?
    );

    assert!(source.schema().field_with_name("evolved").is_ok());
    assert!(head_table.schema().field_with_name("evolved").is_ok());
    assert!(chained.schema().field_with_name("evolved").is_ok());
    assert!(historical.schema().field_with_name("evolved").is_err());
    assert_eq!(
        head_table.get_table_info().meta.cluster_key_meta(),
        source.get_table_info().meta.cluster_key_meta()
    );
    assert!(
        head_table
            .get_table_info()
            .meta
            .cluster_key_meta()
            .is_some()
    );
    let selected_source_snapshot = FuseTable::try_from_table(source.as_ref())?
        .read_table_snapshot_with_location(Some(first_snapshot_location.clone()))
        .await?
        .unwrap();
    let historical_anchor = FuseTable::try_from_table(historical.as_ref())?
        .read_table_snapshot()
        .await?
        .unwrap();
    assert_eq!(
        historical.get_table_info().meta.cluster_key_meta(),
        selected_source_snapshot.cluster_key_meta
    );
    assert_eq!(
        historical_anchor.cluster_key_meta,
        historical.get_table_info().meta.cluster_key_meta()
    );
    assert!(historical_anchor.schema.field_with_name("evolved").is_err());

    let one = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 1        |",
        "+----------+",
    ];
    let one_two = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 1        |",
        "| 2        |",
        "+----------+",
    ];
    assert_ids(&fixture, &database, &historical_clone, &one).await?;
    assert_ids(&fixture, &database, &head_clone, &one_two).await?;
    assert_ids(&fixture, &database, &chained_clone, &one_two).await?;

    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{source_name} (id, t) VALUES (3, (6, 9))"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{head_clone} (id, t) VALUES (4, (8, 12))"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{chained_clone} (id, t) VALUES (5, (10, 15))"
        ))
        .await?;

    let source_expected = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 1        |",
        "| 2        |",
        "| 3        |",
        "+----------+",
    ];
    let head_expected = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 1        |",
        "| 2        |",
        "| 4        |",
        "+----------+",
    ];
    let chained_expected = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 1        |",
        "| 2        |",
        "| 5        |",
        "+----------+",
    ];
    assert_ids(&fixture, &database, &source_name, &source_expected).await?;
    assert_ids(&fixture, &database, &head_clone, &head_expected).await?;
    assert_ids(&fixture, &database, &chained_clone, &chained_expected).await?;
    assert_ids(&fixture, &database, &historical_clone, &one).await?;

    // IF NOT EXISTS must leave the existing target untouched even though the source has advanced.
    let head_id = head_table.get_id();
    fixture
        .execute_command(&format!(
            "CREATE TABLE IF NOT EXISTS {database}.{head_clone} CLONE {database}.{source_name}"
        ))
        .await?;
    let unchanged_head = catalog
        .get_table(&ctx.get_tenant(), &database, &head_clone)
        .await?;
    assert_eq!(unchanged_head.get_id(), head_id);
    assert_ids(&fixture, &database, &head_clone, &head_expected).await?;

    // CREATE OR REPLACE publishes a new clone atomically. Existing descendants keep their own
    // roots and data even though the replaced table becomes a dropped lineage member.
    fixture
        .execute_command(&format!(
            "CREATE OR REPLACE TABLE {database}.{head_clone} CLONE {database}.{source_name}"
        ))
        .await?;
    let replaced_head = catalog
        .get_table(&ctx.get_tenant(), &database, &head_clone)
        .await?;
    assert_ne!(replaced_head.get_id(), head_id);
    assert_ids(&fixture, &database, &head_clone, &source_expected).await?;
    assert_ids(&fixture, &database, &chained_clone, &chained_expected).await?;

    // Self-replacement binds the source before staging, then atomically publishes a new clone.
    // The old table remains a dropped lineage ancestor while the new table is independently
    // writable.
    let source_before_self_clone = catalog
        .get_table(&ctx.get_tenant(), &database, &source_name)
        .await?;
    let source_before_self_clone_id = source_before_self_clone.get_id();
    fixture
        .execute_command(&format!(
            "CREATE OR REPLACE TABLE {database}.{source_name} CLONE {database}.{source_name}"
        ))
        .await?;
    let source_after_self_clone = catalog
        .get_table(&ctx.get_tenant(), &database, &source_name)
        .await?;
    assert_ne!(
        source_after_self_clone.get_id(),
        source_before_self_clone_id
    );
    assert_ids(&fixture, &database, &source_name, &source_expected).await?;

    fixture
        .execute_command(&format!(
            "INSERT INTO {database}.{source_name} (id, t) VALUES (6, (12, 18))"
        ))
        .await?;
    let self_clone_expected = [
        "+----------+",
        "| Column 0 |",
        "+----------+",
        "| 1        |",
        "| 2        |",
        "| 3        |",
        "| 6        |",
        "+----------+",
    ];
    assert_ids(&fixture, &database, &source_name, &self_clone_expected).await?;
    assert_ids(&fixture, &database, &head_clone, &source_expected).await?;
    assert_ids(&fixture, &database, &chained_clone, &chained_expected).await?;

    // CLONE owns its schema and engine and only supports snapshot-ID navigation.
    for invalid_sql in [
        format!(
            "CREATE TABLE {database}.clone_with_schema (id INT) CLONE {database}.{source_name}"
        ),
        format!(
            "CREATE TABLE {database}.clone_with_engine CLONE {database}.{source_name} ENGINE = FUSE"
        ),
        format!(
            "CREATE TABLE {database}.clone_at_timestamp CLONE {database}.{source_name} \
             AT (TIMESTAMP => now())"
        ),
    ] {
        assert!(
            fixture.execute_command(&invalid_sql).await.is_err(),
            "invalid clone statement unexpectedly succeeded: {invalid_sql}"
        );
    }

    Ok(())
}
