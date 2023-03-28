// Copyright 2022 Datafuse Labs.
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

use common_config::InnerConfig;
use common_exception::Result;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;
use databend_query::sessions::TableContext;
use databend_query::storages::fuse::FuseTable;
use databend_query::GlobalServices;

pub async fn correct_segment(conf: &InnerConfig) -> Result<()> {
    let database = if conf.check.database.is_empty() {
        "default"
    } else {
        &conf.check.database
    };

    let table = &conf.check.table;
    let enable_commit = conf.check.enable_commit;
    let ignore_unfound_segment = conf.check.ignore_unfound_segment;

    let mut conf = conf.clone();
    conf.storage.allow_insecure = true;
    GlobalServices::init(conf).await?;

    if table.is_empty() {
        eprintln!("table name must be specified");
        return Ok(());
    }

    let res: Result<_> = try {
        let session = SessionManager::instance()
            .create_session(SessionType::Local)
            .await?;
        let ctx = session.create_query_context().await?;
        let cat = ctx.get_catalog("default")?;
        let tenant = ctx.get_tenant();

        let table = cat.get_table(tenant.as_str(), database, table).await?;

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;

        fuse_table
            .correct_segment(ctx, enable_commit, ignore_unfound_segment)
            .await?;
    };

    res
}
