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

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::LazyLock;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::get_license_manager;
use databend_common_meta_types::MatchSeq;
use databend_common_sql::plans::CreateDynamicTablePlan;
use databend_common_users::UserApiProvider;
use databend_storages_common_table_meta::table::OPT_KEY_AS_QUERY;
use databend_storages_common_table_meta::table::OPT_KEY_DYNAMIC;
use databend_storages_common_table_meta::table::OPT_KEY_LIFECYCLE;
use databend_storages_common_table_meta::table::OPT_KEY_TASK_NAME;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

use super::interpreter_table_create::CREATE_TABLE_OPTIONS;

pub struct CreateDynamicInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateDynamicTablePlan,
}

#[async_trait::async_trait]
impl Interpreter for CreateDynamicInterpreter {
    fn name(&self) -> &str {
        "CreateDynamicInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = &self.plan.tenant;

        let license_manager = get_license_manager();
        license_manager
            .manager
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::DynamicTable)?;

        let has_computed_column = self
        .plan
        .schema
        .fields()
        .iter()
        .any(|f| f.computed_expr().is_some());
        if has_computed_column {
            let license_manager = get_license_manager();
            license_manager
                .manager
                .check_enterprise_enabled(self.ctx.get_license_key(), Feature::ComputedColumn)?;
        }

        let quota_api = UserApiProvider::instance().tenant_quota_api(tenant);
        let quota = quota_api.get_quota(MatchSeq::GE(0)).await?.data;
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str()).await?;
        if quota.max_tables_per_database > 0 {
            // Note:
            // max_tables_per_database is a config quota. Default is 0.
            // If a database has lot of tables, list_tables will be slow.
            // So We check get it when max_tables_per_database != 0
            let tables = catalog
                .list_tables(&self.plan.tenant, &self.plan.database)
                .await?;
            if tables.len() >= quota.max_tables_per_database as usize {
                return Err(ErrorCode::TenantQuotaExceeded(format!(
                    "Max tables per database quota exceeded: {}",
                    quota.max_tables_per_database
                )));
            }
        }

        // check and set refresh mode.
        // only single table and support change tracking can incremental.
        // if full, normal table.
        // if incremental, enable change tracking.
        // merge into using () on row_id 

        todo!();
        Ok(PipelineBuildResult::create())
    }
}

pub fn is_valid_create_opt<S: AsRef<str>>(opt_key: S) -> bool {
    CREATE_TABLE_OPTIONS.contains(opt_key.as_ref().to_lowercase().as_str()) ||
    CREATE_DYNAMIC_TABLE_OPTIONS.contains(opt_key.as_ref().to_lowercase().as_str())
}

/// Table option keys that can occur in 'create table statement'.
pub static CREATE_DYNAMIC_TABLE_OPTIONS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    
    r.insert(OPT_KEY_AS_QUERY);
    r.insert(OPT_KEY_DYNAMIC);
    r.insert(OPT_KEY_LIFECYCLE);
    r.insert(OPT_KEY_TASK_NAME);

    r
});
