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

use std::sync::Arc;

use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TimeNavigation;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::RefApi;
use databend_common_users::UserApiProvider;
use databend_storages_common_table_meta::table::OPT_KEY_SOURCE_BASE_TABLE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_SOURCE_TABLE_ID;

use crate::binder::Binder;
use crate::binder::util::TableIdentifier;

const FUSE_ENGINE: &str = "FUSE";

pub(crate) struct SessionBranchTable {
    pub table: Arc<dyn Table>,
    pub branch: Option<String>,
}

impl Binder {
    pub(crate) fn current_session_branch(&self) -> Result<Option<String>> {
        if self.session_branch_disabled {
            return Ok(None);
        }

        let branch = self.ctx.get_settings().get_session_branch()?;
        if branch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(branch))
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn resolve_data_source_with_session_branch(
        &self,
        catalog: &str,
        database: &str,
        table_name: &str,
        explicit_branch: Option<String>,
        navigation: Option<&TimeNavigation>,
        max_batch_size: Option<u64>,
        disable_session_branch: bool,
    ) -> Result<SessionBranchTable> {
        if explicit_branch.is_some() {
            let table = self.resolve_data_source(
                &self.ctx,
                catalog,
                database,
                table_name,
                explicit_branch.as_deref(),
                navigation,
                max_batch_size,
            )?;
            return Ok(SessionBranchTable {
                table,
                branch: explicit_branch,
            });
        }

        let session_branch = match self.current_session_branch()? {
            Some(branch) if !disable_session_branch && catalog == CATALOG_DEFAULT => branch,
            _ => {
                let table = self.resolve_data_source(
                    &self.ctx,
                    catalog,
                    database,
                    table_name,
                    None,
                    navigation,
                    max_batch_size,
                )?;
                return Ok(SessionBranchTable {
                    table,
                    branch: None,
                });
            }
        };

        let base_table = self.resolve_data_source(
            &self.ctx,
            catalog,
            database,
            table_name,
            None,
            navigation,
            max_batch_size,
        )?;

        if base_table.is_stream() {
            self.check_stream_source_under_session_branch(
                base_table.as_ref(),
                database,
                table_name,
                &session_branch,
            )?;
            return Ok(SessionBranchTable {
                table: base_table,
                branch: None,
            });
        }

        if !Self::is_branchable_base_table(base_table.as_ref()) {
            return Ok(SessionBranchTable {
                table: base_table,
                branch: None,
            });
        }

        if Self::contains_tag_navigation(navigation) {
            return Err(ErrorCode::Unimplemented(format!(
                "Unsupported TAG navigation on branch reference `{catalog}.{database}.{table_name}/{session_branch}`"
            )));
        }

        match self.resolve_data_source(
            &self.ctx,
            catalog,
            database,
            table_name,
            Some(session_branch.as_str()),
            navigation,
            max_batch_size,
        ) {
            Ok(table) => Ok(SessionBranchTable {
                table,
                branch: Some(session_branch),
            }),
            Err(err) if err.code() == ErrorCode::UNKNOWN_REFERENCE => Ok(SessionBranchTable {
                table: base_table,
                branch: None,
            }),
            Err(err) => Err(err),
        }
    }

    pub(crate) async fn resolve_write_table_with_session_branch(
        &self,
        table_identifier: &TableIdentifier,
        catalog: &str,
        database: &str,
        table_name: &str,
        explicit_branch: Option<String>,
    ) -> Result<SessionBranchTable> {
        if explicit_branch.is_some() {
            let table = self
                .ctx
                .get_table_with_branch(catalog, database, table_name, explicit_branch.as_deref())
                .await
                .map_err(|err| table_identifier.not_found_suggest_error(err))?;
            return Ok(SessionBranchTable {
                table,
                branch: explicit_branch,
            });
        }

        let Some(session_branch) = self.current_session_branch()? else {
            let table = self
                .ctx
                .get_table_with_branch(catalog, database, table_name, None)
                .await
                .map_err(|err| table_identifier.not_found_suggest_error(err))?;
            return Ok(SessionBranchTable {
                table,
                branch: None,
            });
        };

        if catalog != CATALOG_DEFAULT {
            let table = self
                .ctx
                .get_table_with_branch(catalog, database, table_name, None)
                .await
                .map_err(|err| table_identifier.not_found_suggest_error(err))?;
            return Ok(SessionBranchTable {
                table,
                branch: None,
            });
        }

        let base_table = self
            .ctx
            .get_table_with_branch(catalog, database, table_name, None)
            .await
            .map_err(|err| table_identifier.not_found_suggest_error(err))?;

        if base_table.is_temp() {
            return Ok(SessionBranchTable {
                table: base_table,
                branch: None,
            });
        }

        if !Self::is_branchable_base_table(base_table.as_ref()) {
            return Err(Self::non_branchable_write_error(
                database,
                table_name,
                base_table.engine(),
                &session_branch,
            ));
        }

        match self
            .ctx
            .get_table_with_branch(catalog, database, table_name, Some(session_branch.as_str()))
            .await
        {
            Ok(table) => Ok(SessionBranchTable {
                table,
                branch: Some(session_branch),
            }),
            Err(err) if err.code() == ErrorCode::UNKNOWN_REFERENCE => Err(
                Self::missing_session_branch_error(database, table_name, &session_branch),
            ),
            Err(err) => Err(table_identifier.not_found_suggest_error(err)),
        }
    }

    pub(crate) fn check_create_stream_under_session_branch(
        &self,
        source_table: &str,
        source_branch: Option<&str>,
    ) -> Result<()> {
        let Some(session_branch) = self.current_session_branch()? else {
            return Ok(());
        };

        match source_branch {
            Some(branch) if branch == session_branch => Ok(()),
            Some(branch) => Err(ErrorCode::SemanticError(format!(
                "CREATE STREAM ON TABLE {source_table}/{branch} is not allowed while session_branch='{session_branch}' is active. \
                 Use the current session branch `{source_table}/{session_branch}` or unset session_branch."
            ))),
            None => Err(ErrorCode::SemanticError(format!(
                "CREATE STREAM ON TABLE {source_table} is not allowed while session_branch='{session_branch}' is active. \
                 Create the stream on `{source_table}/{session_branch}` explicitly or unset session_branch."
            ))),
        }
    }

    fn is_branchable_base_table(table: &dyn Table) -> bool {
        table.engine() == FUSE_ENGINE && !table.is_temp() && !table.is_stream()
    }

    fn contains_tag_navigation(navigation: Option<&TimeNavigation>) -> bool {
        matches!(
            navigation,
            Some(TimeNavigation::TimeTravel(NavigationPoint::TableTag(_)))
                | Some(TimeNavigation::Changes {
                    at: NavigationPoint::TableTag(_),
                    ..
                })
                | Some(TimeNavigation::Changes {
                    end: Some(NavigationPoint::TableTag(_)),
                    ..
                })
        )
    }

    fn check_stream_source_under_session_branch(
        &self,
        stream: &dyn Table,
        database: &str,
        stream_name: &str,
        session_branch: &str,
    ) -> Result<()> {
        let source_branch = Self::stream_source_branch(stream)?;
        let stream_desc = format!("{database}.{stream_name}");

        match source_branch.as_deref() {
            Some(branch) if branch == session_branch => Ok(()),
            Some(branch) => Err(ErrorCode::SemanticError(format!(
                "Stream '{stream_desc}' is bound to branch '{branch}', but session_branch='{session_branch}' is active. \
                 Use a stream on the current branch or unset session_branch."
            ))),
            None => Err(ErrorCode::SemanticError(format!(
                "Stream '{stream_desc}' is bound to a base table, but session_branch='{session_branch}' is active. \
                 Create a stream on the current branch, unset session_branch, or query the branch source table directly."
            ))),
        }
    }

    fn stream_source_branch(stream: &dyn Table) -> Result<Option<String>> {
        if !stream.options().contains_key(OPT_KEY_SOURCE_BASE_TABLE_ID) {
            return Ok(None);
        }

        let source_table_id = stream
            .options()
            .get(OPT_KEY_SOURCE_TABLE_ID)
            .ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "stream option '{}' must be set",
                    OPT_KEY_SOURCE_TABLE_ID
                ))
            })?
            .parse::<u64>()
            .map_err(|e| {
                ErrorCode::Internal(format!(
                    "Invalid stream option '{}': {}",
                    OPT_KEY_SOURCE_TABLE_ID, e
                ))
            })?;

        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let stream_desc = stream.get_table_info().desc.clone();
        let branch = databend_common_base::runtime::block_on(async move {
            meta_api
                .get_branch_name_by_id(source_table_id)
                .await
                .map_err(|e| ErrorCode::MetaServiceError(e.to_string()))
        })?
        .ok_or_else(|| {
            ErrorCode::UnknownReference(format!(
                "Source branch id '{}' not found, cannot read from stream {}",
                source_table_id, stream_desc
            ))
        })?;

        Ok(Some(branch))
    }

    fn missing_session_branch_error(database: &str, table_name: &str, branch: &str) -> ErrorCode {
        ErrorCode::UnknownReference(format!(
            "Branch '{branch}' does not exist for table '{database}.{table_name}' under session_branch mode. \
             Create it with `ALTER TABLE {database}.{table_name} CREATE BRANCH {branch}` or unset session_branch."
        ))
    }

    fn non_branchable_write_error(
        database: &str,
        table_name: &str,
        engine: &str,
        branch: &str,
    ) -> ErrorCode {
        ErrorCode::SemanticError(format!(
            "Table '{database}.{table_name}' uses engine '{engine}' and is not affected by session_branch='{branch}'. \
             Unset session_branch before writing to this table."
        ))
    }
}
