use base62::encode;
use futures::{StreamExt, TryStreamExt, stream::iter};
pub use lib_persist_derive::{MapToScyllaRow, MapToScyllaType};
pub use scylla::{client::pager::QueryPager, statement::prepared::PreparedStatement};
use scylla::{
    client::{
        caching_session::{CachingSession, CachingSessionBuilder},
        session_builder::SessionBuilder,
    },
    frame::Compression,
    response::{PagingState, query_result::QueryResult},
    serialize::row::SerializeRow,
    statement::Statement,
    value::CqlTimestamp,
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tinytemplate::TinyTemplate;
use tracing::{debug, error, instrument};
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
pub enum MappingError {
    #[error("Value is missing for field {0}")]
    MissingValue(&'static str),
    #[error("Invalid variant {0}")]
    InvalidVariant(String),
}

#[derive(thiserror::Error, Debug)]
#[error("{error}")]
pub struct PrepareError {
    query: String,
    #[source]
    error: scylla::errors::PrepareError,
}

#[derive(thiserror::Error, Debug)]
pub enum LoadError {
    #[error("{0}")]
    Load(#[from] scylla::errors::NewSessionError),
    #[error("{0}")]
    KeyspaceSetup(#[from] scylla::errors::UseKeyspaceError),
}

#[derive(thiserror::Error, Debug)]
pub enum SetupError {
    #[error("{0}")]
    Execution(#[from] scylla::errors::ExecutionError),
    #[error("{0}")]
    Result(#[from] scylla::response::query_result::IntoRowsResultError),
}

#[derive(thiserror::Error, Debug)]
pub enum MigrationError {
    #[error("Migration {file}:{index} '{statement}' returned {error}")]
    Migration {
        file: String,
        index: i32,
        #[source]
        error: scylla::errors::ExecutionError,
        statement: String,
    },
    #[error("{0}")]
    Execution(#[from] scylla::errors::ExecutionError),
    #[error("{0}")]
    VersionQuery(#[from] scylla::errors::MaybeFirstRowError),
    #[error("{0}")]
    Result(#[from] scylla::response::query_result::IntoRowsResultError),
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    PagerExecution(#[from] scylla::errors::PagerExecutionError),
    #[error("{0}")]
    Execution(#[from] scylla::errors::ExecutionError),
    #[error("{0}")]
    RowsResult(#[from] scylla::errors::IntoRowsResultError),
    #[error("{0}")]
    RowResult(#[from] scylla::errors::MaybeFirstRowError),
    #[error("{0}")]
    NextRow(#[from] scylla::errors::NextRowError),
    #[error("{0}")]
    TypeCheck(#[from] scylla::errors::TypeCheckError),
}

#[derive(Clone)]
pub struct Instance {
    inner: Arc<CachingSession>,
    app_name: Arc<str>,
    app_instance: Uuid,
    app_version: Arc<str>,
}

impl Instance {
    #[instrument(skip(credentials), err)]
    pub async fn new(
        app_instance: Uuid,
        app_name: String,
        app_version: String,
        nodes: &[impl AsRef<str> + std::fmt::Debug + Send + Sync],
        credentials: Option<(
            impl Into<String> + Send + Sync,
            impl Into<String> + Send + Sync,
        )>,
    ) -> Result<Self, LoadError> {
        let mut builder = SessionBuilder::new()
            .known_nodes(nodes)
            .connection_timeout(Duration::from_secs(30))
            .compression(Some(Compression::Lz4));

        if let Some((username, password)) = credentials {
            builder = builder.user(username, password);
        }

        let session = builder.build().await?;

        Ok(Self {
            inner: CachingSessionBuilder::new(session)
                .use_cached_result_metadata(true)
                .build()
                .into(),
            app_name: app_name.into(),
            app_instance,
            app_version: app_version.into(),
        })
    }

    pub async fn set_keyspace(&self) -> Result<(), LoadError> {
        Ok(self
            .inner
            .get_session()
            .use_keyspace(data_keyspace(self.app_instance, &self.app_name), true)
            .await?)
    }

    pub async fn migrate(&self, file: &str, cql: &str) -> Result<(), MigrationError> {
        let meta_keyspace = meta_keyspace();
        let last_index = self
            .inner
            .get_session()
            .query_unpaged(
                format!(
                    r#"select "index" from {meta_keyspace}.migration
                         where app_instance = ? and app_name = ? and file_name = ?"#
                ),
                (self.app_instance, self.app_name.as_ref(), file),
            )
            .await?
            .into_rows_result()?
            .maybe_first_row::<(i32,)>()?
            .map(|r| r.0);

        debug!(file_name = file, last_index, "Run migration");

        iter(
            cql.split(';')
                .map(str::trim)
                .filter(|statement| !statement.is_empty())
                .enumerate()
                .map(|(idx, statement)| (idx.try_into().unwrap(), statement))
                .skip_while(|(idx, _)| last_index.is_some_and(|last_index| *idx <= last_index)),
        )
        .then(async |(idx, statement)| {
            debug!(index = idx, statement = statement, "Executing statement");
            self.inner
                .get_session()
                .query_unpaged(statement, ())
                .await
                .map_err(|err| MigrationError::Migration {
                    file: (file).into(),
                    index: idx,
                    error: err,
                    statement: statement.to_string(),
                })?;

            self.inner
                .get_session()
                .query_unpaged(
                    format!(
                        r#"insert into {meta_keyspace}.migration
                 (app_instance, app_name, file_name, "index", created)
                  values (?, ?, ?, ?, currentTimestamp())"#
                    ),
                    (self.app_instance, self.app_name.as_ref(), file, &idx),
                )
                .await?;
            Ok::<_, MigrationError>(())
        })
        .try_collect::<()>()
        .await?;

        Ok::<_, MigrationError>(())
    }

    pub async fn query(
        &self,
        query: impl Into<Statement>,
        data: impl SerializeRow + Send + Sync,
    ) -> Result<QueryResult, Error> {
        let query = query.into();

        Ok(self
            .inner
            .execute_single_page(query.clone(), data, PagingState::start())
            .await
            .map_err(|err| {
                error!(
                    query = &query.contents,
                    error = &err as &dyn std::error::Error
                );
                Error::Execution(err)
            })?
            .0)
    }

    pub async fn query_iter(
        &self,
        query: impl Into<Statement>,
        data: impl SerializeRow + Send + Sync,
    ) -> Result<QueryPager, Error> {
        let query = query.into();

        self.inner
            .execute_iter(query.clone(), data)
            .await
            .map_err(|err| {
                error!(
                    query = &query.contents,
                    error = &err as &dyn std::error::Error
                );
                Error::PagerExecution(err)
            })
    }

    #[instrument(skip(self), err)]
    pub async fn setup(
        &self,
        replication_factor: usize,
        implementation: Option<&str>,
    ) -> Result<(), SetupError> {
        create_structure(
            &self.inner,
            self.app_instance,
            &self.app_name,
            implementation,
            replication_factor,
        )
        .await?;

        let meta_keyspace = meta_keyspace();

        let existing_version = self
            .inner
            .execute_unpaged(
                format!("select versions from {meta_keyspace}.app where instance = ? and name = ? limit 1"),
                (self.app_instance, self.app_name.as_ref()),
            )
            .await?
            .into_rows_result()?
            .single_row::<(Option<Vec<(String, CqlTimestamp)>>,)>()
            .unwrap()
            .0
            .map(|v| v.into_iter().any(|(v, _)| *v == *self.app_version));

        if existing_version != Some(true) {
            self.inner.execute_unpaged(
                format!("update {meta_keyspace}.app set versions = versions + [(?, currenttimestamp())] where instance = ? and name = ?"),
                (self.app_version.as_ref(), self.app_instance, self.app_name.as_ref()),
            ).await?;
        }

        Ok(())
    }

    #[allow(dead_code)]
    #[cfg(debug_assertions)]
    pub async fn drop_keyspace(&self) -> Result<(), SetupError> {
        self.inner
            .execute_unpaged(
                format!(
                    r#"drop keyspace if exists "{}""#,
                    &data_keyspace(self.app_instance, &self.app_name)
                ),
                (),
            )
            .await?;
        self.inner
            .execute_unpaged(format!("drop keyspace if exists {}", meta_keyspace()), ())
            .await?;
        Ok(())
    }
}

#[instrument(skip(session), err)]
async fn create_structure(
    session: &CachingSession,
    instance: Uuid,
    name: &str,
    implementation: Option<&str>,
    replication_factor: usize,
) -> Result<(), SetupError> {
    let meta_keyspace = meta_keyspace();
    let data_keyspace = data_keyspace(instance, name);

    let mut tt = TinyTemplate::new();
    tt.add_template("structure", include_str!("../structure.cql"))
        .unwrap();

    iter(
        tt.render(
            "structure",
            &HashMap::from([
                ("meta_keyspace", meta_keyspace),
                ("data_keyspace", &data_keyspace),
                ("replication_factor", &replication_factor.to_string()),
            ]),
        )
        .unwrap()
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty())
        .enumerate(),
    )
    .then(async |(idx, statement)| {
        debug!(index = idx, statement = statement, "Executing statement");
        session.execute_unpaged(statement, ()).await?;
        Ok::<_, SetupError>(())
    })
    .try_collect::<()>()
    .await?;

    session
        .execute_unpaged(
            format!(
                r#"insert into {meta_keyspace}.app (instance, name, "keyspace", implementation) values (?, ?, ?,?) if not exists"#
            ),
            (instance, name, &data_keyspace, implementation),
        )
        .await?;

    Ok(())
}

fn data_keyspace(instance: Uuid, name: &str) -> String {
    format!("{}_{}", name.replace('-', "_"), encode(instance.as_u128()))
}

const fn meta_keyspace() -> &'static str {
    "app_metadata"
}
