use anyhow::{Context, Result};
use flate2::Compression;
use flate2::write::GzEncoder;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::Write;

use tracing::{debug, error};
use url::Url;
use base64::Engine;

#[derive(Clone)]
pub struct CrateClient {
    client: Client,
    base_url: String,
    auth_header: Option<String>,
}

#[derive(Debug, Serialize)]
struct SqlRequest {
    stmt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    args: Option<Vec<Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bulk_args: Option<Vec<Vec<Value>>>,
}

#[derive(Debug, Deserialize)]
pub struct SqlResponse {
    #[serde(default)]
    pub rows: Vec<Vec<Value>>,
    #[serde(default)]
    pub rowcount: i64,
    #[serde(default)]
    pub duration: f64,
    #[serde(default)]
    cols: Vec<String>,
}

impl CrateClient {
    pub async fn new(connection_string: &str) -> Result<Self> {
        Self::with_pool_size(connection_string, 50).await
    }

    pub async fn with_pool_size(connection_string: &str, pool_size: usize) -> Result<Self> {
        let url = Url::parse(connection_string)
            .with_context(|| format!("Invalid connection string: {}", connection_string))?;

        let base_url = format!(
            "{}://{}:{}",
            url.scheme(),
            url.host_str().context("Missing hostname in connection string")?,
            url.port().unwrap_or(4200)
        );

        // Prepare authentication header if credentials are provided
        let auth_header = if let Some(password) = url.password() {
            let username = url.username();
            let credentials = format!("{}:{}", username, password);
            let encoded = base64::engine::general_purpose::STANDARD.encode(credentials.as_bytes());
            Some(format!("Basic {}", encoded))
        } else {
            None
        };

        // Build HTTP client with optimized settings
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .tcp_keepalive(std::time::Duration::from_secs(60))
            .pool_max_idle_per_host(pool_size)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .user_agent("crate-write-rs/0.1.0")
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            client,
            base_url,
            auth_header,
        })
    }

    async fn make_request(&self, request: &SqlRequest) -> Result<SqlResponse> {
        let url = format!("{}/_sql", self.base_url);

        let mut req_builder = self.client
            .post(&url)
            .json(request)
            .header("Content-Type", "application/json");

        if let Some(ref auth) = self.auth_header {
            req_builder = req_builder.header("Authorization", auth);
        }

        debug!("Executing SQL: {}", request.stmt);

        let response = req_builder
            .send()
            .await
            .with_context(|| format!("Failed to send request to {}", url))?;

        let status = response.status();

        if status.is_success() {
            let sql_response: SqlResponse = response
                .json()
                .await
                .context("Failed to parse JSON response")?;

            debug!("SQL executed successfully, rowcount: {}, duration: {}ms",
                   sql_response.rowcount, sql_response.duration);

            Ok(sql_response)
        } else {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| format!("HTTP {}", status));

            anyhow::bail!("SQL execution failed: HTTP {} - {}", status, error_text);
        }
    }

    pub async fn execute(&self, sql: &str, args: &[Value]) -> Result<()> {
        let request = SqlRequest {
            stmt: sql.to_string(),
            args: if args.is_empty() { None } else { Some(args.to_vec()) },
            bulk_args: None,
        };

        self.make_request(&request).await.map(|_| ())
    }

    /// Accept owned bulk_args, gzip-compress on blocking thread, send to CrateDB.
    /// CPU work (serialize + gzip) runs on spawn_blocking so tokio I/O threads stay free.
    pub async fn execute_bulk(&self, sql: &str, bulk_args: Vec<Vec<Value>>) -> Result<()> {
        if bulk_args.is_empty() {
            return Ok(());
        }

        // Move CPU-heavy work (JSON serialize + gzip) to blocking thread pool
        let sql_owned = sql.to_string();
        let compressed = tokio::task::spawn_blocking(move || -> Result<Vec<u8>> {
            let request = SqlRequest {
                stmt: sql_owned,
                args: None,
                bulk_args: Some(bulk_args),
            };

            let json_bytes = serde_json::to_vec(&request)
                .context("Failed to serialize bulk request")?;

            let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
            encoder.write_all(&json_bytes)
                .context("Failed to gzip compress payload")?;
            encoder.finish()
                .context("Failed to finish gzip compression")
        }).await
            .context("Blocking task panicked")??;

        let url = format!("{}/_sql", self.base_url);
        let mut req_builder = self.client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Content-Encoding", "gzip")
            .body(compressed);

        if let Some(ref auth) = self.auth_header {
            req_builder = req_builder.header("Authorization", auth);
        }

        let response = req_builder
            .send()
            .await
            .with_context(|| format!("Failed to send bulk request to {}", url))?;

        let status = response.status();
        if status.is_success() {
            let resp: SqlResponse = response.json().await
                .context("Failed to parse bulk response")?;
            debug!("Bulk insert successful: {} records in {:.2}ms",
                   resp.rowcount, resp.duration);
            Ok(())
        } else {
            let error_text = response.text().await
                .unwrap_or_else(|_| format!("HTTP {}", status));
            error!("Bulk insert failed: HTTP {} - {}", status, error_text);
            anyhow::bail!("Bulk insert failed: HTTP {} - {}", status, error_text);
        }
    }

    pub async fn execute_query(&self, sql: &str) -> Result<Vec<Vec<Value>>> {
        let request = SqlRequest {
            stmt: sql.to_string(),
            args: None,
            bulk_args: None,
        };

        let resp = self.make_request(&request).await?;
        Ok(resp.rows)
    }

    pub async fn test_connection(&self) -> Result<()> {
        self.execute("SELECT 1", &[]).await?;
        Ok(())
    }

    pub fn get_sanitized_url(&self) -> String {
        self.base_url.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_client_creation() {
        let result = CrateClient::new("http://localhost:4200").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_invalid_url() {
        let result = CrateClient::new("invalid-url").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_url_with_auth() {
        let result = CrateClient::new("http://user:pass@localhost:4200").await;
        assert!(result.is_ok());

        let client = result.unwrap();
        assert!(client.auth_header.is_some());
        assert!(client.auth_header.unwrap().starts_with("Basic "));
    }
}
