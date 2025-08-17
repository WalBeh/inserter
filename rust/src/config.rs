use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub table_name: Option<String>,
    pub connection_string: Option<String>,
    pub duration: Option<u64>,
    pub batch_size: usize,
    pub batch_interval: u64,
    pub threads: usize,
    pub objects: usize,
    #[cfg(feature = "dashboard")]
    pub dashboard: bool,
    pub log_level: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            table_name: None,
            connection_string: None,
            duration: None,
            batch_size: 100,
            batch_interval: 100,
            threads: 1,
            objects: 0,
            #[cfg(feature = "dashboard")]
            dashboard: false,
            log_level: "info".to_string(),
        }
    }
}

impl Config {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read config file: {:?}", path.as_ref()))?;

        let config: Config = if path.as_ref().extension().and_then(|s| s.to_str()) == Some("toml") {
            toml::from_str(&content)
                .with_context(|| format!("Failed to parse TOML config: {:?}", path.as_ref()))?
        } else {
            serde_json::from_str(&content)
                .with_context(|| format!("Failed to parse JSON config: {:?}", path.as_ref()))?
        };

        Ok(config)
    }

    pub fn to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = if path.as_ref().extension().and_then(|s| s.to_str()) == Some("toml") {
            toml::to_string_pretty(self)
                .context("Failed to serialize config to TOML")?
        } else {
            serde_json::to_string_pretty(self)
                .context("Failed to serialize config to JSON")?
        };

        std::fs::write(path.as_ref(), content)
            .with_context(|| format!("Failed to write config file: {:?}", path.as_ref()))?;

        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if self.connection_string.is_none() {
            anyhow::bail!("Connection string is required");
        }

        if self.table_name.is_none() {
            anyhow::bail!("Table name is required");
        }

        if self.duration.is_none() {
            anyhow::bail!("Duration is required");
        }

        if self.batch_size == 0 {
            anyhow::bail!("Batch size must be greater than 0");
        }

        if self.threads == 0 {
            anyhow::bail!("Number of threads must be greater than 0");
        }

        if self.threads > 1000 {
            anyhow::bail!("Number of threads cannot exceed 1000");
        }

        if self.batch_size > 10000 {
            anyhow::bail!("Batch size cannot exceed 10000");
        }

        if self.batch_interval > 60000 {
            anyhow::bail!("Batch interval cannot exceed 60 seconds");
        }

        // Validate connection string format
        if let Some(ref conn_str) = self.connection_string {
            url::Url::parse(conn_str)
                .with_context(|| format!("Invalid connection string format: {}", conn_str))?;
        }

        Ok(())
    }

    pub fn get_sanitized_connection_string(&self) -> Option<String> {
        self.connection_string.as_ref().map(|conn_str| {
            match url::Url::parse(conn_str) {
                Ok(url) => {
                    format!("{}://{}:{}",
                        url.scheme(),
                        url.host_str().unwrap_or("unknown"),
                        url.port().unwrap_or(4200)
                    )
                }
                Err(_) => "invalid-connection-string".to_string(),
            }
        })
    }

    pub fn merge_env_vars(&mut self) -> Result<()> {
        // Override with environment variables if not already set
        if self.connection_string.is_none() {
            if let Ok(conn_str) = std::env::var("CRATE_CONNECTION_STRING") {
                self.connection_string = Some(conn_str);
            }
        }

        if let Ok(log_level) = std::env::var("LOG_LEVEL") {
            self.log_level = log_level;
        }

        if let Ok(batch_size) = std::env::var("CRATE_BATCH_SIZE") {
            self.batch_size = batch_size.parse()
                .context("Invalid CRATE_BATCH_SIZE environment variable")?;
        }

        if let Ok(threads) = std::env::var("CRATE_THREADS") {
            self.threads = threads.parse()
                .context("Invalid CRATE_THREADS environment variable")?;
        }

        Ok(())
    }

    pub fn estimate_memory_usage(&self) -> u64 {
        // Rough estimate of memory usage in bytes
        let record_size = 1024; // Approximate size per record in bytes
        let buffer_size = self.batch_size * record_size * self.threads;
        buffer_size as u64
    }

    pub fn get_total_estimated_records(&self) -> Option<u64> {
        self.duration.map(|duration| {
            let batches_per_second = if self.batch_interval > 0 {
                1000.0 / self.batch_interval as f64
            } else {
                1000.0 // Assume 1000 batches per second if no interval
            };

            let total_batches = duration * 60 * batches_per_second as u64 * self.threads as u64;
            total_batches * self.batch_size as u64
        })
    }

    pub fn print_summary(&self) {
        println!("📋 Configuration Summary:");
        println!("   Table: {}", self.table_name.as_deref().unwrap_or("N/A"));
        if let Some(sanitized) = self.get_sanitized_connection_string() {
            println!("   Connection: {}", sanitized);
        }
        if let Some(duration) = self.duration {
            println!("   Duration: {} minutes", duration);
        }
        println!("   Batch size: {}", self.batch_size);
        println!("   Batch interval: {}ms", self.batch_interval);
        println!("   Worker threads: {}", self.threads);
        if self.objects > 0 {
            println!("   Object columns: {}", self.objects);
        }

        let memory_mb = self.estimate_memory_usage() / 1024 / 1024;
        if memory_mb > 0 {
            println!("   Estimated memory usage: {}MB", memory_mb);
        }

        if let Some(total_records) = self.get_total_estimated_records() {
            println!("   Estimated total records: {}", total_records);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::NamedTempFile;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.threads, 1);
        assert_eq!(config.objects, 0);
        assert_eq!(config.log_level, "info");
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();

        // Should fail validation without required fields
        assert!(config.validate().is_err());

        // Add required fields
        config.table_name = Some("test_table".to_string());
        config.connection_string = Some("http://localhost:4200".to_string());
        config.duration = Some(5);

        // Should pass validation now
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_invalid_values_validation() {
        let mut config = Config::default();
        config.table_name = Some("test_table".to_string());
        config.connection_string = Some("http://localhost:4200".to_string());
        config.duration = Some(5);

        // Test invalid batch size
        config.batch_size = 0;
        assert!(config.validate().is_err());

        config.batch_size = 100;

        // Test invalid thread count
        config.threads = 0;
        assert!(config.validate().is_err());

        config.threads = 1001;
        assert!(config.validate().is_err());

        config.threads = 1;
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_connection_string_sanitization() {
        let mut config = Config::default();

        config.connection_string = Some("http://user:pass@localhost:4200".to_string());
        let sanitized = config.get_sanitized_connection_string().unwrap();
        assert_eq!(sanitized, "http://localhost:4200");

        config.connection_string = Some("https://user:pass@example.com:5432".to_string());
        let sanitized = config.get_sanitized_connection_string().unwrap();
        assert_eq!(sanitized, "https://example.com:5432");
    }

    #[test]
    fn test_config_file_json() -> Result<()> {
        let config = Config {
            table_name: Some("test_table".to_string()),
            connection_string: Some("http://localhost:4200".to_string()),
            duration: Some(10),
            batch_size: 200,
            threads: 4,
            ..Default::default()
        };

        let temp_file = NamedTempFile::new()?;
        let path = temp_file.path().with_extension("json");

        config.to_file(&path)?;
        let loaded_config = Config::from_file(&path)?;

        assert_eq!(loaded_config.table_name, config.table_name);
        assert_eq!(loaded_config.batch_size, config.batch_size);
        assert_eq!(loaded_config.threads, config.threads);

        fs::remove_file(&path).ok();
        Ok(())
    }

    #[test]
    fn test_memory_estimation() {
        let config = Config {
            batch_size: 100,
            threads: 4,
            ..Default::default()
        };

        let memory = config.estimate_memory_usage();
        assert!(memory > 0);
        assert_eq!(memory, 100 * 1024 * 4); // batch_size * record_size * threads
    }

    #[test]
    fn test_record_estimation() {
        let config = Config {
            duration: Some(2), // 2 minutes
            batch_size: 100,
            batch_interval: 1000, // 1 second
            threads: 2,
            ..Default::default()
        };

        let estimated = config.get_total_estimated_records().unwrap();
        // 2 minutes * 60 seconds * 1 batch/second * 2 threads * 100 records/batch = 24000
        assert_eq!(estimated, 24000);
    }
}
