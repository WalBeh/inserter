use chrono::{DateTime, Utc};
use fake::Fake;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub id: String,
    pub timestamp: DateTime<Utc>,
    pub region: String,
    pub product_category: String,
    pub event_type: String,
    pub user_id: u32,
    pub user_segment: String,
    pub amount: f64,
    pub quantity: u32,
    pub metadata: Value,
    pub objects: Vec<String>,
}

pub struct RecordGenerator {
    regions: Vec<&'static str>,
    product_categories: Vec<&'static str>,
    event_types: Vec<&'static str>,
    user_segments: Vec<&'static str>,
    object_values: Vec<Vec<String>>,
    num_objects: usize,
}

impl RecordGenerator {
    pub fn new(num_objects: usize) -> Self {
        let regions = vec!["us-east", "us-west", "eu-central", "ap-southeast"];
        let product_categories = vec!["electronics", "books", "clothing", "home", "sports"];
        let event_types = vec!["view", "click", "purchase", "cart_add", "cart_remove"];
        let user_segments = vec!["premium", "standard", "basic", "trial"];

        // Generate object values for each object column
        let mut object_values = Vec::with_capacity(num_objects);
        for i in 0..num_objects {
            let num_vals = (3..=8).fake::<usize>();
            let mut vals = Vec::with_capacity(num_vals);
            for j in 0..num_vals {
                vals.push(format!("obj{}_val_{}", i, j));
            }
            object_values.push(vals);
        }

        Self {
            regions,
            product_categories,
            event_types,
            user_segments,
            object_values,
            num_objects,
        }
    }

    pub async fn generate_record(&self) -> Record {
        let id = Uuid::new_v4().to_string();
        let timestamp = Utc::now();

        // Select random values with controlled cardinality
        let region = self.regions[(0..self.regions.len()).fake::<usize>()];
        let product_category = self.product_categories[(0..self.product_categories.len()).fake::<usize>()];
        let event_type = self.event_types[(0..self.event_types.len()).fake::<usize>()];
        let user_segment = self.user_segments[(0..self.user_segments.len()).fake::<usize>()];

        // Generate random values
        let user_id: u32 = (1..=10000).fake();
        let amount: f64 = (1.0..1000.0).fake();
        let quantity: u32 = (1..=100).fake();

        // Generate metadata object
        let metadata = json!({
            "browser": format!("Browser-{}", (1..=5).fake::<u32>()),
            "os": format!("OS-{}", (1..=3).fake::<u32>()),
            "session_id": Uuid::new_v4().to_string(),
            "page_views": (1..=20).fake::<u32>(),
            "referrer": format!("ref-{}", (1..=10).fake::<u32>())
        });

        // Generate object column values
        let mut objects = Vec::with_capacity(self.num_objects);
        for i in 0..self.num_objects {
            if let Some(vals) = self.object_values.get(i) {
                let idx = (0..vals.len()).fake::<usize>();
                objects.push(vals[idx].clone());
            } else {
                objects.push(format!("default_obj_{}", i));
            }
        }

        Record {
            id,
            timestamp,
            region: region.to_string(),
            product_category: product_category.to_string(),
            event_type: event_type.to_string(),
            user_id,
            user_segment: user_segment.to_string(),
            amount,
            quantity,
            metadata,
            objects,
        }
    }

    pub async fn generate_batch(&self, batch_size: usize) -> Vec<Record> {
        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            batch.push(self.generate_record().await);
        }
        batch
    }
}

impl Record {
    pub fn to_params(&self) -> Vec<Value> {
        let mut params = vec![
            json!(self.id),
            json!(self.timestamp.to_rfc3339()),
            json!(self.region),
            json!(self.product_category),
            json!(self.event_type),
            json!(self.user_id),
            json!(self.user_segment),
            json!(self.amount),
            json!(self.quantity),
            self.metadata.clone(),
        ];

        // Add object columns
        for obj_val in &self.objects {
            params.push(json!(obj_val));
        }

        params
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_record_generation() {
        let generator = RecordGenerator::new(0);
        let record = generator.generate_record().await;

        assert!(!record.id.is_empty());
        assert!(record.user_id >= 1 && record.user_id <= 10000);
        assert!(record.amount >= 1.0 && record.amount <= 1000.0);
        assert!(record.quantity >= 1 && record.quantity <= 100);
        assert!(record.objects.is_empty());
    }

    #[tokio::test]
    async fn test_record_generation_with_objects() {
        let generator = RecordGenerator::new(3);
        let record = generator.generate_record().await;

        assert_eq!(record.objects.len(), 3);
        for (i, obj_val) in record.objects.iter().enumerate() {
            assert!(obj_val.starts_with(&format!("obj{}_val_", i)));
        }
    }

    #[tokio::test]
    async fn test_batch_generation() {
        let generator = RecordGenerator::new(2);
        let batch = generator.generate_batch(10).await;

        assert_eq!(batch.len(), 10);

        // Verify all records have unique IDs
        let mut ids = std::collections::HashSet::new();
        for record in &batch {
            assert!(ids.insert(record.id.clone()));
        }
    }

    #[test]
    fn test_record_to_params() {
        let record = Record {
            id: "test-id".to_string(),
            timestamp: Utc::now(),
            region: "us-east".to_string(),
            product_category: "electronics".to_string(),
            event_type: "purchase".to_string(),
            user_id: 123,
            user_segment: "premium".to_string(),
            amount: 99.99,
            quantity: 2,
            metadata: json!({"test": "value"}),
            objects: vec!["obj0_val_1".to_string(), "obj1_val_2".to_string()],
        };

        let params = record.to_params();
        assert_eq!(params.len(), 12); // 10 base + 2 objects

        assert_eq!(params[0], json!("test-id"));
        assert_eq!(params[2], json!("us-east"));
        assert_eq!(params[5], json!(123));
        assert_eq!(params[10], json!("obj0_val_1"));
        assert_eq!(params[11], json!("obj1_val_2"));
    }
}
