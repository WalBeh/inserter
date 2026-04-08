use chrono::{DateTime, Utc};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

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

#[derive(Clone)]
pub struct RecordGenerator {
    rng: SmallRng,
    worker_tag: u64,
    sequence: u64,
    regions: [&'static str; 4],
    product_categories: [&'static str; 5],
    event_types: [&'static str; 5],
    user_segments: [&'static str; 4],
    object_values: Vec<Vec<String>>,
    num_objects: usize,
}

impl RecordGenerator {
    pub fn new(num_objects: usize) -> Self {
        let mut rng = SmallRng::from_entropy();
        let worker_tag = rng.gen::<u64>();

        let regions = ["us-east", "us-west", "eu-central", "ap-southeast"];
        let product_categories = ["electronics", "books", "clothing", "home", "sports"];
        let event_types = ["view", "click", "purchase", "cart_add", "cart_remove"];
        let user_segments = ["premium", "standard", "basic", "trial"];

        let mut object_values = Vec::with_capacity(num_objects);
        for i in 0..num_objects {
            let num_vals = rng.gen_range(3..=8);
            let mut vals = Vec::with_capacity(num_vals);
            for j in 0..num_vals {
                vals.push(format!("obj{}_val_{}", i, j));
            }
            object_values.push(vals);
        }

        Self {
            rng,
            worker_tag,
            sequence: 0,
            regions,
            product_categories,
            event_types,
            user_segments,
            object_values,
            num_objects,
        }
    }

    #[inline]
    fn next_id(&mut self) -> String {
        self.sequence = self.sequence.wrapping_add(1);
        format!("{:016x}{:016x}", self.worker_tag, self.sequence)
    }

    pub fn generate_record(&mut self) -> Record {
        let id = self.next_id();
        let timestamp = Utc::now();

        let region = self.regions[self.rng.gen_range(0..self.regions.len())];
        let product_category = self.product_categories[self.rng.gen_range(0..self.product_categories.len())];
        let event_type = self.event_types[self.rng.gen_range(0..self.event_types.len())];
        let user_segment = self.user_segments[self.rng.gen_range(0..self.user_segments.len())];

        let user_id: u32 = self.rng.gen_range(1..=10000);
        let amount: f64 = self.rng.gen_range(1.0..1000.0);
        let quantity: u32 = self.rng.gen_range(1..=100);

        let metadata = json!({
            "browser": format!("Browser-{}", self.rng.gen_range(1..=5u32)),
            "os": format!("OS-{}", self.rng.gen_range(1..=3u32)),
            "session_id": format!("{:016x}{:016x}", self.worker_tag, self.sequence),
            "page_views": self.rng.gen_range(1..=20u32),
            "referrer": format!("ref-{}", self.rng.gen_range(1..=10u32))
        });

        let mut objects = Vec::with_capacity(self.num_objects);
        for i in 0..self.num_objects {
            if let Some(vals) = self.object_values.get(i) {
                let idx = self.rng.gen_range(0..vals.len());
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

    pub fn generate_batch(&mut self, batch_size: usize) -> Vec<Record> {
        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            batch.push(self.generate_record());
        }
        batch
    }
}

impl Record {
    /// Consume the record and convert to params, avoiding clones
    pub fn into_params(self) -> Vec<Value> {
        let mut params = vec![
            Value::String(self.id),
            Value::String(self.timestamp.to_rfc3339()),
            Value::String(self.region),
            Value::String(self.product_category),
            Value::String(self.event_type),
            json!(self.user_id),
            Value::String(self.user_segment),
            json!(self.amount),
            json!(self.quantity),
            self.metadata,
        ];

        for obj_val in self.objects {
            params.push(Value::String(obj_val));
        }

        params
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_generation() {
        let mut generator = RecordGenerator::new(0);
        let record = generator.generate_record();

        assert!(!record.id.is_empty());
        assert_eq!(record.id.len(), 32); // hex counter format
        assert!(record.user_id >= 1 && record.user_id <= 10000);
        assert!(record.amount >= 1.0 && record.amount <= 1000.0);
        assert!(record.quantity >= 1 && record.quantity <= 100);
        assert!(record.objects.is_empty());
    }

    #[test]
    fn test_record_generation_with_objects() {
        let mut generator = RecordGenerator::new(3);
        let record = generator.generate_record();

        assert_eq!(record.objects.len(), 3);
        for (i, obj_val) in record.objects.iter().enumerate() {
            assert!(obj_val.starts_with(&format!("obj{}_val_", i)));
        }
    }

    #[test]
    fn test_batch_generation() {
        let mut generator = RecordGenerator::new(2);
        let batch = generator.generate_batch(10);

        assert_eq!(batch.len(), 10);

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

        let params = record.into_params();
        assert_eq!(params.len(), 12); // 10 base + 2 objects

        assert_eq!(params[0], json!("test-id"));
        assert_eq!(params[2], json!("us-east"));
        assert_eq!(params[5], json!(123));
        assert_eq!(params[10], json!("obj0_val_1"));
        assert_eq!(params[11], json!("obj1_val_2"));
    }
}
