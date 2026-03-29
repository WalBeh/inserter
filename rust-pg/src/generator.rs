use chrono::{DateTime, Utc};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};

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
    pub metadata: String,
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
        }
    }

    #[inline]
    fn next_id(&mut self) -> String {
        self.sequence = self.sequence.wrapping_add(1);
        format!("{:016x}{:016x}", self.worker_tag, self.sequence)
    }

    #[inline]
    fn metadata_json(&mut self) -> String {
        format!(
            r#"{{"browser":"Browser-{}","os":"OS-{}","session_id":"{:016x}{:016x}","page_views":{},"referrer":"ref-{}"}}"#,
            self.rng.gen_range(1..=5),
            self.rng.gen_range(1..=3),
            self.worker_tag,
            self.sequence,
            self.rng.gen_range(1..=20),
            self.rng.gen_range(1..=10),
        )
    }

    #[inline]
    pub fn generate_record(&mut self) -> Record {
        let mut objects = Vec::with_capacity(self.object_values.len());
        for values in &self.object_values {
            let idx = self.rng.gen_range(0..values.len());
            objects.push(values[idx].clone());
        }

        let region = self.regions[self.rng.gen_range(0..self.regions.len())].to_owned();
        let product_category = self.product_categories[self.rng.gen_range(0..self.product_categories.len())].to_owned();
        let event_type = self.event_types[self.rng.gen_range(0..self.event_types.len())].to_owned();
        let user_segment = self.user_segments[self.rng.gen_range(0..self.user_segments.len())].to_owned();

        Record {
            id: self.next_id(),
            timestamp: Utc::now(),
            region,
            product_category,
            event_type,
            user_id: self.rng.gen_range(1..=10_000),
            user_segment,
            amount: self.rng.gen_range(1.0..1000.0),
            quantity: self.rng.gen_range(1..=100),
            metadata: self.metadata_json(),
            objects,
        }
    }

    #[inline]
    pub fn generate_batch(&mut self, batch_size: usize) -> Vec<Record> {
        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            batch.push(self.generate_record());
        }
        batch
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
        assert!(record.user_id >= 1 && record.user_id <= 10000);
        assert!(record.amount >= 1.0 && record.amount <= 1000.0);
        assert!(record.quantity >= 1 && record.quantity <= 100);
        assert!(record.objects.is_empty());
        assert!(record.metadata.contains("session_id"));
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
}
