#[derive(Clone)]
pub struct Config {
    /// Segment file bloom filters will be sized based on this desired false positive rate.
    pub bloom_filter_false_positive_rate: f32,

    /// How often the segment files of the database will be compacted in seconds.
    pub compactor_interval_seconds: u64,

    /// When the memtable reaches this number of keys it will be flushed and a new segment file will be created.
    pub memtable_capacity: usize,

    /// Segment file sparse indices will include every N keys where N is the value of this field.
    pub sparse_index_range_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bloom_filter_false_positive_rate: 0.0001,
            compactor_interval_seconds: 10,
            memtable_capacity: 32,
            sparse_index_range_size: 4,
        }
    }
}
