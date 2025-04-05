use std::time::Duration;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Timeout {
    /// Time out after the specified duration elapses.
    After(Duration),
    /// Time out after the specified duration elapses.
    NonBlock,
    /// Block forever.
    Never,
}

impl Timeout {
    /// Converts a timeout to Kafka's expected representation.
    pub(crate) fn as_millis(&self) -> i32 {
        match self {
            Timeout::After(d) => d.as_millis() as i32,
            Timeout::NonBlock => 0,
            Timeout::Never => -1,
        }
    }
}

impl From<Duration> for Timeout {
    fn from(value: Duration) -> Self {
        if value.is_zero() {
            Timeout::NonBlock
        } else {
            Timeout::After(value)
        }
    }
}
