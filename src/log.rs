//! A wrapper module to export logging functionality from
//! [`log`] or [`tracing`] depending on the `tracing` feature.
//!
//! [`log`]: https://docs.rs/log
//! [`tracing`]: https://docs.rs/tracing

use std::str;

#[cfg(not(feature = "tracing"))]
pub use log::{debug, error, info, trace, warn};

use num_enum::TryFromPrimitive;
#[cfg(feature = "tracing")]
pub use tracing::{debug, enabled as log_enabled, error, info, trace, warn};
#[cfg(feature = "tracing")]
pub const DEBUG: tracing::Level = tracing::Level::DEBUG;
#[cfg(feature = "tracing")]
pub const INFO: tracing::Level = tracing::Level::INFO;
#[cfg(feature = "tracing")]
pub const WARN: tracing::Level = tracing::Level::WARN;

/// Syslog [log levels](https://en.wikipedia.org/wiki/Syslog#Severity_level)
/// The log levels supported by librdkafka.
#[derive(Copy, Clone, Debug, TryFromPrimitive)]
#[repr(i32)]
pub enum RDKafkaSyslogLogLevel {
    /// Higher priority then [`Level::Error`](log::Level::Error) from the log
    /// crate.
    Emerg = libc::LOG_EMERG,
    /// Higher priority then [`Level::Error`](log::Level::Error) from the log
    /// crate.
    Alert = libc::LOG_ALERT,
    /// Higher priority then [`Level::Error`](log::Level::Error) from the log
    /// crate.
    Critical = libc::LOG_CRIT,
    /// Equivalent to [`Level::Error`](log::Level::Error) from the log crate.
    Error = libc::LOG_ERR,
    /// Equivalent to [`Level::Warn`](log::Level::Warn) from the log crate.
    Warning = libc::LOG_WARNING,
    /// Higher priority then [`Level::Info`](log::Level::Info) from the log
    /// crate.
    Notice = libc::LOG_NOTICE,
    /// Equivalent to [`Level::Info`](log::Level::Info) from the log crate.
    Info = libc::LOG_INFO,
    /// Equivalent to [`Level::Debug`](log::Level::Debug) from the log crate.
    Debug = libc::LOG_DEBUG,
}

impl From<RDKafkaSyslogLogLevel> for &'static str {
    fn from(value: RDKafkaSyslogLogLevel) -> Self {
        match value {
            RDKafkaSyslogLogLevel::Emerg => "0",
            RDKafkaSyslogLogLevel::Alert => "1",
            RDKafkaSyslogLogLevel::Critical => "2",
            RDKafkaSyslogLogLevel::Error => "3",
            RDKafkaSyslogLogLevel::Warning => "4",
            RDKafkaSyslogLogLevel::Notice => "5",
            RDKafkaSyslogLogLevel::Info => "6",
            RDKafkaSyslogLogLevel::Debug => "7",
        }
    }
}

/// Log levels from [kafka spec](https://cwiki.apache.org/confluence/display/KAFKA/KIP-412%3A+Extend+Admin+API+to+support+dynamic+application+log+levels#KIP412:ExtendAdminAPItosupportdynamicapplicationloglevels-LogLevelDefinitions)
#[derive(Copy, Clone, Debug)]
pub enum RDKafkaLogLevel {
    /// FATAL logs - syslog level 0
    Fatal = 0,
    /// ERROR logs - syslog level 3 and above
    Error,
    /// WARN logs - syslog level 4 and above
    Warn,
    /// INFO logs - syslog level 6 and above
    Info,
    /// DEBUG logs - syslog level 7 and above
    Debug,
    /// TRACE logs - syslog level 7 and above
    // FIXME: maybe remove
    #[allow(unused)]
    Trace,
}

impl From<RDKafkaLogLevel> for RDKafkaSyslogLogLevel {
    fn from(value: RDKafkaLogLevel) -> Self {
        match value {
            RDKafkaLogLevel::Fatal => Self::Critical,
            RDKafkaLogLevel::Error => Self::Error,
            RDKafkaLogLevel::Warn => Self::Warning,
            RDKafkaLogLevel::Info => Self::Info,
            RDKafkaLogLevel::Debug => Self::Debug,
            RDKafkaLogLevel::Trace => Self::Debug,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DebugContext {
    Generic,
    Broker,
    Topic,
    Metadata,
    Feature,
    Queue,
    Msg,
    Protocol,
    Cgrp,
    Security,
    Fetch,
    Interceptor,
    Plugin,
    Consumer,
    Admin,
    Eos,
    Mock,
    Assignor,
    Conf,
    Telemetry,
    All,
}

impl From<DebugContext> for &'static str {
    fn from(value: DebugContext) -> Self {
        match value {
            DebugContext::Generic => "generic",
            DebugContext::Broker => "broker",
            DebugContext::Topic => "topic",
            DebugContext::Metadata => "metadata",
            DebugContext::Feature => "feature",
            DebugContext::Queue => "queue",
            DebugContext::Msg => "msg",
            DebugContext::Protocol => "protocol",
            DebugContext::Cgrp => "cgrp",
            DebugContext::Security => "security",
            DebugContext::Fetch => "fetch",
            DebugContext::Interceptor => "interceptor",
            DebugContext::Plugin => "plugin",
            DebugContext::Consumer => "consumer",
            DebugContext::Admin => "admin",
            DebugContext::Eos => "eos",
            DebugContext::Mock => "mock",
            DebugContext::Assignor => "assignor",
            DebugContext::Conf => "conf",
            DebugContext::Telemetry => "telemetry",
            DebugContext::All => "all",
        }
    }
}

impl From<RDKafkaSyslogLogLevel> for RDKafkaLogLevel {
    fn from(value: RDKafkaSyslogLogLevel) -> Self {
        match value {
            RDKafkaSyslogLogLevel::Emerg => Self::Fatal,
            RDKafkaSyslogLogLevel::Alert => Self::Fatal,
            RDKafkaSyslogLogLevel::Critical => Self::Fatal,
            RDKafkaSyslogLogLevel::Error => Self::Error,
            RDKafkaSyslogLogLevel::Warning => Self::Warn,
            RDKafkaSyslogLogLevel::Notice => Self::Warn,
            RDKafkaSyslogLogLevel::Info => Self::Info,
            RDKafkaSyslogLogLevel::Debug => Self::Debug,
        }
    }
}
