//! Protocol timestamps: RFC 3339 UTC with millisecond precision and a `Z` suffix.

use jiff::Timestamp;

/// The current time as a protocol timestamp, e.g. `2026-11-23T10:20:30.123Z`.
pub fn now_ts() -> String {
    format_ts(Timestamp::now())
}

/// The protocol timestamp `age` before now.
pub fn ts_before(age: std::time::Duration) -> String {
    let age = jiff::SignedDuration::try_from(age).unwrap_or(jiff::SignedDuration::MAX);
    format_ts(Timestamp::now().checked_sub(age).unwrap_or(Timestamp::MIN))
}

pub(crate) fn format_ts(ts: Timestamp) -> String {
    ts.strftime("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// Whether `value` is an RFC 3339 timestamp that carries a UTC offset.
pub(crate) fn is_rfc3339_with_offset(value: &str) -> bool {
    value.parse::<Timestamp>().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_milliseconds_with_z() {
        let ts: Timestamp = "2026-11-23T10:20:30.123456Z"
            .parse()
            .expect("valid timestamp");
        assert_eq!(format_ts(ts), "2026-11-23T10:20:30.123Z");
        let whole: Timestamp = "2026-11-23T10:20:30Z".parse().expect("valid timestamp");
        assert_eq!(format_ts(whole), "2026-11-23T10:20:30.000Z");
    }

    #[test]
    fn requires_offset() {
        assert!(is_rfc3339_with_offset("2026-11-23T10:20:30.123Z"));
        assert!(is_rfc3339_with_offset("2026-11-23T12:20:30+02:00"));
        assert!(!is_rfc3339_with_offset("2026-11-23T10:20:30"));
        assert!(!is_rfc3339_with_offset("yesterday"));
    }
}
