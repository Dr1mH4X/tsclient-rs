use std::fmt;
use thiserror::Error;

/// Known TeamSpeak server error codes exposed for typed matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
#[repr(u32)]
pub enum ErrorCode {
    CommandNotFound = 0x0100,
    FileInvalidName = 0x0800,
    FileInvalidPermissions = 0x0801,
    FileAlreadyExists = 0x0802,
    FileNotFound = 0x0803,
    FileIOError = 0x0804,
    FileInvalidTransferId = 0x0805,
    FileInvalidPath = 0x0806,
    FileNoFilesAvailable = 0x0807,
    FileInvalidSize = 0x0809,
    FileTransferServerQuotaExceeded = 0x0814,
    FileTransferClientQuotaExceeded = 0x0815,
    PermissionInvalidSize = 0x0a05,
    PermissionsClientInsufficient = 0x0a08,
    Permissions = 0x0a0c,
    StreamNotFound = 0x1603,
    StreamNotParticipating = 0x1604,
    StreamAlreadyJoined = 0x1606,
    StreamJoinRequestRedacted = 0x1607,
}

impl ErrorCode {
    /// Return the numeric server error code.
    pub const fn code(self) -> u32 {
        self as u32
    }

    /// Match a numeric server error code against known variants.
    pub const fn from_code(code: u32) -> Option<Self> {
        match code {
            0x0100 => Some(Self::CommandNotFound),
            0x0800 => Some(Self::FileInvalidName),
            0x0801 => Some(Self::FileInvalidPermissions),
            0x0802 => Some(Self::FileAlreadyExists),
            0x0803 => Some(Self::FileNotFound),
            0x0804 => Some(Self::FileIOError),
            0x0805 => Some(Self::FileInvalidTransferId),
            0x0806 => Some(Self::FileInvalidPath),
            0x0807 => Some(Self::FileNoFilesAvailable),
            0x0809 => Some(Self::FileInvalidSize),
            0x0814 => Some(Self::FileTransferServerQuotaExceeded),
            0x0815 => Some(Self::FileTransferClientQuotaExceeded),
            0x0a05 => Some(Self::PermissionInvalidSize),
            0x0a08 => Some(Self::PermissionsClientInsufficient),
            0x0a0c => Some(Self::Permissions),
            0x1603 => Some(Self::StreamNotFound),
            0x1604 => Some(Self::StreamNotParticipating),
            0x1606 => Some(Self::StreamAlreadyJoined),
            0x1607 => Some(Self::StreamJoinRequestRedacted),
            _ => None,
        }
    }

    /// Parse a decimal `id` string from a server `error` packet.
    pub fn from_id(id: &str) -> Option<Self> {
        let code = id.trim().parse::<u32>().ok()?;
        Self::from_code(code)
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:04x}", self.code())
    }
}

// ---- Individual error types (1:1 with TS) -----------------------------------

#[derive(Debug, Clone)]
pub struct TeamspeakError(pub String);

impl fmt::Display for TeamspeakError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for TeamspeakError {}

#[derive(Debug, Clone)]
pub struct ServerError {
    pub id: String,
    pub server_message: String,
    pub extra_message: Option<String>,
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TeamSpeak server error: {} (id={})",
            self.server_message, self.id
        )
    }
}

impl std::error::Error for ServerError {}

impl ServerError {
    /// Parse `id` into its numeric server error code when present and valid.
    pub fn code(&self) -> Option<u32> {
        self.id.trim().parse::<u32>().ok()
    }

    /// Match `id` against [`ErrorCode`].
    pub fn error_code(&self) -> Option<ErrorCode> {
        ErrorCode::from_id(&self.id)
    }
}

#[derive(Debug, Clone)]
pub struct CommandTimeoutError {
    pub command: String,
}

impl fmt::Display for CommandTimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "command timeout: {}", self.command)
    }
}

impl std::error::Error for CommandTimeoutError {}

#[derive(Debug, Clone)]
pub struct AlreadyConnectedError;

impl fmt::Display for AlreadyConnectedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "already connecting or connected")
    }
}

impl std::error::Error for AlreadyConnectedError {}

#[derive(Debug, Clone)]
pub struct EAXTagMismatchError;

impl fmt::Display for EAXTagMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EAX tag mismatch")
    }
}

impl std::error::Error for EAXTagMismatchError {}

#[derive(Debug, Clone)]
pub struct FakeSignatureMismatchError;

impl fmt::Display for FakeSignatureMismatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fake signature mismatch")
    }
}

impl std::error::Error for FakeSignatureMismatchError {}

#[derive(Debug, Clone)]
pub struct FileTransferError(pub String);

impl fmt::Display for FileTransferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for FileTransferError {}

#[derive(Debug, Clone)]
pub struct FileTransferTimeoutError;

impl fmt::Display for FileTransferTimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "timeout waiting for file transfer notification")
    }
}

impl std::error::Error for FileTransferTimeoutError {}

#[derive(Debug, Clone)]
pub struct CryptoInitError(pub String);

impl fmt::Display for CryptoInitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for CryptoInitError {}

#[derive(Debug, Clone)]
pub struct InvalidIdentityError(pub String);

impl fmt::Display for InvalidIdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for InvalidIdentityError {}

impl Default for InvalidIdentityError {
    fn default() -> Self {
        Self("invalid identity format".to_string())
    }
}

impl InvalidIdentityError {
    pub fn new() -> Self {
        Self::default()
    }
}

// ---- Main error enum (Rust-idiomatic, used internally) ----------------------

#[derive(Error, Debug, Clone)]
pub enum Error {
    #[error("{0}")]
    Teamspeak(String),

    #[error("TeamSpeak server error: {server_message} (id={id})")]
    ServerError {
        id: String,
        server_message: String,
        extra_message: Option<String>,
    },

    #[error("command timeout: {command}")]
    CommandTimeout { command: String },

    #[error("already connecting or connected")]
    AlreadyConnected,

    #[error("EAX tag mismatch")]
    EaxTagMismatch,

    #[error("fake signature mismatch")]
    FakeSignatureMismatch,

    #[error("{0}")]
    FileTransfer(String),

    #[error("timeout waiting for file transfer notification")]
    FileTransferTimeout,

    #[error("{0}")]
    CryptoInit(String),

    #[error("{0}")]
    InvalidIdentity(String),
}

impl Error {
    pub fn invalid_identity() -> Self {
        Error::InvalidIdentity("invalid identity format".to_string())
    }
}

impl From<String> for Error {
    fn from(msg: String) -> Self {
        Error::Teamspeak(msg)
    }
}

impl From<&str> for Error {
    fn from(msg: &str) -> Self {
        Error::Teamspeak(msg.to_string())
    }
}

// ---- From impls: individual error types → Error ----------------------------

impl From<TeamspeakError> for Error {
    fn from(e: TeamspeakError) -> Self {
        Error::Teamspeak(e.0)
    }
}

impl From<ServerError> for Error {
    fn from(e: ServerError) -> Self {
        Error::ServerError {
            id: e.id,
            server_message: e.server_message,
            extra_message: e.extra_message,
        }
    }
}

impl From<CommandTimeoutError> for Error {
    fn from(e: CommandTimeoutError) -> Self {
        Error::CommandTimeout { command: e.command }
    }
}

impl From<AlreadyConnectedError> for Error {
    fn from(_: AlreadyConnectedError) -> Self {
        Error::AlreadyConnected
    }
}

impl From<EAXTagMismatchError> for Error {
    fn from(_: EAXTagMismatchError) -> Self {
        Error::EaxTagMismatch
    }
}

impl From<FakeSignatureMismatchError> for Error {
    fn from(_: FakeSignatureMismatchError) -> Self {
        Error::FakeSignatureMismatch
    }
}

impl From<FileTransferError> for Error {
    fn from(e: FileTransferError) -> Self {
        Error::FileTransfer(e.0)
    }
}

impl From<FileTransferTimeoutError> for Error {
    fn from(_: FileTransferTimeoutError) -> Self {
        Error::FileTransferTimeout
    }
}

impl From<CryptoInitError> for Error {
    fn from(e: CryptoInitError) -> Self {
        Error::CryptoInit(e.0)
    }
}

impl From<InvalidIdentityError> for Error {
    fn from(e: InvalidIdentityError) -> Self {
        Error::InvalidIdentity(e.0)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Teamspeak(e.to_string())
    }
}

impl Error {
    pub fn is_flood_error(&self) -> bool {
        matches!(self, Self::ServerError { id, .. } if id == "524")
    }

    /// Return the server error `id` string when this is [`Error::ServerError`].
    pub fn server_error_id(&self) -> Option<&str> {
        match self {
            Self::ServerError { id, .. } => Some(id.as_str()),
            _ => None,
        }
    }

    /// Return the parsed numeric server error code when this is [`Error::ServerError`].
    pub fn server_error_code(&self) -> Option<u32> {
        self.server_error_id()
            .and_then(|id| id.trim().parse::<u32>().ok())
    }

    /// Match this error's server code against [`ErrorCode`].
    pub fn error_code(&self) -> Option<ErrorCode> {
        self.server_error_id().and_then(ErrorCode::from_id)
    }

    /// Check whether this error carries the given [`ErrorCode`].
    pub fn is_server_error_code(&self, code: ErrorCode) -> bool {
        self.error_code() == Some(code)
    }

    pub(crate) fn flood_retry_delay(&self) -> Option<std::time::Duration> {
        if !self.is_flood_error() {
            return None;
        }
        let Error::ServerError {
            extra_message: Some(extra),
            ..
        } = self
        else {
            return None;
        };
        let (digits, multiplier) = if let Some(digits) = extra
            .strip_prefix("retry in ")
            .and_then(|s| s.strip_suffix("ms"))
        {
            (digits, 1)
        } else if let Some(digits) = extra
            .strip_prefix("please wait ")
            .and_then(|s| s.strip_suffix(" seconds"))
        {
            (digits, 1000)
        } else {
            return None;
        };
        if digits.is_empty() || digits.len() > 9 || !digits.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        Some(std::time::Duration::from_millis(
            digits.parse::<u64>().ok()? * multiplier,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_error_codes_match_wire_decimal_and_hex_values() {
        assert_eq!(ErrorCode::CommandNotFound.code(), 0x0100);
        assert_eq!(ErrorCode::StreamNotFound.code(), 0x1603);
        assert_eq!(ErrorCode::StreamNotParticipating.code(), 0x1604);
        assert_eq!(ErrorCode::StreamAlreadyJoined.code(), 0x1606);
        assert_eq!(ErrorCode::StreamJoinRequestRedacted.code(), 0x1607);

        assert_eq!(ErrorCode::from_id("256"), Some(ErrorCode::CommandNotFound));
        assert_eq!(ErrorCode::from_id("5635"), Some(ErrorCode::StreamNotFound));
        assert_eq!(
            ErrorCode::from_id("5636"),
            Some(ErrorCode::StreamNotParticipating)
        );
        assert_eq!(
            ErrorCode::from_id("5638"),
            Some(ErrorCode::StreamAlreadyJoined)
        );
        assert_eq!(
            ErrorCode::from_id("5639"),
            Some(ErrorCode::StreamJoinRequestRedacted)
        );

        let err = Error::ServerError {
            id: "5635".to_string(),
            server_message: "not found".to_string(),
            extra_message: None,
        };
        assert_eq!(err.server_error_code(), Some(5635));
        assert!(err.is_server_error_code(ErrorCode::StreamNotFound));
    }

    #[test]
    fn file_and_permission_error_codes_match_wire_decimal_and_hex_values() {
        let cases = [
            (ErrorCode::FileInvalidName, 0x0800, "2048"),
            (ErrorCode::FileInvalidPermissions, 0x0801, "2049"),
            (ErrorCode::FileAlreadyExists, 0x0802, "2050"),
            (ErrorCode::FileNotFound, 0x0803, "2051"),
            (ErrorCode::FileIOError, 0x0804, "2052"),
            (ErrorCode::FileInvalidTransferId, 0x0805, "2053"),
            (ErrorCode::FileInvalidPath, 0x0806, "2054"),
            (ErrorCode::FileNoFilesAvailable, 0x0807, "2055"),
            (ErrorCode::FileInvalidSize, 0x0809, "2057"),
            (ErrorCode::FileTransferServerQuotaExceeded, 0x0814, "2068"),
            (ErrorCode::FileTransferClientQuotaExceeded, 0x0815, "2069"),
            (ErrorCode::PermissionInvalidSize, 0x0a05, "2565"),
            (ErrorCode::PermissionsClientInsufficient, 0x0a08, "2568"),
            (ErrorCode::Permissions, 0x0a0c, "2572"),
        ];

        for (variant, hex_code, wire_id) in cases {
            assert_eq!(variant.code(), hex_code);
            assert_eq!(ErrorCode::from_id(wire_id), Some(variant));
            let err = Error::ServerError {
                id: wire_id.to_string(),
                server_message: "refused".to_string(),
                extra_message: None,
            };
            assert!(err.is_server_error_code(variant));
        }
    }
}
