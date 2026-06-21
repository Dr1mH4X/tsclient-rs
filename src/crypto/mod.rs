//! Crypto module — ECDH, EAX, identity management

pub mod crypt;
pub mod eax;
pub mod identity;
pub mod primitives;

pub use crypt::Crypt;
pub use eax::EAX;
pub use identity::{
    generate_identity, get_uid_from_public_key, hash512, identity_from_string, import_public_key,
    Identity,
};
pub use primitives::{
    clamp_scalar, derive_license_key, generate_temporary_key, get_shared_secret2, sign, verify_sign,
};
