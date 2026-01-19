// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::{Result, anyhow};
use multibase::Base;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Number of bits used for local offset in Vid/Eid
const LOCAL_OFFSET_BITS: u32 = 48;

/// Maximum value for local offset (2^48 - 1)
const MAX_LOCAL_OFFSET: u64 = (1 << LOCAL_OFFSET_BITS) - 1;

/// Bitmask to extract local offset from Vid/Eid
const LOCAL_OFFSET_MASK: u64 = MAX_LOCAL_OFFSET;

/// Internal Vertex ID (64 bits)
/// ┌─────────────────┬────────────────────────────────────────────┐
/// │ label_id (16)   │ local_offset (48)                          │
/// └─────────────────┴────────────────────────────────────────────┘
#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Vid(u64);

impl Vid {
    /// Creates a new vertex ID from label ID and local offset.
    ///
    /// # Panics
    ///
    /// Panics if `local_offset` exceeds the maximum allowed value (2^48 - 1).
    /// This is a programming error that would cause silent data corruption
    /// if allowed to proceed.
    ///
    /// # Security
    ///
    /// **CWE-190 (Integer Overflow)**: Runtime check prevents silent corruption
    /// of the label_id bits from oversized offsets. Unlike `debug_assert!`,
    /// this check remains active in release builds.
    pub fn new(label_id: u16, local_offset: u64) -> Self {
        assert!(
            local_offset <= MAX_LOCAL_OFFSET,
            "Vid local_offset {} exceeds maximum {}",
            local_offset,
            MAX_LOCAL_OFFSET
        );
        Self(((label_id as u64) << LOCAL_OFFSET_BITS) | local_offset)
    }

    pub fn label_id(&self) -> u16 {
        (self.0 >> LOCAL_OFFSET_BITS) as u16
    }

    pub fn local_offset(&self) -> u64 {
        self.0 & LOCAL_OFFSET_MASK
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Vid {
    fn from(val: u64) -> Self {
        Self(val)
    }
}

impl fmt::Debug for Vid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Vid({}:{:#x})", self.label_id(), self.local_offset())
    }
}

impl fmt::Display for Vid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.label_id(), self.local_offset())
    }
}

impl FromStr for Vid {
    type Err = anyhow::Error;

    /// Parses a Vid from string format "label_id:local_offset".
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The string doesn't contain exactly one ':'
    /// - The label_id part cannot be parsed as u16
    /// - The local_offset part cannot be parsed as u64
    /// - The local_offset exceeds the maximum allowed value
    fn from_str(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(anyhow!(
                "Invalid Vid format '{}': expected 'label_id:offset'",
                s
            ));
        }
        let label_id: u16 = parts[0]
            .parse()
            .map_err(|e| anyhow!("Invalid Vid label_id '{}': {}", parts[0], e))?;
        let offset: u64 = parts[1]
            .parse()
            .map_err(|e| anyhow!("Invalid Vid offset '{}': {}", parts[1], e))?;

        if offset > MAX_LOCAL_OFFSET {
            return Err(anyhow!(
                "Vid offset {} exceeds maximum {}",
                offset,
                MAX_LOCAL_OFFSET
            ));
        }
        Ok(Self::new(label_id, offset))
    }
}

/// Internal Edge ID (64 bits)
/// ┌─────────────────┬────────────────────────────────────────────┐
/// │ type_id (16)    │ local_offset (48)                          │
/// └─────────────────┴────────────────────────────────────────────┘
#[derive(Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Eid(u64);

impl Eid {
    /// Creates a new edge ID from type ID and local offset.
    ///
    /// # Panics
    ///
    /// Panics if `local_offset` exceeds the maximum allowed value (2^48 - 1).
    /// This is a programming error that would cause silent data corruption
    /// if allowed to proceed.
    ///
    /// # Security
    ///
    /// **CWE-190 (Integer Overflow)**: Runtime check prevents silent corruption
    /// of the type_id bits from oversized offsets. Unlike `debug_assert!`,
    /// this check remains active in release builds.
    pub fn new(type_id: u16, local_offset: u64) -> Self {
        assert!(
            local_offset <= MAX_LOCAL_OFFSET,
            "Eid local_offset {} exceeds maximum {}",
            local_offset,
            MAX_LOCAL_OFFSET
        );
        Self(((type_id as u64) << LOCAL_OFFSET_BITS) | local_offset)
    }

    pub fn type_id(&self) -> u16 {
        (self.0 >> LOCAL_OFFSET_BITS) as u16
    }

    pub fn local_offset(&self) -> u64 {
        self.0 & LOCAL_OFFSET_MASK
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Eid {
    fn from(val: u64) -> Self {
        Self(val)
    }
}

impl fmt::Debug for Eid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Eid({}:{:#x})", self.type_id(), self.local_offset())
    }
}

impl fmt::Display for Eid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.type_id(), self.local_offset())
    }
}

impl FromStr for Eid {
    type Err = anyhow::Error;

    /// Parses an Eid from string format "type_id:local_offset".
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The string doesn't contain exactly one ':'
    /// - The type_id part cannot be parsed as u16
    /// - The local_offset part cannot be parsed as u64
    /// - The local_offset exceeds the maximum allowed value
    fn from_str(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(anyhow!(
                "Invalid Eid format '{}': expected 'type_id:offset'",
                s
            ));
        }
        let type_id: u16 = parts[0]
            .parse()
            .map_err(|e| anyhow!("Invalid Eid type_id '{}': {}", parts[0], e))?;
        let offset: u64 = parts[1]
            .parse()
            .map_err(|e| anyhow!("Invalid Eid offset '{}': {}", parts[1], e))?;

        if offset > MAX_LOCAL_OFFSET {
            return Err(anyhow!(
                "Eid offset {} exceeds maximum {}",
                offset,
                MAX_LOCAL_OFFSET
            ));
        }
        Ok(Self::new(type_id, offset))
    }
}

/// UniId: 44-character base32 multibase string (SHA3-256)
#[derive(Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct UniId([u8; 32]);

impl UniId {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Parses a UniId from a multibase-encoded string.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The string is not valid multibase
    /// - The encoding is not Base32Lower (the canonical format for UniId)
    /// - The decoded length is not exactly 32 bytes
    ///
    /// # Security
    ///
    /// **CWE-345 (Insufficient Verification)**: Validates that the input uses
    /// the expected Base32Lower encoding, rejecting other multibase formats
    /// that could cause interoperability issues or confusion.
    pub fn from_multibase(s: &str) -> Result<Self> {
        let (base, bytes) =
            multibase::decode(s).map_err(|e| anyhow!("Multibase decode error: {}", e))?;

        // Validate encoding matches our canonical format
        if base != Base::Base32Lower {
            return Err(anyhow!(
                "UniId must use Base32Lower encoding, got {:?}",
                base
            ));
        }

        let inner: [u8; 32] = bytes.try_into().map_err(|v: Vec<u8>| {
            anyhow!("Invalid UniId length: expected 32 bytes, got {}", v.len())
        })?;

        Ok(Self(inner))
    }

    pub fn to_multibase(&self) -> String {
        multibase::encode(Base::Base32Lower, self.0)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for UniId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UniId({})", self.to_multibase())
    }
}

impl fmt::Display for UniId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_multibase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vid_packing() {
        let vid = Vid::new(1, 0x12345678);
        assert_eq!(vid.label_id(), 1);
        assert_eq!(vid.local_offset(), 0x12345678);
    }

    #[test]
    fn test_eid_packing() {
        let eid = Eid::new(5, 0xABCDEF);
        assert_eq!(eid.type_id(), 5);
        assert_eq!(eid.local_offset(), 0xABCDEF);
    }

    #[test]
    fn test_uni_id_multibase() {
        let bytes = [0u8; 32];
        let uid = UniId(bytes);
        let s = uid.to_multibase();
        let decoded = UniId::from_multibase(&s).unwrap();
        assert_eq!(uid, decoded);
    }

    #[test]
    fn test_vid_from_str() {
        // Valid parsing
        let vid: Vid = "1:42".parse().unwrap();
        assert_eq!(vid.label_id(), 1);
        assert_eq!(vid.local_offset(), 42);

        // Round-trip through Display and FromStr
        let original = Vid::new(100, 0x12345678);
        let s = original.to_string();
        let parsed: Vid = s.parse().unwrap();
        assert_eq!(original, parsed);

        // Error cases
        assert!("invalid".parse::<Vid>().is_err());
        assert!("1:2:3".parse::<Vid>().is_err());
        assert!("abc:42".parse::<Vid>().is_err());
        assert!("1:abc".parse::<Vid>().is_err());
    }

    #[test]
    fn test_eid_from_str() {
        // Valid parsing
        let eid: Eid = "5:100".parse().unwrap();
        assert_eq!(eid.type_id(), 5);
        assert_eq!(eid.local_offset(), 100);

        // Round-trip through Display and FromStr
        let original = Eid::new(200, 0xABCDEF);
        let s = original.to_string();
        let parsed: Eid = s.parse().unwrap();
        assert_eq!(original, parsed);

        // Error cases
        assert!("invalid".parse::<Eid>().is_err());
        assert!("1:2:3".parse::<Eid>().is_err());
        assert!("abc:42".parse::<Eid>().is_err());
        assert!("1:abc".parse::<Eid>().is_err());
    }

    /// Security tests for CWE-190 (Integer Overflow) and CWE-345 (Insufficient Verification).
    mod security_tests {
        use super::*;

        /// CWE-190: Vid offset overflow should panic, not silently corrupt.
        #[test]
        #[should_panic(expected = "exceeds maximum")]
        fn test_vid_offset_overflow_panics() {
            // MAX_LOCAL_OFFSET is (1 << 48) - 1 = 0xFFFF_FFFF_FFFF
            // Exceeding this should panic
            Vid::new(1, MAX_LOCAL_OFFSET + 1);
        }

        /// CWE-190: Eid offset overflow should panic, not silently corrupt.
        #[test]
        #[should_panic(expected = "exceeds maximum")]
        fn test_eid_offset_overflow_panics() {
            Eid::new(1, MAX_LOCAL_OFFSET + 1);
        }

        /// Verify maximum valid offset works correctly.
        #[test]
        fn test_vid_max_offset_valid() {
            let vid = Vid::new(0xFFFF, MAX_LOCAL_OFFSET);
            assert_eq!(vid.label_id(), 0xFFFF);
            assert_eq!(vid.local_offset(), MAX_LOCAL_OFFSET);
        }

        /// CWE-345: UniId should reject non-Base32Lower encodings.
        #[test]
        fn test_uni_id_rejects_wrong_encoding() {
            // Create a Base58Btc encoded string (different from our Base32Lower)
            let bytes = [0u8; 32];
            let base58_encoded = multibase::encode(multibase::Base::Base58Btc, bytes);

            let result = UniId::from_multibase(&base58_encoded);
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("Base32Lower encoding")
            );
        }

        /// CWE-345: UniId should reject wrong length.
        #[test]
        fn test_uni_id_rejects_wrong_length() {
            // Encode only 16 bytes instead of 32
            let short_bytes = [0u8; 16];
            let encoded = multibase::encode(Base::Base32Lower, short_bytes);

            let result = UniId::from_multibase(&encoded);
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("expected 32 bytes")
            );
        }
    }
}
