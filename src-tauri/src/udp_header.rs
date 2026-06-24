//! UDP header parsing for UDP 2-layer dial-out.
//!
//! Zero-copy: all parsing operates on `&[u8]` slices without allocation.
//! Replaces Python's `UdpDialoutServer.parse_udp_header` and `parse_udp_header_mobile`.

use crate::error::{AppError, Result};
use crate::models::{UDPHeader, UDPHeaderMobile, UdpHeaderOption};

/// Minimum UDP header size (no option field).
const UDP_HEADER_MIN_SIZE: usize = 12;
/// UDP header size with option field.
const UDP_HEADER_WITH_OPTION_SIZE: usize = 16;
/// Maximum UDP datagram size.
const UDP_MAX_SIZE: usize = 65535;

/// Parse a UDP header from raw bytes (standard format).
///
/// Format (big-endian / network byte order):
/// ```text
/// Bits:  0-3  | 4-7 | 8-11 | 12-15 | 16-31
///        Vers.|  ET | Header Length | Message Length
/// ```
///
/// Followed by:
/// - 4 bytes: Observation-Domain-ID
/// - 2 or 4 bytes: Message-ID
/// - (optional) 4 bytes: Option (Type, Length, SegmentNumber+L)
#[inline]
pub fn parse_udp_header(data: &[u8]) -> Result<UDPHeader> {
    parse_udp_header_impl(data, false)
}

/// Parse a UDP header from raw bytes (mobile/standard format with S-bit).
///
/// Same format as standard but with S-bit in the first byte:
/// ```text
/// Bits:  0-2  | 3 | 4-7 | 8-11 | 12-15 | 16-31
///        Vers.| S |  ET  | Header Length | Message Length
/// ```
#[inline]
pub fn parse_udp_header_mobile(data: &[u8]) -> Result<UDPHeaderMobile> {
    parse_udp_header_mobile_impl(data)
}

/// Internal implementation for standard UDP header parsing.
fn parse_udp_header_impl(data: &[u8], _mobile: bool) -> Result<UDPHeader> {
    if data.len() < UDP_HEADER_MIN_SIZE {
        return Err(AppError::UdpParse(format!(
            "Data length {} is less than minimum header size {}",
            data.len(),
            UDP_HEADER_MIN_SIZE
        )));
    }

    // First 2 bytes: Version(4 bits) + ET(4 bits) + HeaderLength(8 bits)
    let first_word = u16::from_be_bytes([data[0], data[1]]);

    let version = (first_word & 0xF000) >> 12;
    let et = first_word & 0x000F;
    let header_length = (first_word & 0x0FF0) >> 4;

    // Message length
    let message_length = u16::from_be_bytes([data[2], data[3]]);

    // Domain ID (4 bytes)
    let message_generator_id = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);

    // Message ID (4 bytes from offset 8)
    let msg_id_bytes = &data[8..12];
    let message_id = u32::from_be_bytes([
        msg_id_bytes[0],
        msg_id_bytes[1],
        msg_id_bytes[2],
        msg_id_bytes[3],
    ]);

    // Validate header length
    if header_length < UDP_HEADER_MIN_SIZE as u16 {
        return Err(AppError::UdpParse(format!(
            "Header length {} is less than minimum {}",
            header_length, UDP_HEADER_MIN_SIZE
        )));
    }

    if data.len() < header_length as usize {
        return Err(AppError::UdpParse(format!(
            "Data length {} is less than header length {}",
            data.len(),
            header_length
        )));
    }

    // Parse option if present (header_length > 12)
    let option = if header_length > UDP_HEADER_MIN_SIZE as u16 {
        if data.len() < UDP_HEADER_WITH_OPTION_SIZE {
            return Err(AppError::UdpParse(format!(
                "Data length {} is less than header with option size {}",
                data.len(),
                UDP_HEADER_WITH_OPTION_SIZE
            )));
        }
        Some(parse_option_field(&data[12..16])?)
    } else {
        None
    };

    Ok(UDPHeader {
        version,
        header_length,
        et,
        message_length,
        message_generator_id,
        message_id,
        option,
    })
}

/// Internal implementation for mobile UDP header parsing.
fn parse_udp_header_mobile_impl(data: &[u8]) -> Result<UDPHeaderMobile> {
    if data.len() < UDP_HEADER_MIN_SIZE {
        return Err(AppError::UdpParse(format!(
            "Data length {} is less than minimum header size {}",
            data.len(),
            UDP_HEADER_MIN_SIZE
        )));
    }

    // First 2 bytes: Version(3 bits) + S(1 bit) + ET(4 bits) + HeaderLength(8 bits)
    let first_word = u16::from_be_bytes([data[0], data[1]]);

    let version = (first_word & 0xE000) >> 13;
    let standard = (first_word & 0x1000) >> 12;
    let et = first_word & 0x000F;
    let header_length = first_word & 0x00FF;

    // Message length
    let message_length = u16::from_be_bytes([data[2], data[3]]);

    // Domain ID (4 bytes)
    let message_domain_id = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);

    // Message ID (4 bytes from offset 8)
    let msg_id_bytes = &data[8..12];
    let message_id = u32::from_be_bytes([
        msg_id_bytes[0],
        msg_id_bytes[1],
        msg_id_bytes[2],
        msg_id_bytes[3],
    ]);

    // Validate header length
    if header_length < UDP_HEADER_MIN_SIZE as u16 {
        return Err(AppError::UdpParse(format!(
            "Header length {} is less than minimum {}",
            header_length, UDP_HEADER_MIN_SIZE
        )));
    }

    if data.len() < header_length as usize {
        return Err(AppError::UdpParse(format!(
            "Data length {} is less than header length {}",
            data.len(),
            header_length
        )));
    }

    // Parse option if present
    let option = if header_length > UDP_HEADER_MIN_SIZE as u16 {
        if data.len() < UDP_HEADER_WITH_OPTION_SIZE {
            return Err(AppError::UdpParse(format!(
                "Data length {} is less than header with option size {}",
                data.len(),
                UDP_HEADER_WITH_OPTION_SIZE
            )));
        }
        Some(parse_option_field(&data[12..16])?)
    } else {
        None
    };

    Ok(UDPHeaderMobile {
        version,
        standard,
        header_length,
        et,
        message_length,
        message_domain_id,
        message_id,
        option,
    })
}

/// Parse the 4-byte option field.
///
/// Format:
/// - Byte 0: Type
/// - Byte 1: Length
/// - Bytes 2-3: Segment Number (15 bits) + Last (1 bit)
#[inline]
fn parse_option_field(data: &[u8]) -> Result<UdpHeaderOption> {
    debug_assert!(data.len() >= 4, "Option field requires 4 bytes");

    let option_type = data[0];
    let length = data[1];
    let segment_number_l = u16::from_be_bytes([data[2], data[3]]);
    let segment_number = (segment_number_l & 0xFFFE) >> 1;
    let last = (segment_number_l & 0x0001) != 0;

    Ok(UdpHeaderOption {
        option_type,
        length,
        segment_number,
        last,
    })
}

/// Extract the payload from a UDP datagram given the parsed header length.
///
/// Returns a zero-copy slice of the payload data.
#[inline]
pub fn extract_payload<'a>(data: &'a [u8], header_length: u16) -> &'a [u8] {
    let start = header_length as usize;
    if start < data.len() {
        &data[start..]
    } else {
        &[]
    }
}

/// Extract just the header bytes for hex display.
#[inline]
pub fn header_bytes<'a>(data: &'a [u8], header_length: u16) -> &'a [u8] {
    let end = std::cmp::min(header_length as usize, data.len());
    &data[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_standard_header_minimal() {
        // Construct a 12-byte header: Version=1, ET=0, HeaderLen=12
        let data = [
            0x10, 0x0C, // Version=1(4bits), ET=0(4bits), HeaderLen=12
            0x00, 0x64, // MessageLength=100
            0x00, 0x00, 0x00, 0x01, // DomainID=1
            0x00, 0x00, 0x00, 0x02, // MessageID=2
        ];
        let header = parse_udp_header(&data).unwrap();
        assert_eq!(header.version, 1);
        assert_eq!(header.et, 0);
        assert_eq!(header.header_length, 12);
        assert_eq!(header.message_length, 100);
        assert_eq!(header.message_generator_id, 1);
        assert_eq!(header.message_id, 2);
        assert!(header.option.is_none());
    }

    #[test]
    fn test_parse_mobile_header_minimal() {
        let data = [
            0x10, 0x0C, // Version=1(3bits), S=0, ET=0(4bits), HeaderLen=12
            0x00, 0x64, // MessageLength=100
            0x00, 0x00, 0x00, 0x01, // DomainID=1
            0x00, 0x00, 0x00, 0x02, // MessageID=2
        ];
        let header = parse_udp_header_mobile(&data).unwrap();
        assert_eq!(header.version, 1);
        assert_eq!(header.standard, 0);
        assert_eq!(header.header_length, 12);
        assert_eq!(header.message_length, 100);
        assert!(header.option.is_none());
    }

    #[test]
    fn test_parse_header_too_short() {
        let data = [0u8; 8];
        assert!(parse_udp_header(&data).is_err());
        assert!(parse_udp_header_mobile(&data).is_err());
    }

    #[test]
    fn test_parse_option_field() {
        let data = [
            0x01, // Type=1
            0x04, // Length=4
            0x00, 0x03, // SegmentNumber=1, Last=1
        ];
        // pad to 4 bytes
        let padded = [data[0], data[1], data[2], data[3]];
        let option = parse_option_field(&padded).unwrap();
        assert_eq!(option.option_type, 1);
        assert_eq!(option.length, 4);
        assert_eq!(option.segment_number, 1);
        assert!(option.last);
    }

    #[test]
    fn test_extract_payload() {
        let data = [0u8; 20];
        let payload = extract_payload(&data, 12);
        assert_eq!(payload.len(), 8);
    }
}
