//! Binary packet-journal format.
//!
//! All integers use little-endian byte order. The file begins with a fixed
//! header, followed by records consisting of a four-byte body length and the
//! body itself. An incomplete final length or body is treated as a recoverable
//! truncated tail.

use std::io::{self, ErrorKind, Read, Write};

const MAGIC: &[u8; 8] = b"ECHOPKT\0";
pub(crate) const FORMAT_VERSION: u16 = 1;
const FILE_HEADER_LENGTH: u16 = 12;
const RECORD_METADATA_LENGTH: usize = 30;
const MAX_RECORD_BODY_LENGTH: usize = 65_536;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct PacketRecord {
    pub(crate) arrival_nanos_since_session_start: u64,
    pub(crate) ssrc: u32,
    pub(crate) sequence: u16,
    pub(crate) timestamp: u32,
    pub(crate) payload_start: u32,
    pub(crate) payload_end: u32,
    pub(crate) packet: Vec<u8>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ReadRecord {
    Record(PacketRecord),
    EndOfFile,
    TruncatedTail,
}

pub(crate) fn write_file_header(writer: &mut impl Write) -> io::Result<()> {
    writer.write_all(MAGIC)?;
    writer.write_all(&FORMAT_VERSION.to_le_bytes())?;
    writer.write_all(&FILE_HEADER_LENGTH.to_le_bytes())
}

pub(crate) fn read_file_header(reader: &mut impl Read) -> io::Result<()> {
    let mut header = [0_u8; FILE_HEADER_LENGTH as usize];

    if read_up_to(reader, &mut header)? != header.len() {
        return Err(io::Error::new(
            ErrorKind::UnexpectedEof,
            "packet journal has an incomplete file header",
        ));
    }

    if &header[..MAGIC.len()] != MAGIC {
        return Err(invalid_data("packet journal has an invalid magic value"));
    }

    let version = u16::from_le_bytes([header[8], header[9]]);
    if version != FORMAT_VERSION {
        return Err(invalid_data(format!(
            "unsupported packet journal version {version}"
        )));
    }

    let header_length = u16::from_le_bytes([header[10], header[11]]);
    if header_length != FILE_HEADER_LENGTH {
        return Err(invalid_data(format!(
            "unsupported packet journal header length {header_length}"
        )));
    }

    Ok(())
}

pub(crate) fn write_record(writer: &mut impl Write, record: &PacketRecord) -> io::Result<()> {
    validate_payload_bounds(record)?;

    let packet_length = u32::try_from(record.packet.len())
        .map_err(|_| invalid_input("RTP packet is too large to journal"))?;
    let body_length = RECORD_METADATA_LENGTH
        .checked_add(record.packet.len())
        .ok_or_else(|| invalid_input("packet journal record length overflow"))?;

    if body_length > MAX_RECORD_BODY_LENGTH {
        return Err(invalid_input("packet journal record is too large"));
    }

    let body_length = u32::try_from(body_length)
        .map_err(|_| invalid_input("packet journal record is too large"))?;

    writer.write_all(&body_length.to_le_bytes())?;
    writer.write_all(&record.arrival_nanos_since_session_start.to_le_bytes())?;
    writer.write_all(&record.ssrc.to_le_bytes())?;
    writer.write_all(&record.sequence.to_le_bytes())?;
    writer.write_all(&record.timestamp.to_le_bytes())?;
    writer.write_all(&record.payload_start.to_le_bytes())?;
    writer.write_all(&record.payload_end.to_le_bytes())?;
    writer.write_all(&packet_length.to_le_bytes())?;
    writer.write_all(&record.packet)
}

pub(crate) fn read_record(reader: &mut impl Read) -> io::Result<ReadRecord> {
    let mut length_bytes = [0_u8; 4];

    match read_up_to(reader, &mut length_bytes)? {
        0 => return Ok(ReadRecord::EndOfFile),
        4 => {}
        _ => return Ok(ReadRecord::TruncatedTail),
    }

    let body_length = u32::from_le_bytes(length_bytes) as usize;
    if !(RECORD_METADATA_LENGTH..=MAX_RECORD_BODY_LENGTH).contains(&body_length) {
        return Err(invalid_data(format!(
            "invalid packet journal record length {body_length}"
        )));
    }

    let mut body = vec![0_u8; body_length];
    if read_up_to(reader, &mut body)? != body_length {
        return Ok(ReadRecord::TruncatedTail);
    }

    let packet_length =
        u32::from_le_bytes(body[26..30].try_into().expect("fixed metadata slice")) as usize;
    let expected_body_length = RECORD_METADATA_LENGTH
        .checked_add(packet_length)
        .ok_or_else(|| invalid_data("packet journal record length overflow"))?;

    if expected_body_length != body_length {
        return Err(invalid_data(format!(
            "record body length {body_length} does not match packet length {packet_length}"
        )));
    }

    let record = PacketRecord {
        arrival_nanos_since_session_start: u64::from_le_bytes(
            body[0..8].try_into().expect("fixed metadata slice"),
        ),
        ssrc: u32::from_le_bytes(body[8..12].try_into().expect("fixed metadata slice")),
        sequence: u16::from_le_bytes(body[12..14].try_into().expect("fixed metadata slice")),
        timestamp: u32::from_le_bytes(body[14..18].try_into().expect("fixed metadata slice")),
        payload_start: u32::from_le_bytes(body[18..22].try_into().expect("fixed metadata slice")),
        payload_end: u32::from_le_bytes(body[22..26].try_into().expect("fixed metadata slice")),
        packet: body[RECORD_METADATA_LENGTH..].to_vec(),
    };

    validate_payload_bounds(&record).map_err(|error| invalid_data(error.to_string()))?;

    Ok(ReadRecord::Record(record))
}

fn validate_payload_bounds(record: &PacketRecord) -> io::Result<()> {
    let packet_length = u32::try_from(record.packet.len())
        .map_err(|_| invalid_input("RTP packet is too large to journal"))?;

    if record.payload_start > record.payload_end || record.payload_end > packet_length {
        return Err(invalid_input(format!(
            "invalid payload bounds {}..{} for a {}-byte packet",
            record.payload_start, record.payload_end, packet_length
        )));
    }

    Ok(())
}

fn read_up_to(reader: &mut impl Read, buffer: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;

    while filled < buffer.len() {
        match reader.read(&mut buffer[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }

    Ok(filled)
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(ErrorKind::InvalidData, message.into())
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    fn packet_record(sequence: u16) -> PacketRecord {
        PacketRecord {
            arrival_nanos_since_session_start: u64::from(sequence) * 20_000_000,
            ssrc: 4326,
            sequence,
            timestamp: u32::from(sequence) * 960,
            payload_start: 12,
            payload_end: 16,
            packet: vec![
                0x80, 0x78, 0, 0, 0, 0, 0, 0, 0, 0, 0x10, 0xE6, 0x01, 2, 3, 4,
            ],
        }
    }

    #[test]
    fn file_and_records_round_trip() {
        let first = packet_record(10);
        let second = packet_record(11);
        let mut bytes = Vec::new();

        write_file_header(&mut bytes).unwrap();
        write_record(&mut bytes, &first).unwrap();
        write_record(&mut bytes, &second).unwrap();

        let mut reader = Cursor::new(bytes);
        read_file_header(&mut reader).unwrap();
        assert_eq!(read_record(&mut reader).unwrap(), ReadRecord::Record(first));
        assert_eq!(
            read_record(&mut reader).unwrap(),
            ReadRecord::Record(second)
        );
        assert_eq!(read_record(&mut reader).unwrap(), ReadRecord::EndOfFile);
    }

    #[test]
    fn incomplete_final_record_is_a_recoverable_tail() {
        let first = packet_record(10);
        let second = packet_record(11);
        let mut bytes = Vec::new();

        write_file_header(&mut bytes).unwrap();
        write_record(&mut bytes, &first).unwrap();
        write_record(&mut bytes, &second).unwrap();
        bytes.truncate(bytes.len() - 3);

        let mut reader = Cursor::new(bytes);
        read_file_header(&mut reader).unwrap();
        assert_eq!(read_record(&mut reader).unwrap(), ReadRecord::Record(first));
        assert_eq!(read_record(&mut reader).unwrap(), ReadRecord::TruncatedTail);
    }

    #[test]
    fn invalid_payload_bounds_are_rejected() {
        let mut record = packet_record(10);
        record.payload_end = record.packet.len() as u32 + 1;

        let error = write_record(&mut Vec::new(), &record).unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert!(error.to_string().contains("invalid payload bounds"));
    }
}
