//! Binary Songbird playout-decision journal.
//!
//! All integers use little-endian byte order. The file begins with a fixed
//! header, followed by fixed-size records with a four-byte body length.
//! An incomplete final length or body is a recoverable truncated tail.

use std::io::{self, ErrorKind, Read, Write};

const MAGIC: &[u8; 8] = b"ECHOPLY\0";
const LEGACY_FORMAT_VERSION: u16 = 1;
pub(crate) const FORMAT_VERSION: u16 = 2;
const FILE_HEADER_LENGTH: u16 = 12;
const RECORD_BODY_LENGTH_V1: usize = 23;
const RECORD_BODY_LENGTH_V2: usize = 31;
const DECISION_LOSS: u8 = 0;
const DECISION_PACKET: u8 = 1;

#[derive(Debug, Clone, Eq, PartialEq)]
/// Songbird's decision for one SSRC at one global voice tick.
pub(crate) struct PlayoutRecord {
    pub(crate) tick: u64,
    pub(crate) ssrc: u32,
    pub(crate) decision: PlayoutDecision,
    pub(crate) decoded_samples: u32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
/// Either decoded packet selection or explicit packet-loss concealment.
pub(crate) enum PlayoutDecision {
    Loss,
    Packet {
        sequence: u16,
        timestamp: u32,
        opus_payload: Option<OpusPayloadBounds>,
    },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
/// Location of the Opus payload inside its authoritative packet record.
pub(crate) struct OpusPayloadBounds {
    pub(crate) start: u32,
    pub(crate) end: u32,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum ReadRecord {
    Record(PlayoutRecord),
    EndOfFile,
    TruncatedTail,
}

pub(crate) fn write_file_header(writer: &mut impl Write) -> io::Result<()> {
    writer.write_all(MAGIC)?;
    writer.write_all(&FORMAT_VERSION.to_le_bytes())?;
    writer.write_all(&FILE_HEADER_LENGTH.to_le_bytes())
}

pub(crate) fn write_record(writer: &mut impl Write, record: &PlayoutRecord) -> io::Result<()> {
    let (decision, sequence, timestamp, payload_start, payload_end) = match record.decision {
        PlayoutDecision::Loss => (DECISION_LOSS, 0, 0, 0, 0),
        PlayoutDecision::Packet {
            sequence,
            timestamp,
            opus_payload: Some(bounds),
        } if bounds.start <= bounds.end => (
            DECISION_PACKET,
            sequence,
            timestamp,
            bounds.start,
            bounds.end,
        ),
        PlayoutDecision::Packet {
            opus_payload: Some(bounds),
            ..
        } => {
            return Err(invalid_input(format!(
                "invalid Opus payload bounds {}..{}",
                bounds.start, bounds.end
            )));
        }
        PlayoutDecision::Packet {
            sequence,
            timestamp,
            opus_payload: None,
        } => (DECISION_PACKET, sequence, timestamp, 0, 0),
    };

    writer.write_all(&(RECORD_BODY_LENGTH_V2 as u32).to_le_bytes())?;
    writer.write_all(&record.tick.to_le_bytes())?;
    writer.write_all(&record.ssrc.to_le_bytes())?;
    writer.write_all(&[decision])?;
    writer.write_all(&sequence.to_le_bytes())?;
    writer.write_all(&timestamp.to_le_bytes())?;
    writer.write_all(&record.decoded_samples.to_le_bytes())?;
    writer.write_all(&payload_start.to_le_bytes())?;
    writer.write_all(&payload_end.to_le_bytes())
}

pub(crate) fn read_file_header(reader: &mut impl Read) -> io::Result<u16> {
    let mut header = [0_u8; FILE_HEADER_LENGTH as usize];

    if read_up_to(reader, &mut header)? != header.len() {
        return Err(io::Error::new(
            ErrorKind::UnexpectedEof,
            "playout journal has an incomplete file header",
        ));
    }

    if &header[..MAGIC.len()] != MAGIC {
        return Err(invalid_data("playout journal has an invalid magic value"));
    }

    // Format 2 adds decoded-sample count and Opus bounds. Format 1 remains
    // readable for sessions captured before that evidence existed.
    let version = u16::from_le_bytes([header[8], header[9]]);
    if !matches!(version, LEGACY_FORMAT_VERSION | FORMAT_VERSION) {
        return Err(invalid_data(format!(
            "unsupported playout journal version {version}"
        )));
    }

    let header_length = u16::from_le_bytes([header[10], header[11]]);
    if header_length != FILE_HEADER_LENGTH {
        return Err(invalid_data(format!(
            "unsupported playout journal header length {header_length}"
        )));
    }

    Ok(version)
}

pub(crate) fn read_record(reader: &mut impl Read, format_version: u16) -> io::Result<ReadRecord> {
    let mut length_bytes = [0_u8; 4];

    // Incomplete final data is a recoverable crash tail; malformed complete
    // records remain hard errors.
    match read_up_to(reader, &mut length_bytes)? {
        0 => return Ok(ReadRecord::EndOfFile),
        4 => {}
        _ => return Ok(ReadRecord::TruncatedTail),
    }

    let expected_body_length = match format_version {
        LEGACY_FORMAT_VERSION => RECORD_BODY_LENGTH_V1,
        FORMAT_VERSION => RECORD_BODY_LENGTH_V2,
        version => {
            return Err(invalid_data(format!(
                "unsupported playout journal version {version}"
            )));
        }
    };
    let body_length = u32::from_le_bytes(length_bytes) as usize;
    if body_length != expected_body_length {
        return Err(invalid_data(format!(
            "invalid format {format_version} playout journal record length {body_length}"
        )));
    }

    let mut body = vec![0_u8; body_length];
    if read_up_to(reader, &mut body)? != body_length {
        return Ok(ReadRecord::TruncatedTail);
    }

    let sequence = u16::from_le_bytes(body[13..15].try_into().expect("fixed sequence slice"));
    let timestamp = u32::from_le_bytes(body[15..19].try_into().expect("fixed RTP timestamp slice"));
    let opus_payload = if format_version == FORMAT_VERSION {
        let start = u32::from_le_bytes(body[23..27].try_into().expect("fixed payload-start slice"));
        let end = u32::from_le_bytes(body[27..31].try_into().expect("fixed payload-end slice"));
        if start == 0 && end == 0 {
            None
        } else {
            Some(OpusPayloadBounds { start, end })
        }
    } else {
        None
    };
    let decision = match body[12] {
        DECISION_LOSS
            if sequence == 0
                && timestamp == 0
                && opus_payload.is_none_or(|bounds| bounds.start == 0 && bounds.end == 0) =>
        {
            PlayoutDecision::Loss
        }
        DECISION_LOSS => {
            return Err(invalid_data("loss record contains packet metadata"));
        }
        DECISION_PACKET if opus_payload.is_some_and(|bounds| bounds.start > bounds.end) => {
            return Err(invalid_data(
                "packet record has invalid Opus payload bounds",
            ));
        }
        DECISION_PACKET => PlayoutDecision::Packet {
            sequence,
            timestamp,
            opus_payload,
        },
        value => {
            return Err(invalid_data(format!(
                "invalid playout decision value {value}"
            )));
        }
    };

    Ok(ReadRecord::Record(PlayoutRecord {
        tick: u64::from_le_bytes(body[0..8].try_into().expect("fixed tick slice")),
        ssrc: u32::from_le_bytes(body[8..12].try_into().expect("fixed SSRC slice")),
        decision,
        decoded_samples: u32::from_le_bytes(
            body[19..23]
                .try_into()
                .expect("fixed decoded-sample-count slice"),
        ),
    }))
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

    #[test]
    fn packet_and_loss_records_round_trip() {
        let packet = PlayoutRecord {
            tick: 10,
            ssrc: 4326,
            decision: PlayoutDecision::Packet {
                sequence: 65_535,
                timestamp: 123_456,
                opus_payload: Some(OpusPayloadBounds {
                    start: 24,
                    end: 718,
                }),
            },
            decoded_samples: 960,
        };
        let loss = PlayoutRecord {
            tick: 11,
            ssrc: 4326,
            decision: PlayoutDecision::Loss,
            decoded_samples: 960,
        };
        let mut bytes = Vec::new();

        write_file_header(&mut bytes).unwrap();
        write_record(&mut bytes, &packet).unwrap();
        write_record(&mut bytes, &loss).unwrap();

        let mut reader = Cursor::new(bytes);
        let format_version = read_file_header(&mut reader).unwrap();
        assert_eq!(
            read_record(&mut reader, format_version).unwrap(),
            ReadRecord::Record(packet)
        );
        assert_eq!(
            read_record(&mut reader, format_version).unwrap(),
            ReadRecord::Record(loss)
        );
        assert_eq!(
            read_record(&mut reader, format_version).unwrap(),
            ReadRecord::EndOfFile
        );
    }

    #[test]
    fn incomplete_final_record_is_a_recoverable_tail() {
        let record = PlayoutRecord {
            tick: 10,
            ssrc: 4326,
            decision: PlayoutDecision::Loss,
            decoded_samples: 960,
        };
        let mut bytes = Vec::new();

        write_file_header(&mut bytes).unwrap();
        write_record(&mut bytes, &record).unwrap();
        bytes.truncate(bytes.len() - 3);

        let mut reader = Cursor::new(bytes);
        let format_version = read_file_header(&mut reader).unwrap();
        assert_eq!(
            read_record(&mut reader, format_version).unwrap(),
            ReadRecord::TruncatedTail
        );
    }

    #[test]
    fn malformed_loss_record_is_rejected() {
        let record = PlayoutRecord {
            tick: 10,
            ssrc: 4326,
            decision: PlayoutDecision::Loss,
            decoded_samples: 960,
        };
        let mut bytes = Vec::new();

        write_record(&mut bytes, &record).unwrap();
        bytes[4 + 13] = 1;

        let error = read_record(&mut bytes.as_slice(), FORMAT_VERSION).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(error.to_string().contains("loss record contains"));
    }

    #[test]
    fn format_one_packet_record_remains_readable() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(RECORD_BODY_LENGTH_V1 as u32).to_le_bytes());
        bytes.extend_from_slice(&10_u64.to_le_bytes());
        bytes.extend_from_slice(&4326_u32.to_le_bytes());
        bytes.push(DECISION_PACKET);
        bytes.extend_from_slice(&65_535_u16.to_le_bytes());
        bytes.extend_from_slice(&123_456_u32.to_le_bytes());
        bytes.extend_from_slice(&960_u32.to_le_bytes());

        assert_eq!(
            read_record(&mut bytes.as_slice(), LEGACY_FORMAT_VERSION).unwrap(),
            ReadRecord::Record(PlayoutRecord {
                tick: 10,
                ssrc: 4326,
                decision: PlayoutDecision::Packet {
                    sequence: 65_535,
                    timestamp: 123_456,
                    opus_payload: None,
                },
                decoded_samples: 960,
            })
        );
    }

    #[test]
    fn malformed_format_two_payload_bounds_are_rejected() {
        let record = PlayoutRecord {
            tick: 10,
            ssrc: 4326,
            decision: PlayoutDecision::Packet {
                sequence: 123,
                timestamp: 456,
                opus_payload: Some(OpusPayloadBounds {
                    start: 24,
                    end: 718,
                }),
            },
            decoded_samples: 960,
        };
        let mut bytes = Vec::new();

        write_record(&mut bytes, &record).unwrap();
        bytes[4 + 23..4 + 27].copy_from_slice(&719_u32.to_le_bytes());

        let error = read_record(&mut bytes.as_slice(), FORMAT_VERSION).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(error.to_string().contains("invalid Opus payload bounds"));
    }
}
