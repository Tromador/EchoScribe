//! Binary Songbird playout-decision journal.
//!
//! All integers use little-endian byte order. The file begins with a fixed
//! header, followed by fixed-size records with a four-byte body length.
//! An incomplete final length or body is a recoverable truncated tail.

use std::io::{self, ErrorKind, Read, Write};

const MAGIC: &[u8; 8] = b"ECHOPLY\0";
pub(crate) const FORMAT_VERSION: u16 = 1;
const FILE_HEADER_LENGTH: u16 = 12;
const RECORD_BODY_LENGTH: usize = 23;
const DECISION_LOSS: u8 = 0;
const DECISION_PACKET: u8 = 1;

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct PlayoutRecord {
    pub(crate) tick: u64,
    pub(crate) ssrc: u32,
    pub(crate) decision: PlayoutDecision,
    pub(crate) decoded_samples: u32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum PlayoutDecision {
    Loss,
    Packet { sequence: u16, timestamp: u32 },
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
    let (decision, sequence, timestamp) = match record.decision {
        PlayoutDecision::Loss => (DECISION_LOSS, 0, 0),
        PlayoutDecision::Packet {
            sequence,
            timestamp,
        } => (DECISION_PACKET, sequence, timestamp),
    };

    writer.write_all(&(RECORD_BODY_LENGTH as u32).to_le_bytes())?;
    writer.write_all(&record.tick.to_le_bytes())?;
    writer.write_all(&record.ssrc.to_le_bytes())?;
    writer.write_all(&[decision])?;
    writer.write_all(&sequence.to_le_bytes())?;
    writer.write_all(&timestamp.to_le_bytes())?;
    writer.write_all(&record.decoded_samples.to_le_bytes())
}

pub(crate) fn read_file_header(reader: &mut impl Read) -> io::Result<()> {
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

    let version = u16::from_le_bytes([header[8], header[9]]);
    if version != FORMAT_VERSION {
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

    Ok(())
}

pub(crate) fn read_record(reader: &mut impl Read) -> io::Result<ReadRecord> {
    let mut length_bytes = [0_u8; 4];

    match read_up_to(reader, &mut length_bytes)? {
        0 => return Ok(ReadRecord::EndOfFile),
        4 => {}
        _ => return Ok(ReadRecord::TruncatedTail),
    }

    let body_length = u32::from_le_bytes(length_bytes) as usize;
    if body_length != RECORD_BODY_LENGTH {
        return Err(invalid_data(format!(
            "invalid playout journal record length {body_length}"
        )));
    }

    let mut body = [0_u8; RECORD_BODY_LENGTH];
    if read_up_to(reader, &mut body)? != body.len() {
        return Ok(ReadRecord::TruncatedTail);
    }

    let sequence = u16::from_le_bytes(body[13..15].try_into().expect("fixed sequence slice"));
    let timestamp = u32::from_le_bytes(body[15..19].try_into().expect("fixed RTP timestamp slice"));
    let decision = match body[12] {
        DECISION_LOSS if sequence == 0 && timestamp == 0 => PlayoutDecision::Loss,
        DECISION_LOSS => {
            return Err(invalid_data(
                "loss record contains a packet sequence or timestamp",
            ));
        }
        DECISION_PACKET => PlayoutDecision::Packet {
            sequence,
            timestamp,
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
        read_file_header(&mut reader).unwrap();
        assert_eq!(
            read_record(&mut reader).unwrap(),
            ReadRecord::Record(packet)
        );
        assert_eq!(read_record(&mut reader).unwrap(), ReadRecord::Record(loss));
        assert_eq!(read_record(&mut reader).unwrap(), ReadRecord::EndOfFile);
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
        read_file_header(&mut reader).unwrap();
        assert_eq!(read_record(&mut reader).unwrap(), ReadRecord::TruncatedTail);
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

        let error = read_record(&mut bytes.as_slice()).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidData);
        assert!(error.to_string().contains("loss record contains"));
    }
}
