use std::{
    fs::OpenOptions,
    io::{self, BufWriter, Write},
    path::Path,
};

use serde::Serialize;

pub(crate) const SESSION_FORMAT_VERSION: u16 = 2;
pub(crate) const EVENT_FORMAT_VERSION: u16 = 1;

#[derive(Serialize)]
struct SessionFile<'a> {
    format: u16,
    session_id: &'a str,
    started_at_unix_millis: u64,
    discord: DiscordSession<'a>,
    files: SessionFiles,
}

#[derive(Serialize)]
struct DiscordSession<'a> {
    guild_id: &'a str,
    channel_id: &'a str,
}

#[derive(Serialize)]
struct SessionFiles {
    packets: FileDescription,
    playout: FileDescription,
    events: FileDescription,
}

#[derive(Serialize)]
struct FileDescription {
    path: &'static str,
    format: u16,
}

#[derive(Debug, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub(crate) enum SessionEvent {
    SpeakerMapping {
        format: u16,
        elapsed_nanos: u64,
        ssrc: u32,
        user_id: Option<String>,
        speaking_bits: u8,
    },
}

impl SessionEvent {
    pub(crate) fn speaker_mapping(
        elapsed_nanos: u64,
        ssrc: u32,
        user_id: Option<String>,
        speaking_bits: u8,
    ) -> Self {
        Self::SpeakerMapping {
            format: EVENT_FORMAT_VERSION,
            elapsed_nanos,
            ssrc,
            user_id,
            speaking_bits,
        }
    }
}

pub(crate) fn write_session_file(
    path: &Path,
    session_id: &str,
    started_at_unix_millis: u64,
    guild_id: &str,
    channel_id: &str,
) -> io::Result<()> {
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut writer = BufWriter::new(file);
    let session = SessionFile {
        format: SESSION_FORMAT_VERSION,
        session_id,
        started_at_unix_millis,
        discord: DiscordSession {
            guild_id,
            channel_id,
        },
        files: SessionFiles {
            packets: FileDescription {
                path: "packets.dat",
                format: crate::journal::FORMAT_VERSION,
            },
            playout: FileDescription {
                path: "playout.dat",
                format: crate::playout::FORMAT_VERSION,
            },
            events: FileDescription {
                path: "events.ndjson",
                format: EVENT_FORMAT_VERSION,
            },
        },
    };

    serde_json::to_writer_pretty(&mut writer, &session).map_err(io::Error::other)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    writer.get_ref().sync_data()
}

pub(crate) fn write_event(writer: &mut impl Write, event: &SessionEvent) -> io::Result<()> {
    serde_json::to_writer(&mut *writer, event).map_err(io::Error::other)?;
    writer.write_all(b"\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn speaker_mapping_is_one_json_line() {
        let event =
            SessionEvent::speaker_mapping(123_456, 4326, Some("881203221593464864".into()), 1);
        let mut bytes = Vec::new();

        write_event(&mut bytes, &event).unwrap();

        let text = String::from_utf8(bytes).unwrap();
        assert_eq!(text.lines().count(), 1);
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["event"], "speaker_mapping");
        assert_eq!(value["format"], 1);
        assert_eq!(value["elapsed_nanos"], 123_456);
        assert_eq!(value["ssrc"], 4326);
        assert_eq!(value["user_id"], "881203221593464864");
        assert_eq!(value["speaking_bits"], 1);
    }
}
