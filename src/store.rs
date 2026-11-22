use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Deserialize, Serialize)]
pub struct Event {
    pub schema_version: u32,
    pub id: Uuid,
    pub tool: String,
    pub kind: String,
    pub device_name: String,
    pub occurred_at_ms: i64,
    pub project: Option<String>,
    pub session_id: Option<String>,
    pub turn_id: Option<String>,
    pub message: Option<String>,
    #[serde(default)]
    pub message_truncated: bool,
}

impl Event {
    pub fn valid(&self) -> bool {
        self.schema_version == 1
            && !self.id.is_nil()
            && self.occurred_at_ms >= 0
            && matches!(
                (self.tool.as_str(), self.kind.as_str()),
                ("codex" | "claude-code", "turn.completed") | ("manual", "test")
            )
            && !self.device_name.trim().is_empty()
            && self.device_name.len() <= 256
            && [&self.project, &self.session_id, &self.turn_id]
                .into_iter()
                .flatten()
                .all(|value| value.len() <= 512)
            && self
                .message
                .as_ref()
                .is_none_or(|body| body.len() <= 128 * 1024)
    }
}

pub struct StoredEvent {
    pub seq: i64,
    pub device_id: String,
    pub received_at_ms: i64,
    pub event: Event,
}

#[derive(Serialize)]
pub struct EventSummary {
    pub seq: i64,
    pub id: Uuid,
    pub device_id: String,
    pub device_name: String,
    pub tool: String,
    pub kind: String,
    pub project: Option<String>,
    pub occurred_at_ms: i64,
    pub received_at_ms: i64,
    pub session_id: Option<String>,
    pub preview: Option<String>,
    pub message_truncated: bool,
}

impl StoredEvent {
    pub fn summary(&self) -> EventSummary {
        EventSummary {
            seq: self.seq,
            id: self.event.id,
            device_id: self.device_id.clone(),
            device_name: self.event.device_name.clone(),
            tool: self.event.tool.clone(),
            kind: self.event.kind.clone(),
            project: self.event.project.clone(),
            occurred_at_ms: self.event.occurred_at_ms,
            received_at_ms: self.received_at_ms,
            session_id: self.event.session_id.clone(),
            preview: self.event.message.as_ref().map(|body| {
                let mut chars = body.chars();
                let mut preview: String = chars.by_ref().take(240).collect();
                if chars.next().is_some() {
                    preview.push('…');
                }
                preview
            }),
            message_truncated: self.event.message_truncated,
        }
    }
}

pub struct Store(Connection);

pub enum Insert {
    New(i64),
    Existing(i64),
    Conflict,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let db = Connection::open(path)?;
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE,
                device_id TEXT NOT NULL,
                received_at_ms INTEGER NOT NULL,
                event_json TEXT NOT NULL
            );",
        )?;
        Ok(Self(db))
    }

    pub fn insert(&self, device_id: &str, event: &Event) -> rusqlite::Result<Insert> {
        let json = serde_json::to_string(event)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
        let id = event.id.to_string();
        let received = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let changed = self.0.execute(
            "INSERT INTO events(id, device_id, received_at_ms, event_json)
             VALUES (?1, ?2, ?3, ?4) ON CONFLICT(id) DO NOTHING",
            params![id, device_id, received as i64, json],
        )?;
        if changed == 1 {
            return Ok(Insert::New(self.0.last_insert_rowid()));
        }
        let (seq, identical) = self.0.query_row(
            "SELECT seq, (device_id = ?2 AND event_json = ?3) FROM events WHERE id = ?1",
            params![id, device_id, json],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok(if identical {
            Insert::Existing(seq)
        } else {
            Insert::Conflict
        })
    }

    pub fn events(&self, after: i64, limit: i64) -> rusqlite::Result<Vec<StoredEvent>> {
        self.0
            .prepare(
                "SELECT seq, device_id, received_at_ms, event_json
             FROM events WHERE seq > ?1 ORDER BY seq LIMIT ?2",
            )?
            .query_map(params![after, limit], read_event)?
            .collect()
    }

    pub fn event(&self, id: Uuid) -> rusqlite::Result<Option<StoredEvent>> {
        self.0
            .query_row(
                "SELECT seq, device_id, received_at_ms, event_json FROM events WHERE id = ?1",
                [id.to_string()],
                read_event,
            )
            .optional()
    }
}

fn read_event(row: &Row<'_>) -> rusqlite::Result<StoredEvent> {
    let json: String = row.get(3)?;
    let event = serde_json::from_str(&json).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(err))
    })?;
    Ok(StoredEvent {
        seq: row.get(0)?,
        device_id: row.get(1)?,
        received_at_ms: row.get(2)?,
        event,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_full_message_and_deduplicates() {
        let db = Store::open(":memory:").unwrap();
        let message = "研发\n".repeat(110);
        let event = Event {
            schema_version: 1,
            id: "ab97b1cf-23ea-4792-ab5a-aefa519609e3".parse().unwrap(),
            tool: "codex".into(),
            kind: "turn.completed".into(),
            device_name: "Windows".into(),
            occurred_at_ms: 42,
            project: None,
            session_id: None,
            turn_id: None,
            message: Some(message.clone()),
            message_truncated: false,
        };
        assert!(event.valid());
        assert!(matches!(db.insert("win", &event).unwrap(), Insert::New(1)));
        assert!(matches!(
            db.insert("win", &event).unwrap(),
            Insert::Existing(1)
        ));
        assert!(matches!(
            db.insert("other", &event).unwrap(),
            Insert::Conflict
        ));
        assert_eq!(db.events(0, 25).unwrap().len(), 1);
        assert!(db.events(1, 25).unwrap().is_empty());
        let result = db.event(event.id).unwrap().unwrap();
        assert_eq!(result.event.message, Some(message));
        assert_eq!(result.summary().preview.unwrap().chars().count(), 241);
    }
}
