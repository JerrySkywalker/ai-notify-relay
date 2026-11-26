use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Deserialize, Serialize)]
pub struct Frontend {
    pub id: String,
    pub label: String,
}

fn slug(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"-_.".contains(&byte)
        })
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Event {
    pub schema_version: u32,
    pub id: Uuid,
    pub tool: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frontend: Option<Frontend>,
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
            && ((self.kind == "test" && self.tool == "manual")
                || (self.kind == "turn.completed" && self.tool != "manual" && slug(&self.tool)))
            && self.frontend.as_ref().is_none_or(|source| {
                slug(&source.id)
                    && !source.label.trim().is_empty()
                    && source.label.len() <= 128
                    && !source.label.chars().any(char::is_control)
            })
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

pub fn mask_token(token: &str) -> String {
    let len = token.len();
    if len <= 4 {
        "***".to_string()
    } else {
        let suffix = &token[len - 4..];
        format!("***{suffix}")
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PushDevice {
    #[serde(skip_serializing)]
    pub token: String,
    pub token_mask: String,
    pub device_id: String,
    pub device_type: String,
    pub app_id: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct PushDelivery {
    pub id: i64,
    pub event_id: String,
    #[serde(skip_serializing)]
    pub token: String,
    pub token_mask: String,
    pub channel: String,
    pub status: String,
    pub attempted_at_ms: i64,
    pub error: Option<String>,
    #[serde(default)]
    pub retry_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gateway_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingDelivery {
    pub id: i64,
    pub event_id: String,
    pub token: String,
    pub retry_count: i64,
}

#[derive(Serialize)]
pub struct EventSummary {
    pub seq: i64,
    pub id: Uuid,
    pub device_id: String,
    pub device_name: String,
    pub tool: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frontend: Option<Frontend>,
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
            frontend: self.event.frontend.clone(),
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

pub struct Store {
    pub(crate) db: Connection,
    stream_id: String,
}

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
            );
            CREATE TABLE IF NOT EXISTS relay_metadata (
                name TEXT PRIMARY KEY NOT NULL,
                value TEXT NOT NULL
            );
            INSERT INTO relay_metadata(name, value) VALUES ('stream_id', lower(hex(randomblob(16))))
                ON CONFLICT(name) DO NOTHING;
            CREATE TABLE IF NOT EXISTS push_devices (
                token TEXT PRIMARY KEY NOT NULL,
                device_id TEXT NOT NULL,
                device_type TEXT NOT NULL,
                app_id TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS push_deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL,
                token TEXT NOT NULL,
                channel TEXT NOT NULL DEFAULT 'local_simulation',
                status TEXT NOT NULL,
                attempted_at_ms INTEGER NOT NULL,
                error TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                gateway_id TEXT
            );",
        )?;
        let _ = db.execute("ALTER TABLE push_deliveries ADD COLUMN channel TEXT NOT NULL DEFAULT 'local_simulation'", []);
        let _ = db.execute(
            "ALTER TABLE push_deliveries ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0",
            [],
        );
        let _ = db.execute("ALTER TABLE push_deliveries ADD COLUMN gateway_id TEXT", []);
        crate::mailbox::migrate(&db)?;
        let stream_id = db.query_row(
            "SELECT value FROM relay_metadata WHERE name = 'stream_id'",
            [],
            |row| row.get(0),
        )?;
        Ok(Self { db, stream_id })
    }

    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    pub fn cursor_matches(&self, seq: i64, id: Uuid) -> rusqlite::Result<bool> {
        self.db.query_row(
            "SELECT EXISTS(SELECT 1 FROM events WHERE seq = ?1 AND id = ?2)",
            params![seq, id.to_string()],
            |row| row.get(0),
        )
    }

    pub fn insert(&self, device_id: &str, event: &Event) -> rusqlite::Result<Insert> {
        let json = serde_json::to_string(event)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
        let id = event.id.to_string();
        let received = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let changed = self.db.execute(
            "INSERT INTO events(id, device_id, received_at_ms, event_json)
             VALUES (?1, ?2, ?3, ?4) ON CONFLICT(id) DO NOTHING",
            params![id, device_id, received as i64, json],
        )?;
        if changed == 1 {
            let rowid = self.db.last_insert_rowid();
            let _ = self.record_push_deliveries(event.id);
            return Ok(Insert::New(rowid));
        }
        let (seq, identical) = self.db.query_row(
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
        self.db
            .prepare(
                "SELECT seq, device_id, received_at_ms, event_json
             FROM events WHERE seq > ?1 ORDER BY seq LIMIT ?2",
            )?
            .query_map(params![after, limit], read_event)?
            .collect()
    }

    pub fn event(&self, id: Uuid) -> rusqlite::Result<Option<StoredEvent>> {
        self.db
            .query_row(
                "SELECT seq, device_id, received_at_ms, event_json FROM events WHERE id = ?1",
                [id.to_string()],
                read_event,
            )
            .optional()
    }

    pub fn register_device(
        &self,
        device_id: &str,
        token: &str,
        device_type: &str,
        app_id: &str,
    ) -> rusqlite::Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        self.db.execute(
            "INSERT INTO push_devices(token, device_id, device_type, app_id, created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?5)
             ON CONFLICT(token) DO UPDATE SET
                device_id = excluded.device_id,
                device_type = excluded.device_type,
                app_id = excluded.app_id,
                updated_at_ms = excluded.updated_at_ms",
            params![token, device_id, device_type, app_id, now],
        )?;
        Ok(())
    }

    pub fn unregister_device(&self, token: &str) -> rusqlite::Result<bool> {
        let rows = self
            .db
            .execute("DELETE FROM push_devices WHERE token = ?1", params![token])?;
        Ok(rows > 0)
    }

    pub fn push_devices(&self) -> rusqlite::Result<Vec<PushDevice>> {
        let mut stmt = self.db.prepare(
            "SELECT token, device_id, device_type, app_id, created_at_ms, updated_at_ms FROM push_devices",
        )?;
        let rows = stmt.query_map([], |row| {
            let token: String = row.get(0)?;
            let token_mask = mask_token(&token);
            Ok(PushDevice {
                token,
                token_mask,
                device_id: row.get(1)?,
                device_type: row.get(2)?,
                app_id: row.get(3)?,
                created_at_ms: row.get(4)?,
                updated_at_ms: row.get(5)?,
            })
        })?;
        rows.collect()
    }

    pub fn record_push_deliveries(&self, event_id: Uuid) -> rusqlite::Result<usize> {
        let devices = self.push_devices()?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let mut count = 0;
        for dev in &devices {
            let (channel, status) = match dev.device_type.as_str() {
                "fcm" | "apns" | "provider" => ("provider", "pending"),
                _ => ("local_simulation", "dispatched"),
            };
            self.db.execute(
                "INSERT INTO push_deliveries(event_id, token, channel, status, attempted_at_ms, error, retry_count, gateway_id)
                 VALUES (?1, ?2, ?3, ?4, ?5, NULL, 0, NULL)",
                params![event_id.to_string(), dev.token, channel, status, now],
            )?;
            count += 1;
        }
        Ok(count)
    }

    #[allow(dead_code)]
    pub fn record_provider_delivery(
        &self,
        event_id: Uuid,
        token: &str,
        status: &str,
        error: Option<&str>,
    ) -> rusqlite::Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        self.db.execute(
            "INSERT INTO push_deliveries(event_id, token, channel, status, attempted_at_ms, error, retry_count, gateway_id)
             VALUES (?1, ?2, 'provider', ?3, ?4, ?5, 0, NULL)",
            params![event_id.to_string(), token, status, now, error],
        )?;
        Ok(())
    }

    pub fn push_deliveries(&self, limit: i64) -> rusqlite::Result<Vec<PushDelivery>> {
        let mut stmt = self.db.prepare(
            "SELECT id, event_id, token, channel, status, attempted_at_ms, error, retry_count, gateway_id
             FROM push_deliveries ORDER BY id DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], |row| {
            let token: String = row.get(2)?;
            let token_mask = mask_token(&token);
            Ok(PushDelivery {
                id: row.get(0)?,
                event_id: row.get(1)?,
                token,
                token_mask,
                channel: row.get(3)?,
                status: row.get(4)?,
                attempted_at_ms: row.get(5)?,
                error: row.get(6)?,
                retry_count: row.get(7).unwrap_or(0),
                gateway_id: row.get(8).unwrap_or(None),
            })
        })?;
        rows.collect()
    }

    pub fn pending_provider_deliveries(
        &self,
        limit: i64,
    ) -> rusqlite::Result<Vec<PendingDelivery>> {
        let mut stmt = self.db.prepare(
            "SELECT id, event_id, token, retry_count
             FROM push_deliveries
             WHERE channel = 'provider' AND status = 'pending'
             ORDER BY id ASC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], |row| {
            Ok(PendingDelivery {
                id: row.get(0)?,
                event_id: row.get(1)?,
                token: row.get(2)?,
                retry_count: row.get(3)?,
            })
        })?;
        rows.collect()
    }

    pub fn update_delivery_status(
        &self,
        id: i64,
        status: &str,
        error: Option<&str>,
        gateway_id: Option<&str>,
    ) -> rusqlite::Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        self.db.execute(
            "UPDATE push_deliveries
             SET status = ?1, attempted_at_ms = ?2, error = ?3,
                 gateway_id = COALESCE(?4, gateway_id),
                 retry_count = retry_count + 1
             WHERE id = ?5",
            params![status, now, error, gateway_id, id],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn backup(&self, dest: impl AsRef<Path>) -> rusqlite::Result<()> {
        let mut dest_conn = Connection::open(dest)?;
        dest_conn.pragma_update(None, "journal_mode", "WAL")?;
        let backup = rusqlite::backup::Backup::new(&self.db, &mut dest_conn)?;
        backup.run_to_completion(100, std::time::Duration::from_millis(10), None)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn restore(
        src: impl AsRef<Path>,
        dest: impl AsRef<Path>,
        reset_stream_id: bool,
    ) -> rusqlite::Result<Self> {
        let src_conn = Connection::open(src)?;
        {
            let mut dest_conn = Connection::open(&dest)?;
            dest_conn.pragma_update(None, "journal_mode", "WAL")?;
            let backup = rusqlite::backup::Backup::new(&src_conn, &mut dest_conn)?;
            backup.run_to_completion(100, std::time::Duration::from_millis(10), None)?;
        }
        let mut store = Self::open(dest)?;
        if reset_stream_id {
            store.reset_stream_id()?;
        }
        Ok(store)
    }

    #[allow(dead_code)]
    pub fn reset_stream_id(&mut self) -> rusqlite::Result<String> {
        let new_id: String = self.db.query_row(
            "UPDATE relay_metadata SET value = lower(hex(randomblob(16)))
             WHERE name = 'stream_id'
             RETURNING value",
            [],
            |row| row.get(0),
        )?;
        self.stream_id = new_id.clone();
        Ok(new_id)
    }

    #[allow(dead_code)]
    pub fn prune(
        &self,
        older_than_ms: Option<i64>,
        keep_last: Option<i64>,
    ) -> rusqlite::Result<usize> {
        let mut count = 0;
        if let Some(older) = older_than_ms {
            self.db.execute(
                "DELETE FROM push_deliveries WHERE event_id IN (SELECT id FROM events WHERE received_at_ms < ?1)",
                params![older],
            )?;
            count += self.db.execute(
                "DELETE FROM events WHERE received_at_ms < ?1",
                params![older],
            )?;
        }
        if let Some(keep) = keep_last {
            self.db.execute(
                "DELETE FROM push_deliveries WHERE event_id IN (
                    SELECT id FROM events WHERE seq NOT IN (
                        SELECT seq FROM events ORDER BY seq DESC LIMIT ?1
                    )
                )",
                params![keep],
            )?;
            count += self.db.execute(
                "DELETE FROM events WHERE seq NOT IN (
                    SELECT seq FROM events ORDER BY seq DESC LIMIT ?1
                )",
                params![keep],
            )?;
        }
        Ok(count)
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
            frontend: None,
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
        assert!(db.cursor_matches(1, event.id).unwrap());
        assert!(!db.cursor_matches(2, event.id).unwrap());
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

    #[test]
    fn stream_id_is_persisted_and_unique_per_database() {
        let other = Store::open(":memory:").unwrap();
        let path = std::env::temp_dir().join(format!("ainotify-{}.sqlite3", other.stream_id()));
        let original = Store::open(&path).unwrap().stream_id().to_owned();
        let reopened = Store::open(&path).unwrap().stream_id().to_owned();
        std::fs::remove_file(path).unwrap();
        assert_eq!(original, reopened);
        assert_ne!(original, other.stream_id());
    }

    #[test]
    fn registers_push_devices_and_records_deliveries_on_insert() {
        let db = Store::open(":memory:").unwrap();
        db.register_device(
            "phone",
            "token-1",
            "android",
            "io.github.jerryskywalker.ainotify",
        )
        .unwrap();
        let devices = db.push_devices().unwrap();
        assert_eq!(devices.len(), 1);
        assert_eq!(devices[0].token, "token-1");
        assert_eq!(devices[0].token_mask, "***en-1");

        // Verify JSON serialization hides raw token
        let devices_json = serde_json::to_string(&devices).unwrap();
        assert!(!devices_json.contains("token-1"));
        assert!(!devices_json.contains("\"token\""));
        assert!(devices_json.contains("\"token_mask\":\"***en-1\""));

        let event = Event {
            schema_version: 1,
            id: "11111111-2222-3333-4444-555555555555".parse().unwrap(),
            tool: "codex".into(),
            frontend: None,
            kind: "turn.completed".into(),
            device_name: "Test".into(),
            occurred_at_ms: 100,
            project: None,
            session_id: None,
            turn_id: None,
            message: Some("hello".into()),
            message_truncated: false,
        };
        assert!(matches!(db.insert("win", &event).unwrap(), Insert::New(1)));
        let deliveries = db.push_deliveries(10).unwrap();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].token, "token-1");
        assert_eq!(deliveries[0].token_mask, "***en-1");
        assert_eq!(deliveries[0].channel, "local_simulation");
        assert_eq!(deliveries[0].event_id, event.id.to_string());

        // Verify JSON serialization hides raw token and records channel
        let deliveries_json = serde_json::to_string(&deliveries).unwrap();
        assert!(!deliveries_json.contains("token-1"));
        assert!(!deliveries_json.contains("\"token\""));
        assert!(deliveries_json.contains("\"channel\":\"local_simulation\""));

        // Record a provider delivery
        db.record_provider_delivery(event.id, "provider-token-999", "received", None)
            .unwrap();
        let deliveries2 = db.push_deliveries(10).unwrap();
        assert_eq!(deliveries2.len(), 2);
        assert_eq!(deliveries2[0].channel, "provider");
        assert_eq!(deliveries2[0].status, "received");
        assert_eq!(deliveries2[0].token_mask, "***-999");
        assert_eq!(deliveries2[1].channel, "local_simulation");

        assert!(db.unregister_device("token-1").unwrap());
        assert!(db.push_devices().unwrap().is_empty());
    }

    #[test]
    fn regression_provider_delivery_not_marked_dispatched_without_gateway() {
        let db = Store::open(":memory:").unwrap();
        db.register_device(
            "phone",
            "fcm-token-123",
            "fcm",
            "io.github.jerryskywalker.ainotify",
        )
        .unwrap();
        let event = Event {
            schema_version: 1,
            id: Uuid::new_v4(),
            tool: "codex".into(),
            frontend: None,
            kind: "turn.completed".into(),
            device_name: "Test".into(),
            occurred_at_ms: 100,
            project: None,
            session_id: None,
            turn_id: None,
            message: Some("hello".into()),
            message_truncated: false,
        };
        db.insert("win", &event).unwrap();
        let deliveries = db.push_deliveries(10).unwrap();
        assert_eq!(deliveries.len(), 1);
        assert_eq!(deliveries[0].channel, "provider");
        // Regression: Provider delivery must be recorded as 'pending' awaiting gateway call,
        // and must NOT be marked 'dispatched' before the gateway accepts it.
        assert_eq!(
            deliveries[0].status, "pending",
            "Provider delivery was incorrectly marked dispatched before gateway acceptance"
        );
    }

    #[test]
    fn backups_restores_resets_stream_and_prunes() {
        let temp_dir = std::env::temp_dir().join(format!("ainotify-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let db_path = temp_dir.join("live.sqlite3");
        let backup_path = temp_dir.join("backup.sqlite3");
        let restored_path = temp_dir.join("restored.sqlite3");

        let mut db = Store::open(&db_path).unwrap();
        let initial_stream = db.stream_id().to_owned();

        for i in 1..=5 {
            let event = Event {
                schema_version: 1,
                id: Uuid::new_v4(),
                tool: "codex".into(),
                frontend: None,
                kind: "turn.completed".into(),
                device_name: "Test".into(),
                occurred_at_ms: i * 1000,
                project: None,
                session_id: None,
                turn_id: None,
                message: Some(format!("msg-{}", i)),
                message_truncated: false,
            };
            assert!(matches!(db.insert("win", &event).unwrap(), Insert::New(_)));
        }
        assert_eq!(db.events(0, 10).unwrap().len(), 5);

        // Perform consistent backup
        db.backup(&backup_path).unwrap();
        assert!(backup_path.exists());

        // Restore without stream reset
        let restored_same_stream = Store::restore(&backup_path, &restored_path, false).unwrap();
        assert_eq!(restored_same_stream.stream_id(), initial_stream);
        assert_eq!(restored_same_stream.events(0, 10).unwrap().len(), 5);

        // Restore with stream reset
        let restored_reset_path = temp_dir.join("restored_reset.sqlite3");
        let restored_new_stream = Store::restore(&backup_path, &restored_reset_path, true).unwrap();
        assert_ne!(restored_new_stream.stream_id(), initial_stream);
        assert_eq!(restored_new_stream.events(0, 10).unwrap().len(), 5);

        // Reset stream on live db
        let updated_stream = db.reset_stream_id().unwrap();
        assert_eq!(db.stream_id(), updated_stream);
        assert_ne!(updated_stream, initial_stream);

        // Test pruning: keep last 2
        let pruned = db.prune(None, Some(2)).unwrap();
        assert_eq!(pruned, 3);
        let remaining = db.events(0, 10).unwrap();
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining[0].seq, 4);
        assert_eq!(remaining[1].seq, 5);

        let _ = std::fs::remove_dir_all(temp_dir);
    }
}
