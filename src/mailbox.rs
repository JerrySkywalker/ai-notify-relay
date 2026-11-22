use rusqlite::{Connection, OptionalExtension, Row, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Folder {
    #[default]
    Inbox,
    Archive,
    Trash,
}

impl Folder {
    fn as_str(self) -> &'static str {
        match self {
            Self::Inbox => "inbox",
            Self::Archive => "archive",
            Self::Trash => "trash",
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct Action {
    pub id: Uuid,
    pub event_id: Uuid,
    pub folder: Option<Folder>,
    pub pinned: Option<bool>,
}

impl Action {
    pub fn valid(&self) -> bool {
        !self.id.is_nil()
            && !self.event_id.is_nil()
            && (self.folder.is_some() || self.pinned.is_some())
    }
}

#[derive(Serialize)]
pub struct MailState {
    pub event_id: String,
    pub seq: i64,
    pub folder: String,
    pub pinned: bool,
}

pub enum Applied {
    State(MailState),
    Missing,
    Conflict,
}

pub fn migrate(db: &Connection) -> rusqlite::Result<()> {
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS inbox_ops (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            id TEXT NOT NULL UNIQUE,
            event_id TEXT NOT NULL,
            payload TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS inbox_state (
            event_id TEXT PRIMARY KEY,
            seq INTEGER NOT NULL,
            folder TEXT NOT NULL,
            pinned INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS inbox_state_seq ON inbox_state(seq);",
    )
}

pub fn apply(db: &mut Connection, action: &Action) -> rusqlite::Result<Applied> {
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let id = action.id.to_string();
    let event_id = action.event_id.to_string();
    let payload = serde_json::to_string(action)
        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
    let previous: Option<String> = tx
        .query_row(
            "SELECT payload FROM inbox_ops WHERE id = ?1",
            [&id],
            |row| row.get(0),
        )
        .optional()?;
    if previous.as_ref().is_some_and(|saved| *saved != payload) {
        return Ok(Applied::Conflict);
    }
    if previous.is_none() {
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM events WHERE id = ?1)",
            [&event_id],
            |row| row.get(0),
        )?;
        if !exists {
            return Ok(Applied::Missing);
        }
        tx.execute(
            "INSERT INTO inbox_ops(id, event_id, payload) VALUES (?1, ?2, ?3)",
            params![id, event_id, payload],
        )?;
        let seq = tx.last_insert_rowid();
        tx.execute(
            "INSERT INTO inbox_state(event_id, seq, folder, pinned)
             VALUES (?1, ?2, COALESCE(?3, 'inbox'), COALESCE(?4, 0))
             ON CONFLICT(event_id) DO UPDATE SET seq = ?2,
             folder = COALESCE(?3, inbox_state.folder), pinned = COALESCE(?4, inbox_state.pinned)",
            params![
                event_id,
                seq,
                action.folder.map(Folder::as_str),
                action.pinned
            ],
        )?;
    }
    let state = tx.query_row(
        "SELECT event_id, seq, folder, pinned FROM inbox_state WHERE event_id = ?1",
        [&event_id],
        read_state,
    )?;
    tx.commit()?;
    Ok(Applied::State(state))
}

pub fn changes(db: &Connection, after: i64, limit: i64) -> rusqlite::Result<Vec<MailState>> {
    db.prepare(
        "SELECT event_id, seq, folder, pinned FROM inbox_state WHERE seq > ?1 ORDER BY seq LIMIT ?2",
    )?
    .query_map(params![after, limit], read_state)?
    .collect()
}

fn read_state(row: &Row<'_>) -> rusqlite::Result<MailState> {
    Ok(MailState {
        event_id: row.get(0)?,
        seq: row.get(1)?,
        folder: row.get(2)?,
        pinned: row.get(3)?,
    })
}
