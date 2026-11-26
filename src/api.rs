use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
};
use tokio::sync::mpsc;

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, Query, State as AxumState},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::mailbox::{self, Action, Applied, MailState};
use crate::store::{Event, EventSummary, Insert, Store};

#[derive(Clone)]
pub struct State(Arc<Service>);

struct Service {
    store: Arc<Mutex<Store>>,
    auth: Auth,
    fcm_notifier: Option<mpsc::Sender<()>>,
}

impl State {
    pub fn new(store: Store, auth: Auth) -> Self {
        Self(Arc::new(Service {
            store: Arc::new(Mutex::new(store)),
            auth,
            fcm_notifier: None,
        }))
    }

    pub fn with_fcm_notifier(mut self, tx: mpsc::Sender<()>) -> Self {
        if let Some(s) = Arc::get_mut(&mut self.0) {
            s.fcm_notifier = Some(tx);
        } else {
            self.0 = Arc::new(Service {
                store: self.0.store.clone(),
                auth: self.0.auth.clone(),
                fcm_notifier: Some(tx),
            });
        }
        self
    }

    pub fn store_arc(&self) -> Arc<Mutex<Store>> {
        self.0.store.clone()
    }

    fn store(&self) -> Result<MutexGuard<'_, Store>, ApiError> {
        self.0.store.lock().map_err(|_| ApiError::internal())
    }
}

#[derive(Clone)]
pub struct Auth {
    readers: Vec<[u8; 32]>,
    inboxes: Vec<[u8; 32]>,
    writers: HashMap<String, Vec<[u8; 32]>>,
}

fn split_tokens(raw: &str) -> Vec<&str> {
    raw.split([',', ';'])
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .collect()
}

impl Auth {
    pub fn new(
        reader: &str,
        writers: HashMap<String, String>,
        inbox: Option<&str>,
    ) -> Result<Self, &'static str> {
        let reader_tokens = split_tokens(reader);
        if reader_tokens.is_empty()
            || reader_tokens.iter().any(|t| !valid_token(t))
            || writers.is_empty()
        {
            return Err("configure a reader token and at least one writer");
        }
        let mut used = HashSet::new();
        let mut readers = Vec::new();
        for t in reader_tokens {
            let h = hash(t);
            if !used.insert(h) {
                return Err("each token must be unique");
            }
            readers.push(h);
        }

        let mut inboxes = Vec::new();
        if let Some(inbox_str) = inbox {
            let tokens = split_tokens(inbox_str);
            if tokens.is_empty() || tokens.iter().any(|t| !valid_token(t)) {
                return Err("configure a valid inbox token distinct from all other tokens");
            }
            for t in tokens {
                let h = hash(t);
                if !used.insert(h) {
                    return Err("configure a valid inbox token distinct from all other tokens");
                }
                inboxes.push(h);
            }
        }

        let mut devices = HashMap::new();
        for (device, token_str) in writers {
            if device.is_empty()
                || device.len() > 64
                || !device
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
            {
                return Err("invalid device ID or writer token");
            }
            let tokens = split_tokens(&token_str);
            if tokens.is_empty() || tokens.iter().any(|t| !valid_token(t)) {
                return Err("invalid device ID or writer token");
            }
            let mut device_tokens = Vec::new();
            for t in tokens {
                let h = hash(t);
                if !used.insert(h) {
                    return Err("each device and reader must have a distinct token");
                }
                device_tokens.push(h);
            }
            devices.insert(device, device_tokens);
        }
        Ok(Self {
            readers,
            inboxes,
            writers: devices,
        })
    }

    fn reader(&self, headers: &HeaderMap) -> Result<bool, ApiError> {
        let presented = bearer(headers)?;
        if self
            .inboxes
            .iter()
            .any(|token| bool::from(token.ct_eq(&presented)))
        {
            Ok(true)
        } else if self
            .readers
            .iter()
            .any(|token| bool::from(token.ct_eq(&presented)))
        {
            Ok(false)
        } else {
            Err(ApiError::unauthorized())
        }
    }

    fn writer(&self, headers: &HeaderMap) -> Result<&str, ApiError> {
        let presented = bearer(headers)?;
        self.writers
            .iter()
            .find_map(|(id, stored_tokens)| {
                stored_tokens
                    .iter()
                    .any(|stored| bool::from(stored.ct_eq(&presented)))
                    .then_some(id.as_str())
            })
            .ok_or_else(ApiError::unauthorized)
    }
}

fn valid_token(token: &str) -> bool {
    (24..=512).contains(&token.len()) && token.bytes().all(|byte| byte.is_ascii_graphic())
}

fn hash(value: &str) -> [u8; 32] {
    Sha256::digest(value.as_bytes()).into()
}

fn bearer(headers: &HeaderMap) -> Result<[u8; 32], ApiError> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| valid_token(value))
        .map(hash)
        .ok_or_else(ApiError::unauthorized)
}

pub fn router(state: State) -> Router {
    Router::new()
        .route("/healthz", get(|| async { Json(json!({"ok": true})) }))
        .route("/v1/writer/status", get(writer_status))
        .route("/v1/events", post(create_event).get(events))
        .route("/v1/events/{id}", get(event_detail))
        .route("/v1/inbox/changes", get(inbox_changes))
        .route("/v1/inbox/actions", post(inbox_action))
        .route("/v1/devices/register", post(register_device))
        .route("/v1/devices/unregister", post(unregister_device))
        .route("/v1/devices", get(list_devices))
        .route("/v1/push/deliveries", get(push_deliveries))
        .layer(DefaultBodyLimit::max(820 * 1024))
        .with_state(state)
}

async fn writer_status(
    AxumState(state): AxumState<State>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let id = state.0.auth.writer(&headers)?;
    Ok(Json(json!({"device_id": id})))
}

async fn create_event(
    AxumState(state): AxumState<State>,
    headers: HeaderMap,
    Json(event): Json<Event>,
) -> Result<impl IntoResponse, ApiError> {
    let device = state.0.auth.writer(&headers)?;
    if !event.valid() {
        return Err(ApiError(StatusCode::UNPROCESSABLE_ENTITY, "invalid event"));
    }
    let result = state
        .store()?
        .insert(device, &event)
        .map_err(ApiError::database)?;
    let (status, seq) = match result {
        Insert::New(seq) => {
            if let Some(ref tx) = state.0.fcm_notifier {
                let _ = tx.try_send(());
            }
            (StatusCode::CREATED, seq)
        }
        Insert::Existing(seq) => (StatusCode::OK, seq),
        Insert::Conflict => return Err(ApiError(StatusCode::CONFLICT, "event ID already in use")),
    };
    Ok((status, Json(json!({"id": event.id, "seq": seq}))))
}

#[derive(Deserialize)]
struct Page {
    #[serde(default)]
    after: i64,
    limit: Option<i64>,
    stream_id: Option<String>,
    cursor_event_id: Option<Uuid>,
}

impl Page {
    fn reset_event_cursor(&self, store: &Store) -> rusqlite::Result<bool> {
        if let Some(stream_id) = &self.stream_id {
            return Ok(stream_id != store.stream_id());
        }
        match self.cursor_event_id {
            Some(id) if self.after > 0 => Ok(!store.cursor_matches(self.after, id)?),
            _ => Ok(false),
        }
    }
}

#[derive(Serialize)]
struct Feed {
    events: Vec<EventSummary>,
    next_cursor: i64,
    stream_id: String,
    reset_cursor: bool,
}

async fn events(
    AxumState(state): AxumState<State>,
    headers: HeaderMap,
    Query(page): Query<Page>,
) -> Result<Json<Feed>, ApiError> {
    state.0.auth.reader(&headers)?;
    let limit = page.limit.unwrap_or(50);
    if page.after < 0 || !(1..=100).contains(&limit) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid cursor or limit"));
    }
    let store = state.store()?;
    let reset_cursor = page
        .reset_event_cursor(&store)
        .map_err(ApiError::database)?;
    let after = if reset_cursor { 0 } else { page.after };
    let entries = store.events(after, limit).map_err(ApiError::database)?;
    let stream_id = store.stream_id().to_owned();
    drop(store);
    let next_cursor = entries.last().map_or(after, |entry| entry.seq);
    Ok(Json(Feed {
        events: entries.iter().map(|entry| entry.summary()).collect(),
        next_cursor,
        stream_id,
        reset_cursor,
    }))
}

#[derive(Serialize)]
struct Detail {
    #[serde(flatten)]
    summary: EventSummary,
    turn_id: Option<String>,
    message: Option<String>,
}

async fn event_detail(
    AxumState(state): AxumState<State>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Result<Json<Detail>, ApiError> {
    state.0.auth.reader(&headers)?;
    let entry = state
        .store()?
        .event(id)
        .map_err(ApiError::database)?
        .ok_or(ApiError(StatusCode::NOT_FOUND, "event not found"))?;
    Ok(Json(Detail {
        summary: entry.summary(),
        turn_id: entry.event.turn_id,
        message: entry.event.message,
    }))
}

#[derive(Serialize)]
struct Changes {
    changes: Vec<MailState>,
    next_cursor: i64,
    can_write: bool,
}

async fn inbox_changes(
    AxumState(state): AxumState<State>,
    headers: HeaderMap,
    Query(page): Query<Page>,
) -> Result<Json<Changes>, ApiError> {
    let can_write = state.0.auth.reader(&headers)?;
    let limit = page.limit.unwrap_or(50);
    if page.after < 0 || !(1..=100).contains(&limit) {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid cursor or limit"));
    }
    let changes =
        mailbox::changes(&state.store()?.db, page.after, limit).map_err(ApiError::database)?;
    let next_cursor = changes.last().map_or(page.after, |entry| entry.seq);
    Ok(Json(Changes {
        changes,
        next_cursor,
        can_write,
    }))
}

async fn inbox_action(
    AxumState(state): AxumState<State>,
    headers: HeaderMap,
    Json(action): Json<Action>,
) -> Result<Json<MailState>, ApiError> {
    if !state.0.auth.reader(&headers)? {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "inbox write access required",
        ));
    }
    if !action.valid() {
        return Err(ApiError(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid inbox action",
        ));
    }
    match mailbox::apply(&mut state.store()?.db, &action).map_err(ApiError::database)? {
        Applied::State(state) => Ok(Json(state)),
        Applied::Missing => Err(ApiError(StatusCode::NOT_FOUND, "event not found")),
        Applied::Conflict => Err(ApiError(StatusCode::CONFLICT, "action ID already in use")),
    }
}

#[derive(Deserialize)]
struct RegisterDevice {
    token: String,
    #[serde(default = "default_device_type")]
    device_type: String,
    #[serde(default = "default_app_id")]
    app_id: String,
}

fn default_device_type() -> String {
    "android".into()
}

fn default_app_id() -> String {
    "io.github.jerryskywalker.ainotify".into()
}

#[derive(Deserialize)]
struct UnregisterDevice {
    token: String,
}

async fn register_device(
    AxumState(state): AxumState<State>,
    headers: HeaderMap,
    Json(body): Json<RegisterDevice>,
) -> Result<impl IntoResponse, ApiError> {
    let is_inbox = state.0.auth.reader(&headers)?;
    let device_id = if is_inbox { "inbox" } else { "reader" };
    if body.token.trim().is_empty() || body.token.len() > 4096 {
        return Err(ApiError(StatusCode::BAD_REQUEST, "invalid push token"));
    }
    state
        .store()?
        .register_device(
            device_id,
            body.token.trim(),
            &body.device_type,
            &body.app_id,
        )
        .map_err(ApiError::database)?;
    Ok((StatusCode::OK, Json(json!({"ok": true}))))
}

async fn unregister_device(
    AxumState(state): AxumState<State>,
    headers: HeaderMap,
    Json(body): Json<UnregisterDevice>,
) -> Result<impl IntoResponse, ApiError> {
    let _ = state.0.auth.reader(&headers)?;
    let removed = state
        .store()?
        .unregister_device(body.token.trim())
        .map_err(ApiError::database)?;
    Ok((
        StatusCode::OK,
        Json(json!({"ok": true, "removed": removed})),
    ))
}

async fn list_devices(
    AxumState(state): AxumState<State>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    let _ = state.0.auth.reader(&headers)?;
    let devices = state.store()?.push_devices().map_err(ApiError::database)?;
    Ok(Json(json!({"devices": devices})))
}

async fn push_deliveries(
    AxumState(state): AxumState<State>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    let _ = state.0.auth.reader(&headers)?;
    let deliveries = state
        .store()?
        .push_deliveries(50)
        .map_err(ApiError::database)?;
    Ok(Json(json!({"deliveries": deliveries})))
}

#[derive(Debug)]
struct ApiError(StatusCode, &'static str);

impl ApiError {
    fn unauthorized() -> Self {
        Self(StatusCode::UNAUTHORIZED, "invalid or missing credentials")
    }

    fn internal() -> Self {
        Self(StatusCode::INTERNAL_SERVER_ERROR, "relay storage error")
    }

    fn database(err: rusqlite::Error) -> Self {
        eprintln!("relay storage error: {err}");
        Self::internal()
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_cursor_resets_for_replaced_stream_and_missing_legacy_anchor() {
        let store = Store::open(":memory:").unwrap();
        let mut page = Page {
            after: 5,
            limit: None,
            stream_id: Some(store.stream_id().to_owned()),
            cursor_event_id: None,
        };
        assert!(!page.reset_event_cursor(&store).unwrap());
        page.stream_id = Some("another-stream".into());
        assert!(page.reset_event_cursor(&store).unwrap());
        page.stream_id = None;
        assert!(!page.reset_event_cursor(&store).unwrap());
        page.cursor_event_id = Some(Uuid::from_u128(1));
        assert!(page.reset_event_cursor(&store).unwrap());
    }

    #[test]
    fn credential_rotation_allows_parallel_transition_and_revocation() {
        let r1 = "reader-token-version-one-1234567";
        let r2 = "reader-token-version-two-1234567";
        let i1 = "inbox--token-version-one-1234567";
        let i2 = "inbox--token-version-two-1234567";
        let w1 = "writer-token-version-one-1234567";
        let w2 = "writer-token-version-two-1234567";

        // During rotation: both v1 and v2 accepted
        let mut writers = HashMap::new();
        writers.insert("device-1".into(), format!("{w1}, {w2}"));
        let auth = Auth::new(
            &format!("{r1}, {r2}"),
            writers,
            Some(&format!("{i1}, {i2}")),
        )
        .unwrap();

        let header_for = |token: &str| {
            let mut h = HeaderMap::new();
            h.insert(
                header::AUTHORIZATION,
                format!("Bearer {token}").parse().unwrap(),
            );
            h
        };

        // Readers: both accepted
        assert_eq!(auth.reader(&header_for(r1)).unwrap(), false);
        assert_eq!(auth.reader(&header_for(r2)).unwrap(), false);

        // Inboxes: both accepted as inbox (true)
        assert_eq!(auth.reader(&header_for(i1)).unwrap(), true);
        assert_eq!(auth.reader(&header_for(i2)).unwrap(), true);

        // Writers: both accepted
        assert_eq!(auth.writer(&header_for(w1)).unwrap(), "device-1");
        assert_eq!(auth.writer(&header_for(w2)).unwrap(), "device-1");

        // Invalid token rejected
        assert!(
            auth.reader(&header_for("invalid-token-string-1234567"))
                .is_err()
        );
        assert!(
            auth.writer(&header_for("invalid-token-string-1234567"))
                .is_err()
        );

        // After rotation: v1 revoked, only v2 accepted
        let mut final_writers = HashMap::new();
        final_writers.insert("device-1".into(), w2.into());
        let final_auth = Auth::new(r2, final_writers, Some(i2)).unwrap();

        // v1 now unauthorized
        assert!(final_auth.reader(&header_for(r1)).is_err());
        assert!(final_auth.reader(&header_for(i1)).is_err());
        assert!(final_auth.writer(&header_for(w1)).is_err());

        // v2 still authorized
        assert_eq!(final_auth.reader(&header_for(r2)).unwrap(), false);
        assert_eq!(final_auth.reader(&header_for(i2)).unwrap(), true);
        assert_eq!(final_auth.writer(&header_for(w2)).unwrap(), "device-1");
    }
}
