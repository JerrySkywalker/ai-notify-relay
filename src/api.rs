use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, MutexGuard},
};

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
    store: Mutex<Store>,
    auth: Auth,
}

impl State {
    pub fn new(store: Store, auth: Auth) -> Self {
        Self(Arc::new(Service {
            store: Mutex::new(store),
            auth,
        }))
    }

    fn store(&self) -> Result<MutexGuard<'_, Store>, ApiError> {
        self.0.store.lock().map_err(|_| ApiError::internal())
    }
}

pub struct Auth {
    reader: [u8; 32],
    inbox: Option<[u8; 32]>,
    writers: HashMap<String, [u8; 32]>,
}

impl Auth {
    pub fn new(
        reader: &str,
        writers: HashMap<String, String>,
        inbox: Option<&str>,
    ) -> Result<Self, &'static str> {
        if !valid_token(reader) || writers.is_empty() {
            return Err("configure a reader token and at least one writer");
        }
        let reader = hash(reader);
        let mut used = HashSet::from([reader]);
        if inbox.is_some_and(|token| !valid_token(token) || !used.insert(hash(token))) {
            return Err("configure a valid inbox token distinct from all other tokens");
        }
        let mut devices = HashMap::new();
        for (device, token) in writers {
            if device.is_empty()
                || device.len() > 64
                || !device
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
                || !valid_token(&token)
            {
                return Err("invalid device ID or writer token");
            }
            let token = hash(&token);
            if !used.insert(token) {
                return Err("each device and reader must have a distinct token");
            }
            devices.insert(device, token);
        }
        Ok(Self {
            reader,
            inbox: inbox.map(hash),
            writers: devices,
        })
    }

    fn reader(&self, headers: &HeaderMap) -> Result<bool, ApiError> {
        let presented = bearer(headers)?;
        if self
            .inbox
            .as_ref()
            .is_some_and(|token| bool::from(token.ct_eq(&presented)))
        {
            Ok(true)
        } else if bool::from(self.reader.ct_eq(&presented)) {
            Ok(false)
        } else {
            Err(ApiError::unauthorized())
        }
    }
    fn writer(&self, headers: &HeaderMap) -> Result<&str, ApiError> {
        let presented = bearer(headers)?;
        self.writers
            .iter()
            .find_map(|(id, stored)| bool::from(stored.ct_eq(&presented)).then_some(id.as_str()))
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
        Insert::New(seq) => (StatusCode::CREATED, seq),
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
}

#[derive(Serialize)]
struct Feed {
    events: Vec<EventSummary>,
    next_cursor: i64,
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
    let entries = state
        .store()?
        .events(page.after, limit)
        .map_err(ApiError::database)?;
    let next_cursor = entries.last().map_or(page.after, |entry| entry.seq);
    Ok(Json(Feed {
        events: entries.iter().map(|entry| entry.summary()).collect(),
        next_cursor,
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
        mailbox::changes(&state.store()?.0, page.after, limit).map_err(ApiError::database)?;
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
    match mailbox::apply(&mut state.store()?.0, &action).map_err(ApiError::database)? {
        Applied::State(state) => Ok(Json(state)),
        Applied::Missing => Err(ApiError(StatusCode::NOT_FOUND, "event not found")),
        Applied::Conflict => Err(ApiError(StatusCode::CONFLICT, "action ID already in use")),
    }
}

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
