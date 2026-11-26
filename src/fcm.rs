use crate::store::Store;
use gcp_auth::{CustomServiceAccount, TokenProvider};
use std::env;
use std::fs;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{OnceCell, mpsc};

const FCM_SCOPE: &str = "https://www.googleapis.com/auth/firebase.messaging";

#[derive(Clone)]
pub struct FcmConfig {
    pub project_id: Option<String>,
    pub gateway_url: String,
    pub access_token: Option<String>,
    pub credentials_file: Option<String>,
}

impl FcmConfig {
    pub fn from_env() -> Self {
        let mut project_id = env::var("AINOTIFY_FCM_PROJECT")
            .ok()
            .or_else(|| env::var("FCM_PROJECT_ID").ok());

        let gateway_url = env::var("AINOTIFY_FCM_GATEWAY_URL")
            .ok()
            .or_else(|| env::var("FCM_GATEWAY_URL").ok())
            .unwrap_or_else(|| "https://fcm.googleapis.com".to_string());

        let access_token = env::var("AINOTIFY_FCM_TOKEN")
            .ok()
            .or_else(|| env::var("FCM_ACCESS_TOKEN").ok())
            .or_else(|| env::var("GOOGLE_OAUTH_TOKEN").ok());

        let standard_adc_file = env::var("GOOGLE_APPLICATION_CREDENTIALS").ok();
        let credentials_file = standard_adc_file
            .is_none()
            .then(|| env::var("AINOTIFY_FCM_CREDENTIALS_FILE").ok())
            .flatten();

        if project_id.is_none() {
            if let Some(path) = standard_adc_file.as_ref().or(credentials_file.as_ref()) {
                if let Ok(content) = fs::read_to_string(path) {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&content) {
                        if let Some(p) = v.get("project_id").and_then(|s| s.as_str()) {
                            project_id = Some(p.to_string());
                        }
                    }
                }
            }
        }

        Self {
            project_id,
            gateway_url,
            access_token,
            credentials_file,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.project_id.is_some()
    }

    async fn get_token(
        &self,
        adc: &OnceCell<Arc<dyn TokenProvider>>,
    ) -> Result<String, &'static str> {
        if let Some(token) = self
            .access_token
            .as_deref()
            .map(str::trim)
            .filter(|t| !t.is_empty())
        {
            return Ok(token.to_owned());
        }
        let credentials = adc
            .get_or_try_init(|| async {
                match &self.credentials_file {
                    Some(path) => CustomServiceAccount::from_file(path)
                        .map(|provider| Arc::new(provider) as Arc<dyn TokenProvider>),
                    None => gcp_auth::provider().await,
                }
                .map_err(|_| "Could not configure Google ADC for FCM")
            })
            .await?;
        credentials
            .token(&[FCM_SCOPE])
            .await
            .map(|token| token.as_str().to_owned())
            .map_err(|_| "Google ADC could not obtain an FCM access token")
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum FcmDeliveryResult {
    Accepted { gateway_id: String },
    TemporaryFailure { error: String, status_code: u16 },
    InvalidToken { reason: String },
    PermanentFailure { error: String },
}

fn send_fcm_message(
    config: &FcmConfig,
    auth_token: &str,
    token: &str,
    event_id: &str,
) -> Result<FcmDeliveryResult, String> {
    let project_id = config
        .project_id
        .as_deref()
        .ok_or_else(|| "FCM project_id is required".to_string())?;

    let url = format!(
        "{}/v1/projects/{}/messages:send",
        config.gateway_url.trim_end_matches('/'),
        project_id
    );

    // Push data only carries event_id; body is retrieved by client from Relay
    let payload = serde_json::json!({
        "message": {
            "token": token,
            "data": {
                "event_id": event_id
            },
            "android": {
                "priority": "HIGH"
            }
        }
    });

    let mut config_builder = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(10)))
        .http_status_as_error(false);

    if url.contains("://127.0.0.1") || url.contains("://localhost") {
        config_builder = config_builder.proxy(None);
    }

    let agent = config_builder.build().new_agent();

    let response = agent
        .post(&url)
        .header("Authorization", &format!("Bearer {}", auth_token))
        .header("Content-Type", "application/json")
        .send_json(&payload);

    match response {
        Ok(mut res) => {
            let status = res.status().as_u16();
            if status == 200 {
                let json: serde_json::Value = res
                    .body_mut()
                    .read_json()
                    .map_err(|e| format!("Failed to parse FCM response JSON: {}", e))?;
                let name = json
                    .get("name")
                    .and_then(|n| n.as_str())
                    .filter(|id| id.starts_with("projects/") && id.contains("/messages/"))
                    .ok_or_else(|| "FCM response is missing a message ID".to_owned())?
                    .to_string();
                Ok(FcmDeliveryResult::Accepted { gateway_id: name })
            } else if status == 429
                || status == 500
                || status == 502
                || status == 503
                || status == 504
            {
                Ok(FcmDeliveryResult::TemporaryFailure {
                    error: format!("FCM temporarily unavailable (HTTP {status})"),
                    status_code: status,
                })
            } else if status == 401 || status == 403 {
                Ok(FcmDeliveryResult::TemporaryFailure {
                    error: format!("FCM authorization failed (HTTP {status})"),
                    status_code: status,
                })
            } else if status == 404
                && res
                    .body_mut()
                    .read_json::<serde_json::Value>()
                    .ok()
                    .and_then(|body| {
                        body["error"]["details"].as_array().map(|details| {
                            details
                                .iter()
                                .any(|item| item["errorCode"] == "UNREGISTERED")
                        })
                    })
                    .unwrap_or(false)
            {
                Ok(FcmDeliveryResult::InvalidToken {
                    reason: "FCM UNREGISTERED".to_owned(),
                })
            } else {
                Ok(FcmDeliveryResult::PermanentFailure {
                    error: format!("FCM rejected the request (HTTP {status})"),
                })
            }
        }
        Err(_) => Ok(FcmDeliveryResult::TemporaryFailure {
            error: "FCM transport unavailable".to_owned(),
            status_code: 0,
        }),
    }
}

async fn process_pending_deliveries(
    store: &Mutex<Store>,
    config: &FcmConfig,
    adc: &OnceCell<Arc<dyn TokenProvider>>,
) -> usize {
    if !config.is_enabled() {
        return 0;
    }
    let Some(pending) = store
        .lock()
        .ok()
        .and_then(|db| db.pending_provider_deliveries(20).ok())
    else {
        return 0;
    };
    if pending.is_empty() {
        return 0;
    }
    let auth = tokio::time::timeout(Duration::from_secs(15), config.get_token(adc))
        .await
        .unwrap_or(Err("Timed out obtaining an FCM access token"));
    let mut processed = 0;
    for item in pending {
        let id = item.id;
        let device_token = item.token.clone();
        let result = match &auth {
            Ok(token) => {
                let config = config.clone();
                let token = token.clone();
                tokio::task::spawn_blocking(move || {
                    send_fcm_message(&config, &token, &item.token, &item.event_id)
                })
                .await
                .unwrap_or_else(|_| Err("FCM request task failed".into()))
            }
            Err(err) => Err((*err).to_owned()),
        };
        let Ok(store) = store.lock() else { break };
        match result {
            Ok(FcmDeliveryResult::Accepted { gateway_id }) => {
                let _ = store.update_delivery_status(id, "accepted", None, Some(&gateway_id));
                processed += 1;
            }
            Ok(FcmDeliveryResult::TemporaryFailure { error, .. }) => {
                let _ = store.update_delivery_status(id, "pending", Some(&error), None);
            }
            Ok(FcmDeliveryResult::InvalidToken { reason }) => {
                let _ = store.unregister_device(&device_token);
                let _ = store.update_delivery_status(id, "unregistered", Some(&reason), None);
                processed += 1;
            }
            Ok(FcmDeliveryResult::PermanentFailure { error }) => {
                let _ = store.update_delivery_status(id, "failed", Some(&error), None);
                processed += 1;
            }
            Err(err) => {
                let _ = store.update_delivery_status(id, "pending", Some(&err), None);
            }
        }
    }
    processed
}

pub fn start_dispatcher(store: Arc<Mutex<Store>>, config: FcmConfig) -> mpsc::Sender<()> {
    let (tx, mut rx) = mpsc::channel::<()>(32);
    if !config.is_enabled() {
        return tx;
    }

    tokio::spawn(async move {
        let adc = OnceCell::new();
        // The first interval tick handles pending deliveries left by the previous process.
        let mut interval = tokio::time::interval(Duration::from_millis(1500));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                Some(()) = rx.recv() => {}
            }
            process_pending_deliveries(&store, &config, &adc).await;
        }
    });

    tx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{Event, Insert};
    use axum::extract::State as AxumState;
    use axum::http::HeaderMap;
    use axum::response::IntoResponse;
    use axum::routing::post;
    use axum::{Json, Router, http::StatusCode};
    use rsa::RsaPrivateKey;
    use rsa::pkcs8::{EncodePrivateKey, LineEnding};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use uuid::Uuid;

    struct MockServerState {
        attempts: AtomicUsize,
        status_mode: AtomicUsize, // 0 = 200, 1 = 429, 2 = 404 UNREGISTERED
        token_requests: AtomicUsize,
        gateway_auth: Mutex<Vec<String>>,
        last_body: std::sync::Mutex<Option<serde_json::Value>>,
    }

    async fn mock_fcm_handler(
        AxumState(state): AxumState<Arc<MockServerState>>,
        headers: HeaderMap,
        Json(body): Json<serde_json::Value>,
    ) -> impl IntoResponse {
        state.attempts.fetch_add(1, Ordering::SeqCst);
        if let Some(header) = headers.get("Authorization") {
            state
                .gateway_auth
                .lock()
                .unwrap()
                .push(header.to_str().unwrap().to_owned());
        }
        *state.last_body.lock().unwrap() = Some(body.clone());
        let mode = state.status_mode.load(Ordering::SeqCst);
        match mode {
            1 => (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({
                    "error": {
                        "code": 429,
                        "message": "Resource exhausted",
                        "status": "RESOURCE_EXHAUSTED"
                    }
                })),
            ),
            2 => (
                StatusCode::NOT_FOUND,
                Json(json!({
                    "error": {
                        "code": 404,
                        "message": "Requested entity not found",
                        "status": "NOT_FOUND",
                        "details": [{
                            "errorCode": "UNREGISTERED"
                        }]
                    }
                })),
            ),
            3 => (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": {"status": "UNAUTHENTICATED"}})),
            ),
            4 => (
                StatusCode::NOT_FOUND,
                Json(json!({"error": {"status": "NOT_FOUND", "message": "Unknown project"}})),
            ),
            _ => (
                StatusCode::OK,
                Json(json!({
                    "name": "projects/test-proj/messages/mock-msg-0001"
                })),
            ),
        }
    }

    async fn mock_oauth_handler(
        AxumState(state): AxumState<Arc<MockServerState>>,
    ) -> Json<serde_json::Value> {
        let attempt = state.token_requests.fetch_add(1, Ordering::SeqCst);
        Json(json!({
            "access_token": format!("fixture-access-{attempt}"),
            "expires_in": if attempt == 0 { 1 } else { 3600 },
            "token_type": "Bearer",
        }))
    }

    async fn start_mock_fcm_server() -> (String, Arc<MockServerState>) {
        let state = Arc::new(MockServerState {
            attempts: AtomicUsize::new(0),
            status_mode: AtomicUsize::new(0),
            token_requests: AtomicUsize::new(0),
            gateway_auth: Mutex::new(Vec::new()),
            last_body: std::sync::Mutex::new(None),
        });
        let app = Router::new()
            .route(
                "/v1/projects/{project_id}/messages:send",
                post(mock_fcm_handler),
            )
            .route("/token", post(mock_oauth_handler))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://127.0.0.1:{}", addr.port()), state)
    }

    fn sample_event() -> Event {
        Event {
            schema_version: 1,
            id: Uuid::new_v4(),
            tool: "claude".into(),
            frontend: None,
            kind: "turn.completed".into(),
            device_name: "MacBook".into(),
            occurred_at_ms: 1000,
            project: None,
            session_id: None,
            turn_id: None,
            message: Some("Sensitive body should never appear in FCM payload".into()),
            message_truncated: false,
        }
    }

    #[tokio::test]
    async fn adc_refreshes_expired_tokens_and_reuses_valid_tokens() {
        let (gateway_url, mock) = start_mock_fcm_server().await;
        let key = RsaPrivateKey::new(&mut rand::rngs::OsRng, 2048).unwrap();
        let pem = key.to_pkcs8_pem(LineEnding::LF).unwrap();
        let account = CustomServiceAccount::from_json(
            &json!({
                "type": "service_account",
                "client_email": "fixture@example.invalid",
                "private_key": pem.as_str(),
                "token_uri": format!("{gateway_url}/token"),
                "project_id": "test-proj",
            })
            .to_string(),
        )
        .unwrap();
        let config = FcmConfig {
            project_id: Some("test-proj".into()),
            gateway_url,
            access_token: None,
            credentials_file: None,
        };
        let store = Mutex::new(Store::open(":memory:").unwrap());
        store
            .lock()
            .unwrap()
            .register_device("phone", "fixture-device", "fcm", "test.app")
            .unwrap();
        let adc = OnceCell::new();
        adc.set(Arc::new(account) as Arc<dyn TokenProvider>)
            .ok()
            .unwrap();
        for index in 0..3 {
            if index == 1 {
                tokio::time::sleep(Duration::from_millis(1100)).await;
            }
            store
                .lock()
                .unwrap()
                .insert("mac", &sample_event())
                .unwrap();
            assert_eq!(process_pending_deliveries(&store, &config, &adc).await, 1);
        }
        assert_eq!(mock.token_requests.load(Ordering::SeqCst), 2);
        assert_eq!(
            *mock.gateway_auth.lock().unwrap(),
            [
                "Bearer fixture-access-0",
                "Bearer fixture-access-1",
                "Bearer fixture-access-1",
            ]
        );
    }

    #[tokio::test]
    async fn gateway_authorization_and_project_errors_preserve_registration() {
        let (gateway_url, mock) = start_mock_fcm_server().await;
        let config = FcmConfig {
            project_id: Some("test-proj".into()),
            gateway_url,
            access_token: Some("fixture-bearer".into()),
            credentials_file: None,
        };
        let store = Mutex::new(Store::open(":memory:").unwrap());
        store
            .lock()
            .unwrap()
            .register_device("phone", "fixture-device", "fcm", "test.app")
            .unwrap();
        store
            .lock()
            .unwrap()
            .insert("mac", &sample_event())
            .unwrap();
        let adc = OnceCell::new();
        mock.status_mode.store(3, Ordering::SeqCst);
        assert_eq!(process_pending_deliveries(&store, &config, &adc).await, 0);
        assert_eq!(
            store.lock().unwrap().push_deliveries(1).unwrap()[0].status,
            "pending"
        );
        mock.status_mode.store(4, Ordering::SeqCst);
        assert_eq!(process_pending_deliveries(&store, &config, &adc).await, 1);
        assert_eq!(
            store.lock().unwrap().push_deliveries(1).unwrap()[0].status,
            "failed"
        );
        assert_eq!(store.lock().unwrap().push_devices().unwrap().len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fcm_gateway_dispatches_only_event_id_and_records_accepted() {
        let (gateway_url, mock_state) = start_mock_fcm_server().await;
        let config = FcmConfig {
            project_id: Some("test-proj".into()),
            gateway_url,
            access_token: Some("mock-bearer-token".into()),
            credentials_file: None,
        };

        let store = Store::open(":memory:").unwrap();
        store
            .register_device(
                "user-phone",
                "test-fcm-token-1",
                "fcm",
                "io.github.jerryskywalker.ainotify",
            )
            .unwrap();

        let event = sample_event();
        assert!(matches!(
            store.insert("mac", &event).unwrap(),
            Insert::New(_)
        ));

        // Delivery is initially 'pending' before calling gateway
        let pending = store.push_deliveries(10).unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].status, "pending");
        assert_eq!(pending[0].channel, "provider");

        // Process pending deliveries
        let store = Mutex::new(store);
        let processed = process_pending_deliveries(&store, &config, &OnceCell::new()).await;
        assert_eq!(processed, 1);

        // Verify gateway was called once
        assert_eq!(mock_state.attempts.load(Ordering::SeqCst), 1);
        let captured = mock_state.last_body.lock().unwrap().clone().unwrap();
        let msg = captured.get("message").unwrap();
        assert_eq!(
            msg.get("token").unwrap().as_str().unwrap(),
            "test-fcm-token-1"
        );
        assert_eq!(
            msg.get("data")
                .unwrap()
                .get("event_id")
                .unwrap()
                .as_str()
                .unwrap(),
            event.id.to_string()
        );
        // CRITICAL: Ensure message body / content is NOT in push payload
        assert!(msg.get("data").unwrap().get("message").is_none());
        assert!(!captured.to_string().contains("Sensitive body"));

        // Verify delivery record transitioned to 'accepted' with gateway_id
        let deliveries = store.lock().unwrap().push_deliveries(10).unwrap();
        assert_eq!(deliveries[0].status, "accepted");
        assert_eq!(
            deliveries[0].gateway_id.as_deref(),
            Some("projects/test-proj/messages/mock-msg-0001")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fcm_gateway_retries_429_temporary_failures_and_recovers() {
        let (gateway_url, mock_state) = start_mock_fcm_server().await;
        mock_state.status_mode.store(1, Ordering::SeqCst); // 429 TOO_MANY_REQUESTS

        let config = FcmConfig {
            project_id: Some("test-proj".into()),
            gateway_url,
            access_token: Some("mock-bearer-token".into()),
            credentials_file: None,
        };

        let store = Store::open(":memory:").unwrap();
        store
            .register_device(
                "user-phone",
                "test-fcm-token-2",
                "fcm",
                "io.github.jerryskywalker.ainotify",
            )
            .unwrap();

        let event = sample_event();
        store.insert("mac", &event).unwrap();

        // Round 1: 429 failure
        let store = Mutex::new(store);
        let processed = process_pending_deliveries(&store, &config, &OnceCell::new()).await;
        assert_eq!(processed, 0); // Not finished
        let deliveries = store.lock().unwrap().push_deliveries(10).unwrap();
        assert_eq!(deliveries[0].status, "pending");
        assert_eq!(deliveries[0].retry_count, 1);
        assert!(deliveries[0].error.as_ref().unwrap().contains("429"));

        // Round 2: Gateway recovers to 200 OK
        mock_state.status_mode.store(0, Ordering::SeqCst);
        let processed2 = process_pending_deliveries(&store, &config, &OnceCell::new()).await;
        assert_eq!(processed2, 1);
        let deliveries2 = store.lock().unwrap().push_deliveries(10).unwrap();
        assert_eq!(deliveries2[0].status, "accepted");
        assert_eq!(deliveries2[0].retry_count, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fcm_gateway_unregisters_invalid_or_expired_device_tokens() {
        let (gateway_url, mock_state) = start_mock_fcm_server().await;
        mock_state.status_mode.store(2, Ordering::SeqCst); // 404 UNREGISTERED

        let config = FcmConfig {
            project_id: Some("test-proj".into()),
            gateway_url,
            access_token: Some("mock-bearer-token".into()),
            credentials_file: None,
        };

        let store = Store::open(":memory:").unwrap();
        store
            .register_device(
                "user-phone",
                "expired-token-999",
                "fcm",
                "io.github.jerryskywalker.ainotify",
            )
            .unwrap();
        assert_eq!(store.push_devices().unwrap().len(), 1);

        let event = sample_event();
        store.insert("mac", &event).unwrap();

        let store = Mutex::new(store);
        let processed = process_pending_deliveries(&store, &config, &OnceCell::new()).await;
        assert_eq!(processed, 1);

        // Verify device was automatically unregistered from push_devices
        assert!(store.lock().unwrap().push_devices().unwrap().is_empty());

        // Verify delivery record status is 'unregistered'
        let deliveries = store.lock().unwrap().push_deliveries(10).unwrap();
        assert_eq!(deliveries[0].status, "unregistered");
        assert!(
            deliveries[0]
                .error
                .as_ref()
                .unwrap()
                .contains("UNREGISTERED")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fcm_dispatcher_recovers_pending_tasks_after_restart() {
        let (gateway_url, _) = start_mock_fcm_server().await;
        let config = FcmConfig {
            project_id: Some("test-proj".into()),
            gateway_url,
            access_token: Some("mock-bearer-token".into()),
            credentials_file: None,
        };

        let temp_dir = tempfile::tempdir().unwrap();
        let db_path = temp_dir.path().join("restart-test.sqlite3");

        let event_id = {
            let store = Store::open(&db_path).unwrap();
            store
                .register_device(
                    "phone",
                    "restart-token",
                    "fcm",
                    "io.github.jerryskywalker.ainotify",
                )
                .unwrap();
            let event = sample_event();
            store.insert("mac", &event).unwrap();
            event.id
            // store drops here, simulating server shutdown while task is pending
        };

        // Reopen database (simulating server restart)
        let store2 = Store::open(&db_path).unwrap();
        let pending_before = store2.pending_provider_deliveries(10).unwrap();
        assert_eq!(pending_before.len(), 1);
        assert_eq!(pending_before[0].event_id, event_id.to_string());

        // Process pending deliveries on restart
        let store2 = Mutex::new(store2);
        let processed = process_pending_deliveries(&store2, &config, &OnceCell::new()).await;
        assert_eq!(processed, 1);

        let deliveries = store2.lock().unwrap().push_deliveries(10).unwrap();
        assert_eq!(deliveries[0].status, "accepted");
        assert!(deliveries[0].gateway_id.is_some());
    }
}
