# AINotify protocol v1

API paths are relative to the configured relay URL. The writer identity comes from the Bearer token. Reader and inbox credentials open the single owner's inbox.

| Method and path | Credentials | Behavior |
| --- | --- | --- |
| `GET /healthz` | None | Status without event data |
| `GET /v1/writer/status` | Writer | Returns its assigned `device_id` |
| `POST /v1/events` | Writer | Returns `201` for a new event or `200` for an identical retry by the same writer. A conflicting ID returns `409` |
| `GET /v1/events?after=0&limit=50` | Reader or inbox | Returns `events`, `next_cursor`, `stream_id` and `reset_cursor` |
| `GET /v1/events/{id}` | Reader or inbox | Returns the event summary, session identifiers and complete stored reply |
| `POST /v1/devices/register` | Reader or inbox | Registers or updates a device push token |
| `POST /v1/devices/unregister` | Reader or inbox | Removes a device push token |
| `GET /v1/devices` | Reader or inbox | Lists active registered push devices |
| `GET /v1/push/deliveries` | Reader or inbox | Returns recent push delivery attempts and status |

```json
{
  "schema_version": 1,
  "id": "93d42e8e-8a1d-4b11-9322-4c282125a377",
  "tool": "codex",
  "frontend": {"id":"codex-desktop","label":"Codex Desktop"},
  "kind": "turn.completed",
  "device_name": "Office PC",
  "occurred_at_ms": 1795334400000,
  "project": "demo",
  "session_id": "session-1",
  "turn_id": "turn-2",
  "message": "完成。\nAll tests passed.",
  "message_truncated": false
}
```

A collector chooses the event ID once, persists the event, and reuses both for retries. The server assigns an increasing `seq` for delivery order. Event time is a source timestamp for display. List items include at most 240 Unicode scalar values of preview, with an ellipsis if needed. The stored message limit is 128 KiB of UTF-8. Optional source fields and the message may be null.

An event cursor belongs to the database's `stream_id`, which persists across restarts. Send the last response's `next_cursor` as `after` together with its `stream_id`. A different stream ID returns a page starting at zero with `reset_cursor: true`, even if the Relay URL still matches. A client upgrading an existing unscoped cache can send `cursor_event_id` with the UUID at its `after` value. The Relay resumes there if that UUID matches the sequence and resets the cursor otherwise. Requests with only `after` keep their existing behavior.

The reserved tool 'manual' accepts kind 'test'. Other tool IDs accept 'turn.completed'. Tool IDs and frontend IDs are 1–64 ASCII lowercase letters, digits, dots, underscores or hyphens. An optional frontend label is at most 128 UTF-8 bytes, must contain visible text and cannot contain control characters. Use the same tool and frontend IDs across devices for the same frontend. Omitting frontend preserves compatibility and identifies older or unlabelled sources.

## Phone inbox state

A relay can optionally set AINOTIFY_INBOX_TOKEN to permit inbox edits. Use a token distinct from the reader and all writer tokens. A read-only reader can fetch inbox state. Collector writers only ingest events.

- GET /v1/inbox/changes?after=0&limit=50 returns changes, next_cursor and can_write. Each change contains event_id, seq, folder and pinned. The inbox cursor is independent of the event feed cursor. Each response returns the latest state for each changed event in cursor order.
- POST /v1/inbox/actions accepts an operation UUID id, an event UUID event_id, and at least one of folder or pinned. Supported folders are inbox, archive and trash. Missing or null fields preserve the field's current state. A changed payload with a reused operation ID returns 409. A retry of the same action returns the event's current state without reapplying the patch. A missing event returns 404.
- Events start in inbox and unpinned. Setting trash keeps the event retrievable and reversible. Each field takes the value from the latest applied operation for that field. A read-only credential receives 403 for actions.

## Push orchestration

Relay orchestrates wakeups to registered devices upon receiving new events.

- Devices register their push token via `POST /v1/devices/register` with `{"token": "...", "device_type": "android", "app_id": "io.github.jerryskywalker.ainotify"}`.
- To revoke push wakeups, call `POST /v1/devices/unregister` with `{"token": "..."}`.
- Wakeup payload design: Push payloads contain only lightweight synchronization pointers (`event_id`, `seq`, `stream_id`). Full content is never relayed over untrusted third-party push channels; the client pulls full event details and message bodies directly from the self-hosted Relay over authenticated GET.
- If push fails or is delayed (e.g. Doze, network partition, token invalidation), clients recover automatically through the unified incremental pull protocol when foregrounded or reconnected.
