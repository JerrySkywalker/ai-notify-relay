# AINotify protocol v1

API paths are relative to the configured relay URL. The writer identity comes from the Bearer token. Reader and inbox credentials open the single owner's inbox.

| Method and path | Credentials | Behavior |
| --- | --- | --- |
| `GET /healthz` | None | Status without event data |
| `GET /v1/writer/status` | Writer | Returns its assigned `device_id` |
| `POST /v1/events` | Writer | Returns `201` for a new event or `200` for an identical retry by the same writer. A conflicting ID returns `409` |
| `GET /v1/events?after=0&limit=50` | Reader or inbox | Returns `events` and `next_cursor`. Pass the cursor to the next request to read later entries |
| `GET /v1/events/{id}` | Reader or inbox | Returns the event summary, session identifiers and complete stored reply |

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

The reserved tool 'manual' accepts kind 'test'. Other tool IDs accept 'turn.completed'. Tool IDs and frontend IDs are 1–64 ASCII lowercase letters, digits, dots, underscores or hyphens. An optional frontend label is at most 128 UTF-8 bytes, must contain visible text and cannot contain control characters. Use the same tool and frontend IDs across devices for the same frontend. Omitting frontend preserves compatibility and identifies older or unlabelled sources.

## Phone inbox state

A relay can optionally set AINOTIFY_INBOX_TOKEN to permit inbox edits. Use a token distinct from the reader and all writer tokens. A read-only reader can fetch inbox state. Collector writers only ingest events.

- GET /v1/inbox/changes?after=0&limit=50 returns changes, next_cursor and can_write. Each change contains event_id, seq, folder and pinned. The inbox cursor is independent of the event feed cursor. Each response returns the latest state for each changed event in cursor order.
- POST /v1/inbox/actions accepts an operation UUID id, an event UUID event_id, and at least one of folder or pinned. Supported folders are inbox, archive and trash. Missing or null fields preserve the field's current state. A changed payload with a reused operation ID returns 409. A retry of the same action returns the event's current state without reapplying the patch. A missing event returns 404.
- Events start in inbox and unpinned. Setting trash keeps the event retrievable and reversible. Each field takes the value from the latest applied operation for that field. A read-only credential receives 403 for actions.
