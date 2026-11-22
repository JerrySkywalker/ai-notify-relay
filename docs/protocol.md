# AINotify protocol v1

API paths are relative to the configured relay URL. The writer identity comes from the Bearer token. Reader credentials open the single owner's inbox.

| Method and path | Credentials | Behavior |
| --- | --- | --- |
| `GET /healthz` | None | Status without event data |
| `GET /v1/writer/status` | Writer | Returns its assigned `device_id` |
| `POST /v1/events` | Writer | Returns `201` for a new event or `200` for an identical retry by the same writer. A conflicting ID returns `409` |
| `GET /v1/events?after=0&limit=50` | Reader | Returns `events` and `next_cursor`. Pass the cursor to the next request to read later entries |
| `GET /v1/events/{id}` | Reader | Returns the event summary, session identifiers and complete stored reply |

```json
{
  "schema_version": 1,
  "id": "93d42e8e-8a1d-4b11-9322-4c282125a377",
  "tool": "codex",
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

A collector chooses the event ID once, persists the event, and reuses both for retries. The server assigns an increasing `seq` for delivery order. Event time is a source timestamp for display. List items include at most 240 Unicode scalar values of preview, with an ellipsis if needed. The stored message limit is 128 KiB of UTF-8. Optional source fields and the message may be null. The v1 tool and event pairs are `codex` and `claude-code` with `turn.completed`, and `manual` with `test`.
