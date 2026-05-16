# RemoraHQ - Alert State

MeshCentral plugin providing server-side persistence for the RemoraHQ alert
overlay (acknowledge / resolve / snooze status + shared bookmarks).

## Identity

| Field | Value |
|-------|-------|
| Display name | `RemoraHQ - Alert State` |
| Short name (Mesh shortName) | `remoraAlertState` |
| Entry file | `remoraAlertState.js` |
| Source repo folder | `RemoraHQ-Alert-State-Plugins` |
| Deploy folder under Mesh | `meshcentral/plugins/remoraAlertState` |

## Storage

Single JSON file at `<datapath>/remora-alert-state.json`. Atomic write via
temp-file + rename. Auto-loaded on plugin startup; auto-saved on every mutation.

Schema:

```jsonc
{
  "state": {
    "<alertId>": {
      "status": "active" | "acknowledged" | "resolved" | "snoozed",
      "snoozedUntil"?: "<ISO timestamp>",
      "timeline": [
        { "kind": "acknowledged", "at": "...", "by"?: "user//...", "note"?: "..." }
      ]
    }
  },
  "bookmarks": ["<alertId>", ...]
}
```

`bookmarks` is a **shared** list — all admins see the same set.

## Wire protocol

All actions follow the same envelope. Client → server:

```jsonc
{
  "action": "plugin",
  "plugin": "remoraAlertState",
  "pluginaction": "<verb>",
  "tag": "<correlation-id>",
  "responseid": "<correlation-id>",
  "id"?: "<alertId>",
  "durationMinutes"?: 60,
  "by"?: "user//...",
  "note"?: "..."
}
```

Server reply echoes `tag`/`responseid` and adds `result: "ok"` plus payload.

| Action | Payload | Reply |
|--------|---------|-------|
| `list` | — | `{ state, bookmarks }` |
| `acknowledge` | `{ id, by?, note? }` | `{ record }` |
| `resolve` | `{ id, by?, note? }` | `{ record }` |
| `snooze` | `{ id, durationMinutes, by? }` | `{ record }` |
| `unacknowledge` | `{ id }` | `{ record }` |
| `bookmark` | `{ id }` | `{ bookmarks }` |
| `unbookmark` | `{ id }` | `{ bookmarks }` |

### Real-time broadcasts

Every mutation triggers a server-wide broadcast to all admins:

```jsonc
{
  "action": "plugin",
  "plugin": "remoraAlertState",
  "pluginaction": "changed",
  "state": { ... },
  "bookmarks": [ ... ]
}
```

Dispatched via `parent.parent.DispatchEvent(['*', 'server-users'], ...)`.
RemoraHQ clients subscribe to action `plugin` filtered by `plugin === 'remoraAlertState'`
and invalidate their alert-state query on receipt.

## Install (development)

```powershell
# from MeshCentral root
New-Item -ItemType SymbolicLink `
  -Path .\plugins\remoraAlertState `
  -Target "D:\…\RemoraHQ-Alert-State-Plugins"
```

Then register via `Admin → Plugins → Add` with the `configUrl` from `config.json`.

## License

Apache-2.0 (matches MeshCentral). See `LICENSE`.
