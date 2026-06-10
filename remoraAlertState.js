/**
 * RemoraHQ - Alert State MeshCentral plugin.
 *
 * Server-side persistence for RemoraHQ alert overlays:
 *  - Acknowledge / Resolve / Snooze status per alertId (shared across admins).
 *  - Bookmarks list (shared — represents collective "watch list").
 *  - Timeline entries attached to each state record.
 *
 * Storage: single JSON file `<datapath>/remora-alert-state.json`. Atomic write
 * via temp-file + rename. The dataset is small (overlays only — raw alerts
 * stay in Mesh events), so synchronous JSON IO is acceptable.
 *
 * Real-time: every mutation broadcasts `{action:'plugin', plugin:'remoraAlertState',
 * pluginaction:'changed', ...}` to all server users via parent.parent.DispatchEvent.
 * Clients subscribe and invalidate their alert-state query on receipt.
 *
 * Wire protocol (RemoraHQ ↔ Mesh ↔ this plugin):
 *   client → server: { action:'plugin', plugin:'remoraAlertState',
 *                      pluginaction:'<verb>', tag, responseid, ...payload }
 *   server → client (reply): same envelope with result, tag echoed
 *   server → all admins (broadcast): { action:'plugin', plugin:'remoraAlertState',
 *                                       pluginaction:'changed', state, bookmarks }
 *
 * Actions:
 *   list          → { state: AlertStateMap, bookmarks: string[] }
 *   acknowledge   ← { id, note? }
 *   resolve       ← { id, note? }
 *   snooze        ← { id, durationMinutes }
 *   unacknowledge ← { id }
 *   bookmark      ← { id }
 *   unbookmark    ← { id }
 *
 * v0.2.0 (AUDIT-5 #35): every mutation requires an authenticated session user;
 * timeline attribution (`by`) is pinned to that server-side identity, never a
 * client field (was spoofable). `id`/`note` are length-bounded and the record
 * / timeline / bookmark counts are capped so the shared, broadcast state file
 * cannot be grown without bound.
 */

'use strict';

var path = require('path');
var fs = require('fs');

var PLUGIN_SHORT_NAME = 'remoraAlertState';
var PLUGIN_VERSION = '0.2.0';
// v0.2.0 (AUDIT-5 #35) — bounds against unbounded growth of the shared,
// broadcast-to-all-admins state file.
var ALERT_ID_MAX_LEN = 256;
var ALERT_NOTE_MAX_LEN = 512;
var ALERT_RECORDS_MAX = 5000;
var ALERT_TIMELINE_MAX = 100;

module.exports.remoraAlertState = function (parent) {
    var obj = {};
    obj.parent = parent;
    obj.meshServer = parent.parent;

    obj.exports = ['serveraction'];

    // ─── State ──────────────────────────────────────────────────────────────
    /** @type {Object<string, {status:string, snoozedUntil?:string, timeline:Array}>} */
    var stateMap = {};
    /** @type {string[]} */
    var bookmarks = [];
    var storePath = null;
    var writeQueue = Promise.resolve();

    function resolveStorePath() {
        // parent.parent is the MeshCentral mesh-server (`obj.parent` in mesh-server.js).
        // It exposes `datapath` (default: meshcentral-data folder).
        var datapath = (obj.meshServer && obj.meshServer.datapath) || process.cwd();
        return path.join(datapath, 'remora-alert-state.json');
    }

    function loadFromDisk() {
        try {
            if (!storePath) storePath = resolveStorePath();
            if (!fs.existsSync(storePath)) return;
            var raw = fs.readFileSync(storePath, 'utf8');
            var parsed = JSON.parse(raw);
            if (parsed && typeof parsed === 'object') {
                stateMap = (parsed.state && typeof parsed.state === 'object') ? parsed.state : {};
                bookmarks = Array.isArray(parsed.bookmarks) ? parsed.bookmarks.filter(function (id) { return typeof id === 'string'; }) : [];
            }
        } catch (e) {
            console.log('[remoraAlertState] load failed:', e.message);
        }
    }

    function persist() {
        // Serialize writes so a slow disk does not produce torn JSON when
        // mutations land back-to-back.
        var snapshot = JSON.stringify({ state: stateMap, bookmarks: bookmarks });
        writeQueue = writeQueue.then(function () {
            return new Promise(function (resolve) {
                if (!storePath) storePath = resolveStorePath();
                var tmp = storePath + '.tmp';
                fs.writeFile(tmp, snapshot, 'utf8', function (err) {
                    if (err) {
                        console.log('[remoraAlertState] tmp write failed:', err.message);
                        return resolve();
                    }
                    fs.rename(tmp, storePath, function (err2) {
                        if (err2) console.log('[remoraAlertState] rename failed:', err2.message);
                        resolve();
                    });
                });
            });
        });
        return writeQueue;
    }

    function broadcast() {
        try {
            if (!obj.meshServer || typeof obj.meshServer.DispatchEvent !== 'function') return;
            obj.meshServer.DispatchEvent(['*', 'server-users'], obj, {
                action: 'plugin',
                plugin: PLUGIN_SHORT_NAME,
                pluginaction: 'changed',
                etype: 'plugin',
                nolog: 1,
                state: stateMap,
                bookmarks: bookmarks
            });
        } catch (e) {
            console.log('[remoraAlertState] broadcast failed:', e.message);
        }
    }

    // Returns the record, or null when a NEW record would exceed the cap (so a
    // caller cannot grow stateMap without bound). Existing records always resolve.
    function ensureRecord(id) {
        if (!stateMap[id]) {
            if (Object.keys(stateMap).length >= ALERT_RECORDS_MAX) return null;
            stateMap[id] = { status: 'active', timeline: [] };
        }
        if (!Array.isArray(stateMap[id].timeline)) stateMap[id].timeline = [];
        return stateMap[id];
    }

    // Append a timeline entry, capping per-record history at ALERT_TIMELINE_MAX.
    function pushTimeline(rec, kind, by, note) {
        var entry = { kind: String(kind), at: new Date().toISOString() };
        if (by) entry.by = String(by);
        if (note) entry.note = String(note).slice(0, ALERT_NOTE_MAX_LEN);
        rec.timeline.push(entry);
        if (rec.timeline.length > ALERT_TIMELINE_MAX) {
            rec.timeline = rec.timeline.slice(-ALERT_TIMELINE_MAX);
        }
        return entry;
    }

    function reply(session, command, payload) {
        var body = Object.assign({
            action: 'plugin',
            plugin: PLUGIN_SHORT_NAME,
            pluginaction: command.pluginaction,
            tag: command.tag,
            responseid: command.responseid || command.tag,
            result: 'ok'
        }, payload || {});
        try { session.send(body); } catch (e) { /* ignore */ }
    }

    function replyError(session, command, error) {
        try {
            session.send({
                action: 'plugin',
                plugin: PLUGIN_SHORT_NAME,
                pluginaction: command.pluginaction || 'unknown',
                tag: command.tag,
                responseid: command.responseid || command.tag,
                result: 'error',
                error: String(error || 'remora_alert_state_failed')
            });
        } catch (e) { /* ignore */ }
    }

    // ─── Lifecycle ──────────────────────────────────────────────────────────
    obj.server_startup = function () {
        loadFromDisk();
        console.log('[remoraAlertState] v' + PLUGIN_VERSION + ' loaded. Storage: ' + (storePath || '(uninitialised)'));
        console.log('[remoraAlertState] tracking ' + Object.keys(stateMap).length + ' alert(s), ' + bookmarks.length + ' bookmark(s).');
    };

    // ─── Action dispatcher ──────────────────────────────────────────────────
    obj.serveraction = function (command, dbGet, ws) {
        var session = dbGet || ws;
        if (!session || typeof session.send !== 'function') return;

        var action = String(command.pluginaction || '');
        var id = (command.id != null) ? String(command.id) : null;
        // v0.2.0 (AUDIT-5 #35). Attribution is pinned to the server-authenticated
        // session user — NEVER the client `by`/`userid`, which could be set to
        // any other user's id. `list` is a read; every mutation requires an
        // authenticated actor and a bounded id.
        var actor = session && session.user;
        var by = (actor && actor._id) ? String(actor._id) : null;

        try {
            switch (action) {
                case 'list': {
                    reply(session, command, { state: stateMap, bookmarks: bookmarks });
                    return;
                }
                case 'acknowledge': {
                    if (!id) return replyError(session, command, 'missing_id');
                    if (id.length > ALERT_ID_MAX_LEN) return replyError(session, command, 'invalid_id');
                    if (!by) return replyError(session, command, 'auth_required');
                    var rec = ensureRecord(id);
                    if (!rec) return replyError(session, command, 'capacity_exceeded');
                    rec.status = 'acknowledged';
                    delete rec.snoozedUntil;
                    pushTimeline(rec, 'acknowledged', by, command.note);
                    persist();
                    broadcast();
                    reply(session, command, { record: rec });
                    return;
                }
                case 'resolve': {
                    if (!id) return replyError(session, command, 'missing_id');
                    if (id.length > ALERT_ID_MAX_LEN) return replyError(session, command, 'invalid_id');
                    if (!by) return replyError(session, command, 'auth_required');
                    var rec2 = ensureRecord(id);
                    if (!rec2) return replyError(session, command, 'capacity_exceeded');
                    rec2.status = 'resolved';
                    delete rec2.snoozedUntil;
                    pushTimeline(rec2, 'resolved', by, command.note);
                    persist();
                    broadcast();
                    reply(session, command, { record: rec2 });
                    return;
                }
                case 'snooze': {
                    if (!id) return replyError(session, command, 'missing_id');
                    if (id.length > ALERT_ID_MAX_LEN) return replyError(session, command, 'invalid_id');
                    if (!by) return replyError(session, command, 'auth_required');
                    var minutes = Number(command.durationMinutes);
                    if (!isFinite(minutes) || minutes <= 0) return replyError(session, command, 'invalid_duration');
                    var rec3 = ensureRecord(id);
                    if (!rec3) return replyError(session, command, 'capacity_exceeded');
                    rec3.status = 'snoozed';
                    rec3.snoozedUntil = new Date(Date.now() + minutes * 60000).toISOString();
                    pushTimeline(rec3, 'snoozed:' + minutes + 'm', by);
                    persist();
                    broadcast();
                    reply(session, command, { record: rec3 });
                    return;
                }
                case 'unacknowledge': {
                    if (!id) return replyError(session, command, 'missing_id');
                    if (id.length > ALERT_ID_MAX_LEN) return replyError(session, command, 'invalid_id');
                    if (!by) return replyError(session, command, 'auth_required');
                    if (stateMap[id]) {
                        stateMap[id].status = 'active';
                        delete stateMap[id].snoozedUntil;
                        if (!Array.isArray(stateMap[id].timeline)) stateMap[id].timeline = [];
                        pushTimeline(stateMap[id], 'reopened', by);
                        persist();
                        broadcast();
                    }
                    reply(session, command, { record: stateMap[id] || null });
                    return;
                }
                case 'bookmark': {
                    if (!id) return replyError(session, command, 'missing_id');
                    if (id.length > ALERT_ID_MAX_LEN) return replyError(session, command, 'invalid_id');
                    if (!by) return replyError(session, command, 'auth_required');
                    if (bookmarks.indexOf(id) === -1) {
                        if (bookmarks.length >= ALERT_RECORDS_MAX) return replyError(session, command, 'capacity_exceeded');
                        bookmarks.push(id);
                        persist();
                        broadcast();
                    }
                    reply(session, command, { bookmarks: bookmarks });
                    return;
                }
                case 'unbookmark': {
                    if (!id) return replyError(session, command, 'missing_id');
                    if (id.length > ALERT_ID_MAX_LEN) return replyError(session, command, 'invalid_id');
                    if (!by) return replyError(session, command, 'auth_required');
                    var ix = bookmarks.indexOf(id);
                    if (ix !== -1) {
                        bookmarks.splice(ix, 1);
                        persist();
                        broadcast();
                    }
                    reply(session, command, { bookmarks: bookmarks });
                    return;
                }
                default: {
                    return replyError(session, command, 'unknown_pluginaction');
                }
            }
        } catch (e) {
            console.log('[remoraAlertState] action error:', e.message);
            replyError(session, command, 'internal_error');
        }
    };

    return obj;
};
