package state

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xmm2022/emby-range-cache-proxy/go/internal/model"
	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

type PlaybackSessionUpdate struct {
	SessionHash     string
	DeviceHash      string
	ItemID          string
	MediaSourceID   string
	CacheKey        string
	OriginSignature string
	MediaSize       int64
	ByteRange       model.ByteRange
	ObservedAt      float64
}

type PlaybackSessionRecord struct {
	SessionHash        string
	DeviceHash         string
	ItemID             string
	MediaSourceID      string
	CacheKey           string
	OriginSignature    string
	MediaSize          int64
	LastRangeStart     int64
	LastRangeEnd       int64
	MaxObservedOffset  int64
	FirstSeenAt        float64
	LastSeenAt         float64
	LastEmbyObservedAt sql.NullFloat64
	Status             string
	QueuedUntil        sql.NullInt64
}

type PrefetchTaskRecord struct {
	ID             int64
	ItemID         string
	MediaSourceID  string
	CacheKey       string
	Start          int64
	End            int64
	Priority       int
	Status         string
	Attempts       int
	CreatedAt      float64
	UpdatedAt      float64
	LastErrorClass sql.NullString
	NextAttemptAt  sql.NullFloat64
}

type MiddleBlockRecord struct {
	CacheKey     string
	Start        int64
	End          int64
	Path         string
	Size         int64
	CreatedAt    float64
	LastAccessAt float64
	ExpiresAt    float64
}

type SourceMetadataRecord struct {
	ItemID          string
	MediaSourceID   string
	CacheKey        string
	OriginURL       string
	OriginSignature string
	MediaSize       int64
	UpdatedAt       float64
}

func HashIdentifier(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func Open(path string) (*Store, error) {
	if err := ensureParent(path); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &Store{db: db}
	for _, pragma := range []string{
		`PRAGMA busy_timeout=5000`,
		`PRAGMA journal_mode=WAL`,
		`PRAGMA synchronous=NORMAL`,
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, err
		}
	}
	if err := store.initSchema(); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) initSchema() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS playback_sessions (
    session_hash TEXT PRIMARY KEY,
    device_hash TEXT,
    item_id TEXT NOT NULL,
    media_source_id TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    origin_signature TEXT NOT NULL,
    media_size INTEGER NOT NULL,
    last_range_start INTEGER NOT NULL,
    last_range_end INTEGER NOT NULL,
    max_observed_offset INTEGER NOT NULL,
    first_seen_at REAL NOT NULL,
    last_seen_at REAL NOT NULL,
    last_emby_observed_at REAL,
    status TEXT NOT NULL,
    queued_until INTEGER
);
CREATE TABLE IF NOT EXISTS prefetch_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id TEXT NOT NULL,
    media_source_id TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    last_error_class TEXT,
    next_attempt_at REAL,
    UNIQUE(cache_key, start, end)
);
CREATE TABLE IF NOT EXISTS middle_blocks (
    cache_key TEXT NOT NULL,
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    path TEXT NOT NULL,
    size INTEGER NOT NULL,
    created_at REAL NOT NULL,
    last_access_at REAL NOT NULL,
    expires_at REAL NOT NULL,
    PRIMARY KEY(cache_key, start, end)
);
CREATE TABLE IF NOT EXISTS source_metadata (
    item_id TEXT NOT NULL,
    media_source_id TEXT NOT NULL,
    cache_key TEXT NOT NULL,
    origin_url TEXT NOT NULL,
    origin_signature TEXT NOT NULL,
    media_size INTEGER NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY(item_id, media_source_id, cache_key)
);
`)
	if err != nil {
		return err
	}
	if err := s.ensureColumn("prefetch_tasks", "next_attempt_at", "REAL"); err != nil {
		return err
	}
	_, err = s.db.Exec(`
UPDATE prefetch_tasks
SET next_attempt_at = updated_at
WHERE status IN ('failed', 'skipped')
  AND next_attempt_at IS NULL
  AND (
        last_error_class IS NULL
     OR last_error_class NOT IN ('PermanentError', 'PrefetchSourceMismatch', 'RangeTooLarge')
  )
`)
	return err
}

func (s *Store) ensureColumn(tableName, columnName, columnType string) error {
	rows, err := s.db.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableName))
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var defaultValue any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			return err
		}
		if name == columnName {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = s.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", tableName, columnName, columnType))
	return err
}

func (s *Store) RecordPlayback(update PlaybackSessionUpdate) error {
	_, err := s.db.Exec(`
INSERT INTO playback_sessions (
    session_hash, device_hash, item_id, media_source_id,
    cache_key, origin_signature, media_size, last_range_start,
    last_range_end, max_observed_offset, first_seen_at,
    last_seen_at, last_emby_observed_at, status, queued_until
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'active', NULL)
ON CONFLICT(session_hash) DO UPDATE SET
    device_hash = excluded.device_hash,
    item_id = excluded.item_id,
    media_source_id = excluded.media_source_id,
    cache_key = excluded.cache_key,
    origin_signature = excluded.origin_signature,
    media_size = excluded.media_size,
    last_range_start = excluded.last_range_start,
    last_range_end = excluded.last_range_end,
    max_observed_offset = MAX(playback_sessions.max_observed_offset, excluded.max_observed_offset),
    last_seen_at = excluded.last_seen_at,
    status = 'active'
`, update.SessionHash, nullableString(update.DeviceHash), update.ItemID, update.MediaSourceID, update.CacheKey,
		update.OriginSignature, update.MediaSize, update.ByteRange.Start, update.ByteRange.End,
		update.ByteRange.End, update.ObservedAt, update.ObservedAt)
	return err
}

func (s *Store) GetSession(sessionHash string) (*PlaybackSessionRecord, error) {
	row := s.db.QueryRow(`SELECT * FROM playback_sessions WHERE session_hash = ?`, sessionHash)
	return scanSession(row)
}

func (s *Store) MarkIdleSessions(now float64, idleSeconds int) ([]PlaybackSessionRecord, error) {
	cutoff := now - float64(idleSeconds)
	rows, err := s.db.Query(`
SELECT * FROM playback_sessions
WHERE status = 'active' AND last_seen_at <= ?
ORDER BY last_seen_at ASC, session_hash ASC`, cutoff)
	if err != nil {
		return nil, err
	}
	sessions, err := scanSessions(rows)
	if err != nil {
		return nil, err
	}
	out := make([]PlaybackSessionRecord, 0, len(sessions))
	for _, session := range sessions {
		result, err := s.db.Exec(`
UPDATE playback_sessions
SET status = 'idle'
WHERE session_hash = ? AND status = 'active' AND last_seen_at <= ?`, session.SessionHash, cutoff)
		if err != nil {
			return nil, err
		}
		if changed, _ := result.RowsAffected(); changed > 0 {
			session.Status = "idle"
			out = append(out, session)
		}
	}
	return out, nil
}

func (s *Store) RecentActiveSessions(now float64, activeSeconds int) ([]PlaybackSessionRecord, error) {
	rows, err := s.db.Query(`
SELECT * FROM playback_sessions
WHERE status = 'active' AND last_seen_at >= ?
ORDER BY last_seen_at DESC, session_hash ASC`, now-float64(activeSeconds))
	if err != nil {
		return nil, err
	}
	return scanSessions(rows)
}

func (s *Store) ExpireOldSessions(now float64, expireSeconds int) (int64, error) {
	result, err := s.db.Exec(`
UPDATE playback_sessions
SET status = 'expired'
WHERE status != 'expired' AND last_seen_at <= ?`, now-float64(expireSeconds))
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *Store) RecordObservedSessions(sessionHashes map[string]struct{}, observedAt float64) error {
	for sessionHash := range sessionHashes {
		if _, err := s.db.Exec(`
UPDATE playback_sessions SET last_emby_observed_at = ? WHERE session_hash = ?`, observedAt, sessionHash); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) MarkMissingObservedSessionsStopped(now float64, stopGraceSeconds int) ([]PlaybackSessionRecord, error) {
	cutoff := now - float64(stopGraceSeconds)
	rows, err := s.db.Query(`
SELECT * FROM playback_sessions
WHERE last_emby_observed_at IS NOT NULL
  AND status IN ('active', 'idle')
  AND last_emby_observed_at <= ?
ORDER BY last_emby_observed_at ASC, session_hash ASC`, cutoff)
	if err != nil {
		return nil, err
	}
	sessions, err := scanSessions(rows)
	if err != nil {
		return nil, err
	}
	out := make([]PlaybackSessionRecord, 0, len(sessions))
	for _, session := range sessions {
		result, err := s.db.Exec(`
UPDATE playback_sessions
SET status = 'stopped'
WHERE session_hash = ?
  AND last_emby_observed_at IS NOT NULL
  AND status IN ('active', 'idle')
  AND last_emby_observed_at <= ?`, session.SessionHash, cutoff)
		if err != nil {
			return nil, err
		}
		if changed, _ := result.RowsAffected(); changed > 0 {
			session.Status = "stopped"
			out = append(out, session)
		}
	}
	return out, nil
}

func (s *Store) PrefetchCandidateSessions() ([]PlaybackSessionRecord, error) {
	rows, err := s.db.Query(`
SELECT * FROM playback_sessions
WHERE status IN ('idle', 'stopped')
ORDER BY last_seen_at ASC, session_hash ASC`)
	if err != nil {
		return nil, err
	}
	return scanSessions(rows)
}

func (s *Store) UpdateSessionQueuedUntil(sessionHash string, queuedUntil int64, now float64) error {
	_, err := s.db.Exec(`
UPDATE playback_sessions
SET queued_until = CASE
    WHEN queued_until IS NULL OR queued_until < ? THEN ?
    ELSE queued_until
END
WHERE session_hash = ? AND status != 'expired'`, queuedUntil, queuedUntil, sessionHash)
	return err
}

func (s *Store) UpsertSourceMetadata(itemID, mediaSourceID, cacheKey, originURL, originSignature string, mediaSize int64, updatedAt float64) error {
	_, err := s.db.Exec(`
INSERT INTO source_metadata (
    item_id, media_source_id, cache_key, origin_url,
    origin_signature, media_size, updated_at
)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(item_id, media_source_id, cache_key) DO UPDATE SET
    origin_url = excluded.origin_url,
    origin_signature = excluded.origin_signature,
    media_size = excluded.media_size,
    updated_at = excluded.updated_at
`, itemID, mediaSourceID, cacheKey, originURL, originSignature, mediaSize, updatedAt)
	return err
}

func (s *Store) GetSourceMetadata(itemID, mediaSourceID, cacheKey string) (*SourceMetadataRecord, error) {
	row := s.db.QueryRow(`
SELECT item_id, media_source_id, cache_key, origin_url, origin_signature, media_size, updated_at
FROM source_metadata
WHERE item_id = ? AND media_source_id = ? AND cache_key = ?`, itemID, mediaSourceID, cacheKey)
	var record SourceMetadataRecord
	err := row.Scan(&record.ItemID, &record.MediaSourceID, &record.CacheKey, &record.OriginURL, &record.OriginSignature, &record.MediaSize, &record.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &record, err
}

func (s *Store) DeleteSourceMetadataOlderThan(cutoff float64) (int64, error) {
	result, err := s.db.Exec(`DELETE FROM source_metadata WHERE updated_at < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (s *Store) EnqueuePrefetchTask(itemID, mediaSourceID, cacheKey string, start, end int64, priority int, now float64, maxQueueDepth int) (*PrefetchTaskRecord, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var queued int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM prefetch_tasks WHERE status = 'queued'`).Scan(&queued); err != nil {
		return nil, err
	}
	if queued >= maxQueueDepth {
		return nil, tx.Commit()
	}
	result, err := tx.Exec(`
INSERT OR IGNORE INTO prefetch_tasks (
    item_id, media_source_id, cache_key, start, end,
    priority, status, attempts, created_at, updated_at,
    last_error_class, next_attempt_at
)
VALUES (?, ?, ?, ?, ?, ?, 'queued', 0, ?, ?, NULL, NULL)
`, itemID, mediaSourceID, cacheKey, start, end, priority, now, now)
	if err != nil {
		return nil, err
	}
	changed, _ := result.RowsAffected()
	if changed == 0 {
		return nil, tx.Commit()
	}
	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}
	row := tx.QueryRow(`SELECT * FROM prefetch_tasks WHERE id = ?`, id)
	record, err := scanPrefetchTask(row)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return record, nil
}

func (s *Store) PrefetchTaskExists(cacheKey string, start, end int64) (bool, error) {
	var one int
	err := s.db.QueryRow(`
SELECT 1 FROM prefetch_tasks
WHERE cache_key = ? AND start = ? AND end = ?
  AND (
        status IN ('queued', 'running')
     OR (status IN ('failed', 'skipped') AND next_attempt_at IS NOT NULL)
  )
LIMIT 1`, cacheKey, start, end).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func (s *Store) ReusablePrefetchRanges(cacheKey string, byteRange model.ByteRange) ([]model.ByteRange, error) {
	rows, err := s.db.Query(`
SELECT start, end FROM middle_blocks
WHERE cache_key = ? AND start <= ? AND end >= ?
UNION ALL
SELECT start, end FROM prefetch_tasks
WHERE cache_key = ? AND start <= ? AND end >= ?
  AND (
        status IN ('queued', 'running')
     OR (status IN ('failed', 'skipped') AND next_attempt_at IS NOT NULL)
  )
ORDER BY start ASC, end ASC
`, cacheKey, byteRange.End, byteRange.Start, cacheKey, byteRange.End, byteRange.Start)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.ByteRange
	for rows.Next() {
		var start, end int64
		if err := rows.Scan(&start, &end); err != nil {
			return nil, err
		}
		out = append(out, model.ByteRange{Start: start, End: end})
	}
	return out, rows.Err()
}

func (s *Store) ClaimPrefetchTasks(limit int, now float64, runningStaleSeconds int) ([]PrefetchTaskRecord, error) {
	if limit <= 0 {
		return nil, nil
	}
	staleCutoff := now - float64(runningStaleSeconds)
	rows, err := s.db.Query(`
SELECT * FROM prefetch_tasks
WHERE status = 'queued'
   OR (status IN ('failed', 'skipped') AND next_attempt_at IS NOT NULL AND next_attempt_at <= ?)
   OR (? > 0 AND status = 'running' AND updated_at <= ?)
ORDER BY priority DESC, created_at ASC, id ASC
LIMIT ?`, now, runningStaleSeconds, staleCutoff, limit)
	if err != nil {
		return nil, err
	}
	candidates, err := scanPrefetchTasks(rows)
	if err != nil {
		return nil, err
	}
	out := make([]PrefetchTaskRecord, 0, len(candidates))
	for _, task := range candidates {
		result, err := s.db.Exec(`
UPDATE prefetch_tasks
SET status = 'running',
    attempts = attempts + 1,
    updated_at = ?,
    next_attempt_at = NULL
WHERE id = ?
  AND (
        status = 'queued'
     OR (status IN ('failed', 'skipped') AND next_attempt_at IS NOT NULL AND next_attempt_at <= ?)
     OR (? > 0 AND status = 'running' AND updated_at <= ?)
  )`, now, task.ID, now, runningStaleSeconds, staleCutoff)
		if err != nil {
			return nil, err
		}
		if changed, _ := result.RowsAffected(); changed == 0 {
			continue
		}
		updated, err := s.getPrefetchTask(task.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, *updated)
	}
	return out, nil
}

func (s *Store) ClaimablePrefetchTaskCount(now float64, runningStaleSeconds int) (int, error) {
	staleCutoff := now - float64(runningStaleSeconds)
	var count int
	err := s.db.QueryRow(`
SELECT COUNT(*) FROM prefetch_tasks
WHERE status = 'queued'
   OR (status IN ('failed', 'skipped') AND next_attempt_at IS NOT NULL AND next_attempt_at <= ?)
   OR (? > 0 AND status = 'running' AND updated_at <= ?)`, now, runningStaleSeconds, staleCutoff).Scan(&count)
	return count, err
}

func (s *Store) QueueDepth() (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM prefetch_tasks WHERE status = 'queued'`).Scan(&count)
	return count, err
}

func (s *Store) CompletePrefetchTask(taskID int64, now float64, expectedAttempts int) error {
	_, err := s.db.Exec(`
UPDATE prefetch_tasks
SET status = 'done', updated_at = ?, last_error_class = NULL, next_attempt_at = NULL
WHERE id = ? AND status = 'running' AND attempts = ?`, now, taskID, expectedAttempts)
	return err
}

func (s *Store) FailPrefetchTask(taskID int64, errorClass string, now float64, retryAfterSeconds int, expectedAttempts int) error {
	var next any
	if retryAfterSeconds > 0 {
		next = now + float64(retryAfterSeconds)
	}
	_, err := s.db.Exec(`
UPDATE prefetch_tasks
SET status = 'failed', updated_at = ?, last_error_class = ?, next_attempt_at = ?
WHERE id = ? AND status = 'running' AND attempts = ?`, now, errorClass, next, taskID, expectedAttempts)
	return err
}

func (s *Store) SkipPrefetchTask(taskID int64, errorClass string, now float64, retryAfterSeconds int, expectedAttempts int) error {
	var next any
	if retryAfterSeconds > 0 {
		next = now + float64(retryAfterSeconds)
	}
	_, err := s.db.Exec(`
UPDATE prefetch_tasks
SET status = 'skipped', updated_at = ?, last_error_class = ?, next_attempt_at = ?
WHERE id = ? AND status = 'running' AND attempts = ?`, now, errorClass, next, taskID, expectedAttempts)
	return err
}

func (s *Store) RequeuePrefetchTask(taskID int64, now float64, errorClass string, expectedAttempts int) error {
	_, err := s.db.Exec(`
UPDATE prefetch_tasks
SET status = 'queued', updated_at = ?, last_error_class = ?, next_attempt_at = NULL
WHERE id = ? AND status = 'running' AND attempts = ?`, now, nullableString(errorClass), taskID, expectedAttempts)
	return err
}

func (s *Store) UpsertMiddleBlock(block MiddleBlockRecord) error {
	_, err := s.db.Exec(`
INSERT INTO middle_blocks (
    cache_key, start, end, path, size, created_at, last_access_at, expires_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(cache_key, start, end) DO UPDATE SET
    path = excluded.path,
    size = excluded.size,
    created_at = excluded.created_at,
    last_access_at = excluded.last_access_at,
    expires_at = excluded.expires_at
`, block.CacheKey, block.Start, block.End, block.Path, block.Size, block.CreatedAt, block.LastAccessAt, block.ExpiresAt)
	return err
}

func (s *Store) FindMiddleBlock(cacheKey string, byteRange model.ByteRange) (*MiddleBlockRecord, error) {
	row := s.db.QueryRow(`
SELECT cache_key, start, end, path, size, created_at, last_access_at, expires_at
FROM middle_blocks
WHERE cache_key = ? AND start <= ? AND end >= ?
ORDER BY start DESC, end ASC
LIMIT 1`, cacheKey, byteRange.Start, byteRange.End)
	return scanMiddleBlock(row)
}

func (s *Store) TouchMiddleBlock(cacheKey string, start, end int64, now float64, ttlSeconds int) error {
	_, err := s.db.Exec(`
UPDATE middle_blocks
SET last_access_at = ?, expires_at = ?
WHERE cache_key = ? AND start = ? AND end = ?`, now, now+float64(ttlSeconds), cacheKey, start, end)
	return err
}

func (s *Store) ExpiredMiddleBlocks(now float64) ([]MiddleBlockRecord, error) {
	rows, err := s.db.Query(`
SELECT cache_key, start, end, path, size, created_at, last_access_at, expires_at
FROM middle_blocks
WHERE expires_at <= ?
ORDER BY expires_at ASC, cache_key ASC, start ASC, end ASC`, now)
	if err != nil {
		return nil, err
	}
	return scanMiddleBlocks(rows)
}

func (s *Store) LeastRecentMiddleBlocks() ([]MiddleBlockRecord, error) {
	rows, err := s.db.Query(`
SELECT cache_key, start, end, path, size, created_at, last_access_at, expires_at
FROM middle_blocks
ORDER BY last_access_at ASC, cache_key ASC, start ASC, end ASC`)
	if err != nil {
		return nil, err
	}
	return scanMiddleBlocks(rows)
}

func (s *Store) DeleteMiddleBlockRecord(cacheKey string, start, end int64) error {
	_, err := s.db.Exec(`DELETE FROM middle_blocks WHERE cache_key = ? AND start = ? AND end = ?`, cacheKey, start, end)
	return err
}

func (s *Store) MiddleCacheBytes() (int64, error) {
	var total int64
	err := s.db.QueryRow(`SELECT COALESCE(SUM(size), 0) FROM middle_blocks`).Scan(&total)
	return total, err
}

func (s *Store) PublishMiddleBlockAndCompletePrefetchTask(taskID int64, expectedAttempts int, block MiddleBlockRecord, now float64, publish func() error) (bool, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var one int
	err = tx.QueryRow(`SELECT 1 FROM prefetch_tasks WHERE id = ? AND status = 'running' AND attempts = ?`, taskID, expectedAttempts).Scan(&one)
	if err == sql.ErrNoRows {
		return false, tx.Commit()
	}
	if err != nil {
		return false, err
	}
	if err := publish(); err != nil {
		return false, err
	}
	if _, err := tx.Exec(`
INSERT INTO middle_blocks (
    cache_key, start, end, path, size, created_at, last_access_at, expires_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(cache_key, start, end) DO UPDATE SET
    path = excluded.path,
    size = excluded.size,
    created_at = excluded.created_at,
    last_access_at = excluded.last_access_at,
    expires_at = excluded.expires_at
`, block.CacheKey, block.Start, block.End, block.Path, block.Size, block.CreatedAt, block.LastAccessAt, block.ExpiresAt); err != nil {
		return false, err
	}
	result, err := tx.Exec(`
UPDATE prefetch_tasks
SET status = 'done', updated_at = ?, last_error_class = NULL, next_attempt_at = NULL
WHERE id = ? AND status = 'running' AND attempts = ?`, now, taskID, expectedAttempts)
	if err != nil {
		return false, err
	}
	changed, _ := result.RowsAffected()
	if changed == 0 {
		return false, nil
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) getPrefetchTask(id int64) (*PrefetchTaskRecord, error) {
	return scanPrefetchTask(s.db.QueryRow(`SELECT * FROM prefetch_tasks WHERE id = ?`, id))
}

type scanner interface {
	Scan(dest ...any) error
}

func scanSession(row scanner) (*PlaybackSessionRecord, error) {
	var record PlaybackSessionRecord
	var deviceHash sql.NullString
	err := row.Scan(&record.SessionHash, &deviceHash, &record.ItemID, &record.MediaSourceID,
		&record.CacheKey, &record.OriginSignature, &record.MediaSize, &record.LastRangeStart,
		&record.LastRangeEnd, &record.MaxObservedOffset, &record.FirstSeenAt, &record.LastSeenAt,
		&record.LastEmbyObservedAt, &record.Status, &record.QueuedUntil)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if deviceHash.Valid {
		record.DeviceHash = deviceHash.String
	}
	return &record, err
}

func scanSessions(rows *sql.Rows) ([]PlaybackSessionRecord, error) {
	defer rows.Close()
	var out []PlaybackSessionRecord
	for rows.Next() {
		record, err := scanSession(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *record)
	}
	return out, rows.Err()
}

func scanPrefetchTask(row scanner) (*PrefetchTaskRecord, error) {
	var record PrefetchTaskRecord
	err := row.Scan(&record.ID, &record.ItemID, &record.MediaSourceID, &record.CacheKey,
		&record.Start, &record.End, &record.Priority, &record.Status, &record.Attempts,
		&record.CreatedAt, &record.UpdatedAt, &record.LastErrorClass, &record.NextAttemptAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &record, err
}

func scanPrefetchTasks(rows *sql.Rows) ([]PrefetchTaskRecord, error) {
	defer rows.Close()
	var out []PrefetchTaskRecord
	for rows.Next() {
		record, err := scanPrefetchTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *record)
	}
	return out, rows.Err()
}

func scanMiddleBlock(row scanner) (*MiddleBlockRecord, error) {
	var record MiddleBlockRecord
	err := row.Scan(&record.CacheKey, &record.Start, &record.End, &record.Path, &record.Size,
		&record.CreatedAt, &record.LastAccessAt, &record.ExpiresAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &record, err
}

func scanMiddleBlocks(rows *sql.Rows) ([]MiddleBlockRecord, error) {
	defer rows.Close()
	var out []MiddleBlockRecord
	for rows.Next() {
		record, err := scanMiddleBlock(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *record)
	}
	return out, rows.Err()
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func ensureParent(path string) error {
	dir := filepath.Dir(path)
	if dir == "." || dir == "" {
		return nil
	}
	return mkdirAll(dir)
}

func mkdirAll(path string) error {
	return os.MkdirAll(path, 0o755)
}
