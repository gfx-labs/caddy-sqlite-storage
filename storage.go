package storagesqlite

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	_ "modernc.org/sqlite"
)

type SqliteStorage struct {
	QueryTimeout time.Duration `json:"query_timeout,omitempty"`
	LockTimeout  time.Duration `json:"lock_timeout,omitempty"`
	Dsn          string        `json:"dsn,omitempty"`
	Database     *sql.DB       `json:"-"`
}

func init() {
	caddy.RegisterModule(SqliteStorage{})
}

func (c *SqliteStorage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		var value string
		key := d.Val()
		if !d.Args(&value) {
			continue
		}
		switch key {
		case "query_timeout":
			QueryTimeout, err := strconv.Atoi(value)
			if err == nil {
				c.QueryTimeout = time.Duration(QueryTimeout)
			}
		case "lock_timeout":
			LockTimeout, err := strconv.Atoi(value)
			if err == nil {
				c.LockTimeout = time.Duration(LockTimeout)
			}
		case "dsn":
			c.Dsn = value
		}
	}
	caddy.Log().Named("storage.sqlite").Debug(fmt.Sprintf("UnmarshalCaddyfile %v", c))

	return nil
}

func (c *SqliteStorage) Provision(ctx caddy.Context) error {

	// Load Environment
	if c.Dsn == "" {
		c.Dsn = os.Getenv("sqlite_DSN")
	}
	if c.Dsn == "" {
		c.Dsn = "/var/lib/caddy/.local/share/caddy/certs.sqlite"
	}
	if c.QueryTimeout == 0 {
		c.QueryTimeout = 3
	}
	if c.LockTimeout == 0 {
		c.LockTimeout = 60
	}

	caddy.Log().Named("storage.sqlite").Debug(fmt.Sprintf("Provision %v", c))

	return nil
}

func (SqliteStorage) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "caddy.storage.sqlite",
		New: func() caddy.Module {
			return new(SqliteStorage)
		},
	}
}

func NewStorage(c SqliteStorage) (certmagic.Storage, error) {
	var connStr string
	if len(c.Dsn) > 0 {
		connStr = c.Dsn
	} else {
		return nil, errors.New("Dsn not set")
	}

	db, err := sql.Open("sqlite", connStr)
	if err != nil {
		return nil, err
	}
	s := &SqliteStorage{
		Database:     db,
		QueryTimeout: c.QueryTimeout,
		LockTimeout:  c.LockTimeout,
	}

	caddy.Log().Named("storage.sqlite").Debug(fmt.Sprintf("NewStorage %v %v", c, s))
	return s, s.ensureTableSetup()
}

func (c *SqliteStorage) CertMagicStorage() (certmagic.Storage, error) {
	return NewStorage(*c)
}

type DB interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}

// Database RDBs this library supports, currently supports Postgres only.
type Database int

const (
	Sqlite Database = iota
)

func (s *SqliteStorage) ensureTableSetup() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.QueryTimeout*time.Second)
	defer cancel()
	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	caddy.Log().Named("storage.sqlite.sql").Debug(fmt.Sprintf("ensureTableSetup"))
	dataTable := `CREATE TABLE IF NOT EXISTS
	certmagic_data (
  	key_hash char(40) NOT NULL,
  	key TEXT NOT NULL,
  	value BLOB,
  	modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  	PRIMARY KEY (key_hash)
	)`
	_, err = tx.ExecContext(ctx, dataTable)
	if err != nil {
		return err
	}
	lockTable := `
  	CREATE TABLE IF NOT EXISTS certmagic_locks (
  	key_hash char(40) NOT NULL,
  	key TEXT NOT NULL,
  	expires TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  	PRIMARY KEY (key_hash)
	)`
	_, err = tx.ExecContext(ctx, lockTable)
	if err != nil {
		return err
	}

	triggerUpdate := `
	CREATE TRIGGER if not exists Trg_LastUpdated
	AFTER UPDATE ON certmagic_data
	FOR EACH ROW
	BEGIN
	UPDATE certmagic_data SET modified = CURRENT_TIMESTAMP WHERE key_hash = OLD.key_hash;
	END
	`
	_, err = tx.ExecContext(ctx, triggerUpdate)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func getMD5String(s string) string {
	md5Code := md5.Sum([]byte(s + "storage.sqlite.salt"))
	return hex.EncodeToString(md5Code[:])
}

// Lock the key and implement certmagic.Storage.Lock.
func (s *SqliteStorage) Lock(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, s.QueryTimeout*time.Second)
	defer cancel()

	tx, err := s.Database.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if err := s.isLocked(tx, key); err != nil {
		return err
	}

	expires := time.Now().Add(s.LockTimeout * time.Second)
	key_hash := getMD5String(key)
	query := `INSERT INTO certmagic_locks (key_hash,key, expires) VALUES (?, ?, ?) ON CONFLICT(key_hash) DO UPDATE set expires = ?`
	if _, err := tx.ExecContext(ctx, query, key_hash, key, expires, expires); err != nil {
		return fmt.Errorf("failed to lock key: %s: %w", key, err)
	}

	return tx.Commit()
}

// Unlock the key and implement certmagic.Storage.Unlock.
func (s *SqliteStorage) Unlock(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, s.QueryTimeout*time.Second)
	defer cancel()
	key_hash := getMD5String(key)
	caddy.Log().Named("storage.sqlite.sql").Debug(fmt.Sprintf("DELETE FROM certmagic_locks WHERE key_hash = %s", key_hash))
	_, err := s.Database.ExecContext(ctx, "DELETE FROM certmagic_locks WHERE key_hash = ?", key_hash)
	return err
}

type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// isLocked returns nil if the key is not locked.
func (s *SqliteStorage) isLocked(queryer queryer, key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.QueryTimeout*time.Second)
	defer cancel()
	key_hash := getMD5String(key)
	current_timestamp := time.Now()

	row := queryer.QueryRowContext(ctx, "select exists(select 1 from certmagic_locks where key_hash = ? and expires > ?)", key_hash, current_timestamp)
	var locked bool
	if err := row.Scan(&locked); err != nil {
		return err
	}
	if locked {
		return fmt.Errorf("key is locked: %s", key)
	}
	return nil
}

// Store puts value at key.
func (s *SqliteStorage) Store(ctx context.Context, key string, value []byte) error {
	ctx, cancel := context.WithTimeout(ctx, s.QueryTimeout*time.Second)
	defer cancel()
	key_hash := getMD5String(key)
	_, err := s.Database.ExecContext(ctx, `INSERT INTO certmagic_data (key_hash, key, value)
	VALUES (?, ?, ?) ON CONFLICT(key_hash) DO UPDATE
	set value = ?, modified = current_timestamp`, key_hash, key, value, value)
	return err
}

// Load retrieves the value at key.
func (s *SqliteStorage) Load(ctx context.Context, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, s.QueryTimeout*time.Second)
	defer cancel()
	var value []byte
	key_hash := getMD5String(key)
	caddy.Log().Named("storage.sqlite.sql").Debug(fmt.Sprintf("SELECT value FROM certmagic_data WHERE key_hash = %s", key_hash))

	err := s.Database.QueryRowContext(ctx, "SELECT value FROM certmagic_data WHERE key_hash = ?", key_hash).Scan(&value)
	if err == sql.ErrNoRows {
		return nil, fs.ErrNotExist
	}
	return value, err
}

// Delete deletes key. An error should be
// returned only if the key still exists
// when the method returns.
func (s *SqliteStorage) Delete(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, s.QueryTimeout*time.Second)
	defer cancel()
	key_hash := getMD5String(key)
	caddy.Log().Named("storage.sqlite.sql").Debug(fmt.Sprintf("DELETE FROM certmagic_data WHERE key_hash =  %s", key_hash))
	_, err := s.Database.ExecContext(ctx, "DELETE FROM certmagic_data WHERE key_hash = ?", key_hash)
	return err
}

// Exists returns true if the key exists
// and there was no error checking.
func (s *SqliteStorage) Exists(ctx context.Context, key string) bool {
	ctx, cancel := context.WithTimeout(ctx, s.QueryTimeout*time.Second)
	defer cancel()
	key_hash := getMD5String(key)

	caddy.Log().Named("storage.sqlite.sql").Debug(fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM certmagic_data WHERE key_hash = %s)", key_hash))

	row := s.Database.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM certmagic_data WHERE key_hash = ?)", key_hash)
	var exists bool
	err := row.Scan(&exists)
	return err == nil && exists
}

// List returns all keys that match prefix.
// If recursive is true, non-terminal keys
// will be enumerated (i.e. "directories"
// should be walked); otherwise, only keys
// prefixed exactly by prefix will be listed.
func (s *SqliteStorage) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.QueryTimeout*time.Second)
	defer cancel()
	if recursive {
		return nil, fmt.Errorf("recursive not supported")
	}

	caddy.Log().Named("storage.sqlite.sql").Debug(fmt.Sprintf("select key from certmagic_data where key like '%s%%'", prefix))

	rows, err := s.Database.QueryContext(ctx, fmt.Sprintf("select key from certmagic_data where key like '%s%%'", prefix))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// Stat returns information about key.
func (s *SqliteStorage) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, s.QueryTimeout*time.Second)
	defer cancel()
	var modified time.Time
	var size int64
	key_hash := getMD5String(key)
	caddy.Log().Named("storage.sqlite.sql").Debug(fmt.Sprintf("select length(value), modified from certmagic_data where key_hash = %s", key_hash))

	row := s.Database.QueryRowContext(ctx, "select length(value), modified from certmagic_data where key_hash = ?", key_hash)
	err := row.Scan(&size, &modified)
	if err != nil {
		return certmagic.KeyInfo{}, err
	}
	return certmagic.KeyInfo{
		Key:        key,
		Modified:   modified,
		Size:       size,
		IsTerminal: true,
	}, nil
}

func (s SqliteStorage) Validate() error {
	caddy.Log().Named("storage.sqlite.sql").Info(fmt.Sprintf("Validate"))
	return nil
}

var (
	_ caddy.Module          = (*SqliteStorage)(nil)
	_ caddy.Provisioner     = (*SqliteStorage)(nil)
	_ caddy.Validator       = (*SqliteStorage)(nil)
	_ caddyfile.Unmarshaler = (*SqliteStorage)(nil)
)
