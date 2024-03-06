package storagesqlite

import (
	"context"
	"os"
	"testing"

	"github.com/caddyserver/certmagic"
	_ "modernc.org/sqlite"
)

func setup(t *testing.T) certmagic.Storage {
	return setupWithOptions(t)
}

func setupWithOptions(t *testing.T) certmagic.Storage {
	os.Setenv("sqlite_DSN", "./db.sqlite")
	connStr := os.Getenv("sqlite_DSN")
	if connStr == "" {
		t.Skipf("must set sqlite_DSN")
	}
	// _, err := sql.Open("sqlite", connStr)
	// if err != nil {
	// 	t.Fatal(err)
	// }

	c := SqliteStorage{}

	c.Dsn = connStr
	c.QueryTimeout = 10
	c.LockTimeout = 60
	storage, err := NewStorage(c)
	if err != nil {
		t.Fatal(err)
	}
	return storage

}

func TestCaddySqliteAdapter(t *testing.T) {
	storage := setup(t)

	var ctx = context.Background()

	var testDataSet = []string{"test", "test1", "test2"}

	for _, s := range testDataSet {
		err := storage.Store(ctx, s, []byte(s))
		if err != nil {
			t.Fatalf("TestCaddySqliteAdapter Store %v", err)
		}
	}

	list_res, list_err := storage.List(ctx, "test", false)
	if list_err != nil {
		t.Fatalf("TestCaddySqliteAdapter List Error %v", list_err)
	}
	t.Logf("TestCaddySqliteAdapter list res %s err %v", list_res, list_err)
	for _, s := range testDataSet {
		exists := storage.Exists(ctx, s)
		if !exists {
			t.Fatalf("TestCaddySqliteAdapter Exists %s not found ", s)
		}

		info, stat_err := storage.Stat(ctx, s)
		if stat_err != nil {
			t.Fatalf("TestCaddySqliteAdapter Stat %v", stat_err)
		}
		t.Logf("TestCaddySqliteAdapter res %v", info)

		delete_err := storage.Delete(ctx, s)
		if delete_err != nil {
			t.Fatalf("TestCaddySqliteAdapter Delete %v", delete_err)
		}

		exists = storage.Exists(ctx, s)
		if exists {
			t.Fatalf("TestCaddySqliteAdapter Delete %s not works ", s)
		}

		lock_err := storage.Lock(ctx, s)
		if lock_err != nil {
			t.Fatalf("TestCaddySqliteAdapter Lock %v", lock_err)
		}

		lock_err = storage.Lock(ctx, s)
		if lock_err == nil {
			t.Fatalf("TestCaddySqliteAdapter Lock not works %v", lock_err)
		}

		lock_err = storage.Unlock(ctx, s)
		if lock_err != nil {
			t.Fatalf("TestCaddySqliteAdapter Unlock %v", lock_err)
		}
	}

	// res, err := storage.Load(ctx, "ddd")
	// if err != nil {
	// 	t.Fatalf("TestCaddySqliteAdapter Error %v", err)
	// }

	// t.Logf("TestCaddySqliteAdapter res %s", string(res))
	// cancel()
}
