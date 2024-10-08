package gcstorage

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var emulator *exec.Cmd

func setup(t *testing.T) {
	_ = exec.Command(`docker`, `rm`, `-f`, `gcp-storage-emulator`).Run()

	err := exec.Command(`docker`, `pull`, `oittaa/gcp-storage-emulator`).Run()

	emulator = exec.Command(`docker`, `run`, `-e`, `PORT=9023`, `-p`, `9023:9023`, `--name`, `gcp-storage-emulator`,
		`oittaa/gcp-storage-emulator`, `start`, `--port=9023`, `--in-memory`, `--default-bucket=my-bucket`)

	out, err := emulator.StdoutPipe()
	require.NoError(t, err)
	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := out.Read(buf)
			if err != nil {
				return
			}
			t.Log(string(buf[:n]))
		}
	}()

	stderr, err := emulator.StderrPipe()
	require.NoError(t, err)
	go func() {
		buf := make([]byte, 1024)
		for {
			n, err := stderr.Read(buf)
			if err != nil {
				return
			}
			t.Log(string(buf[:n]))
		}
	}()

	err = emulator.Start()
	require.NoError(t, err)

	err = os.Setenv("STORAGE_EMULATOR_HOST", "localhost:9023")
	require.NoError(t, err)

	time.Sleep(5 * time.Second)
}

func teardown(t *testing.T) {
	exec.Command(`docker`, `rm`, `-f`, `gcp-storage-emulator`).Run()
}

func TestGCStorage(t *testing.T) {
	setup(t)
	defer teardown(t)

	cfg := &Config{
		Bucket:    "my-bucket",
		KeyPrefix: "test/",
	}

	backend := Backend(cfg)

	store, err := NewWithBackend[string](backend)
	require.NoError(t, err)

	t.Run("Set_Get", func(t *testing.T) {
		ctx := context.Background()

		err = store.Set(ctx, "foo", "bar")
		require.NoError(t, err)

		v, ok, err := store.Get(ctx, "foo")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "bar", v)
	})

	t.Run("Exists", func(t *testing.T) {
		ctx := context.Background()

		err = store.Set(ctx, "foo", "bar")
		require.NoError(t, err)

		ok, err := store.Exists(ctx, "foo")
		require.NoError(t, err)
		require.True(t, ok)

		ok, err = store.Exists(ctx, "bar")
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("Delete", func(t *testing.T) {
		ctx := context.Background()

		err = store.Set(ctx, "foo", "bar")
		require.NoError(t, err)

		err = store.Delete(ctx, "foo")
		require.NoError(t, err)

		ok, err := store.Exists(ctx, "foo")
		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("SetEx", func(t *testing.T) {
		ctx := context.Background()

		err = store.SetEx(ctx, "foo", "bar", 1*time.Second)
		require.NoError(t, err)

		v, ok, err := store.Get(ctx, "foo")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "bar", v)

		time.Sleep(2 * time.Second)

		v, ok, err = store.Get(ctx, "foo")
		require.NoError(t, err)
		require.False(t, ok)
		require.Equal(t, "", v)
	})

	t.Run("BatchSetEx", func(t *testing.T) {
		ctx := context.Background()

		err = store.BatchSetEx(ctx, []string{"foo", "bar"}, []string{"1", "2"}, 1*time.Second)
		require.NoError(t, err)

		v, ok, err := store.Get(ctx, "foo")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "1", v)

		v, ok, err = store.Get(ctx, "bar")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, "2", v)

		time.Sleep(2 * time.Second)

		v, ok, err = store.Get(ctx, "foo")
		require.NoError(t, err)
		require.False(t, ok)
		require.Equal(t, "", v)

		v, ok, err = store.Get(ctx, "bar")
		require.NoError(t, err)
		require.False(t, ok)
		require.Equal(t, "", v)
	})

	t.Run("BatchGet", func(t *testing.T) {
		ctx := context.Background()

		err = store.BatchSetEx(ctx, []string{"foo", "bar"}, []string{"1", "2"}, 1*time.Second)
		require.NoError(t, err)

		values, exists, err := store.BatchGet(ctx, []string{"foo", "bar", "baz"})
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", ""}, values)
		require.Equal(t, []bool{true, true, false}, exists)

		time.Sleep(2 * time.Second)

		values, exists, err = store.BatchGet(ctx, []string{"foo", "bar", "baz"})
		require.NoError(t, err)
		require.Equal(t, []string{"", "", ""}, values)
		require.Equal(t, []bool{false, false, false}, exists)
	})
}
