package vfstest

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFileForgetGrown checks that after a file grows out of band on the remote
// and is then forgotten, the mount reports the new size rather than the
// kernel's stale (truncated) cached one.
//
// It asserts on the stat size, not on read content, so as not to trip the
// backend's own changed-file detection (which would mask the kernel cache).
//
// See https://github.com/rclone/rclone/issues/9617
func TestFileForgetGrown(t *testing.T) {
	run.skipIfVFS(t)
	run.skipIfNoFUSE(t)
	if !run.supportsKernelInvalidation() {
		t.Skip("mount backend doesn't invalidate the kernel cache on forget")
	}
	if run.vfsOpt.CacheMode != vfscommon.CacheModeOff {
		t.Skip("only meaningful with --vfs-cache-mode off - reads go straight to the remote")
	}

	const (
		short = "small"
		long  = "much longer contents than the original file had"
	)

	// Create through the mount and stat it so the kernel caches the short size.
	run.createFile(t, "file", short)
	fi, err := run.os.Stat(run.path("file"))
	require.NoError(t, err)
	require.Equal(t, int64(len(short)), fi.Size())

	// Grow the object out of band, directly on the remote.
	src := object.NewStaticObjectInfo("file", time.Now(), int64(len(long)), true, nil, nil)
	_, err = run.fremote.Put(context.Background(), strings.NewReader(long), src)
	require.NoError(t, err)

	// Forget it - on a mount this drops the kernel's cached entry.
	run.forgetFile("file")

	// The kernel must now report the new size. Invalidation is dispatched
	// asynchronously so allow a few retries, but stay well under --attr-timeout
	// (1s) so that without the fix this fails rather than self-healing on the
	// attribute timeout expiring.
	var size int64
	for range 20 {
		fi, err = run.os.Stat(run.path("file"))
		require.NoError(t, err)
		if size = fi.Size(); size == int64(len(long)) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Equal(t, int64(len(long)), size)

	run.rm(t, "file")
}
