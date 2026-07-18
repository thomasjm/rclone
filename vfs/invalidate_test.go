package vfs

import (
	"context"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collect drains ch until it has seen all of want (or times out) and returns
// the set of paths seen.
func collectInvalidations(ch <-chan string, want ...string) map[string]bool {
	got := map[string]bool{}
	need := map[string]bool{}
	for _, w := range want {
		need[w] = true
	}
	timeout := time.After(5 * time.Second)
	for {
		done := true
		for w := range need {
			if !got[w] {
				done = false
				break
			}
		}
		if done {
			return got
		}
		select {
		case p := <-ch:
			got[p] = true
		case <-timeout:
			return got
		}
	}
}

// drain removes anything currently queued on ch.
func drain(ch <-chan string) {
	for {
		select {
		case <-ch:
		case <-time.After(100 * time.Millisecond):
			return
		}
	}
}

// set up a VFS with dir/file1 and dir/file2 looked up (cached) and a hook
// feeding paths into the returned channel.
func invalidateTestVFS(t *testing.T) (vfs *VFS, ch chan string) {
	r, vfs := newTestVFS(t)
	ctx := context.Background()
	file1 := r.WriteObject(ctx, "dir/file1", "content one", t1)
	file2 := r.WriteObject(ctx, "dir/file2", "content two two", t1)
	r.CheckRemoteItems(t, file1, file2)

	// Look them up so they are cached
	for _, path := range []string{"dir", "dir/file1", "dir/file2"} {
		_, err := vfs.Stat(path)
		require.NoError(t, err)
	}

	ch = make(chan string, 64)
	t.Cleanup(func() { drain(ch) })
	vfs.AddInvalidateKernelCacheHook(func(ctx context.Context, node Node) {
		ch <- node.Path()
	})
	return vfs, ch
}

// ForgetPath on a file invalidates that file's kernel entry
func TestInvalidateForgetPath(t *testing.T) {
	vfs, ch := invalidateTestVFS(t)
	root, err := vfs.Root()
	require.NoError(t, err)

	root.ForgetPath("dir/file1", fs.EntryObject)

	got := collectInvalidations(ch, "dir/file1")
	assert.True(t, got["dir/file1"], "got %v", got)
}

// ForgetAll (i.e. vfs/forget with no args) invalidates the whole looked-up tree
func TestInvalidateForgetAll(t *testing.T) {
	vfs, ch := invalidateTestVFS(t)
	root, err := vfs.Root()
	require.NoError(t, err)

	root.ForgetAll()

	got := collectInvalidations(ch, "dir", "dir/file1", "dir/file2")
	assert.True(t, got["dir"], "got %v", got)
	assert.True(t, got["dir/file1"], "got %v", got)
	assert.True(t, got["dir/file2"], "got %v", got)
}

// changeNotify invalidates the changed node
func TestInvalidateChangeNotify(t *testing.T) {
	vfs, ch := invalidateTestVFS(t)
	root, err := vfs.Root()
	require.NoError(t, err)

	root.changeNotify("dir/file1", fs.EntryObject)

	got := collectInvalidations(ch, "dir/file1")
	assert.True(t, got["dir/file1"], "got %v", got)
}

// vfs/refresh invalidates the refreshed subtree
func TestInvalidateRefresh(t *testing.T) {
	vfs, ch := invalidateTestVFS(t)
	root, err := vfs.Root()
	require.NoError(t, err)

	root.invalidateKernelCacheForSubtree()

	got := collectInvalidations(ch, "dir", "dir/file1", "dir/file2")
	assert.True(t, got["dir/file1"], "got %v", got)
	assert.True(t, got["dir/file2"], "got %v", got)
}

// Two hooks both fire (a VFS can be shared by more than one mount), and
// unsubscribing removes only that hook.
func TestInvalidateMultipleHooks(t *testing.T) {
	vfs, ch1 := invalidateTestVFS(t)
	root, err := vfs.Root()
	require.NoError(t, err)

	ch2 := make(chan string, 64)
	t.Cleanup(func() { drain(ch2) })
	remove2 := vfs.AddInvalidateKernelCacheHook(func(ctx context.Context, node Node) {
		ch2 <- node.Path()
	})

	root.ForgetPath("dir/file1", fs.EntryObject)
	assert.True(t, collectInvalidations(ch1, "dir/file1")["dir/file1"])
	assert.True(t, collectInvalidations(ch2, "dir/file1")["dir/file1"])

	// Remove the second hook - it should stop receiving
	remove2()
	drain(ch1)
	drain(ch2)
	root.ForgetPath("dir/file2", fs.EntryObject)

	assert.True(t, collectInvalidations(ch1, "dir/file2")["dir/file2"])
	select {
	case p := <-ch2:
		t.Fatalf("removed hook still fired with %q", p)
	case <-time.After(500 * time.Millisecond):
	}
}

// Unsubscribing waits for an in-flight hook to finish, so a mount can tear
// down safely.
func TestInvalidateUnsubscribeBarrier(t *testing.T) {
	r, vfs := newTestVFS(t)
	file1 := r.WriteObject(context.Background(), "dir/file1", "content one", t1)
	r.CheckRemoteItems(t, file1)
	_, err := vfs.Stat("dir/file1")
	require.NoError(t, err)
	root, err := vfs.Root()
	require.NoError(t, err)

	entered := make(chan struct{})
	release := make(chan struct{})
	remove := vfs.AddInvalidateKernelCacheHook(func(ctx context.Context, node Node) {
		close(entered)
		<-release
	})

	// Trigger one invalidation and wait until the hook is running and blocked.
	root.ForgetPath("dir/file1", fs.EntryObject)
	<-entered

	removed := make(chan struct{})
	go func() {
		remove()
		close(removed)
	}()

	// remove() must not return while the hook is still in-flight.
	select {
	case <-removed:
		t.Fatal("remove() returned while a hook was still running")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	select {
	case <-removed:
	case <-time.After(2 * time.Second):
		t.Fatal("remove() did not return after the hook finished")
	}
}

// Parent() returns the parent dir and nil for the root
func TestNodeParent(t *testing.T) {
	vfs, _ := invalidateTestVFS(t)
	root, err := vfs.Root()
	require.NoError(t, err)
	assert.Nil(t, root.Parent())

	file, err := vfs.Stat("dir/file1")
	require.NoError(t, err)
	parent := file.Parent()
	require.NotNil(t, parent)
	assert.Equal(t, "dir", parent.Path())
	assert.Equal(t, "", parent.Parent().(*Dir).Path()) // parent of dir is root
}
