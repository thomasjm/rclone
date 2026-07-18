package vfs

import (
	"context"
	"strings"
)

// InvalidateKernelCacheHook runs on a dedicated goroutine, never a FUSE
// handler, so a synchronous kernel notify is safe.
type InvalidateKernelCacheHook func(ctx context.Context, node Node)

// AddInvalidateKernelCacheHook registers fn and returns a function to
// unregister it. A VFS shared by several mounts (see New) calls every hook.
func (vfs *VFS) AddInvalidateKernelCacheHook(fn InvalidateKernelCacheHook) (remove func()) {
	vfs.invalidateKernelCacheMu.Lock()
	defer vfs.invalidateKernelCacheMu.Unlock()
	if vfs.invalidateKernelCacheHooks == nil {
		vfs.invalidateKernelCacheHooks = map[int]InvalidateKernelCacheHook{}
		vfs.invalidateKernelCachePending = map[Node]struct{}{}
		vfs.invalidateKernelCacheWake = make(chan struct{}, 1)
		go vfs.invalidateKernelCacheDispatcher()
	}
	id := vfs.invalidateKernelCacheNextID
	vfs.invalidateKernelCacheNextID++
	vfs.invalidateKernelCacheHooks[id] = fn
	return func() {
		vfs.invalidateKernelCacheMu.Lock()
		delete(vfs.invalidateKernelCacheHooks, id)
		vfs.invalidateKernelCacheMu.Unlock()
		// Barrier: wait for any in-flight dispatch to finish so the caller
		// can tear down its mount safely.
		vfs.invalidateKernelCacheDispatchMu.Lock()
		vfs.invalidateKernelCacheDispatchMu.Unlock() //nolint:staticcheck // barrier, not an empty critical section
	}
}

// HasInvalidateKernelCacheHooks reports whether any hook is registered.
func (vfs *VFS) HasInvalidateKernelCacheHooks() bool {
	vfs.invalidateKernelCacheMu.Lock()
	defer vfs.invalidateKernelCacheMu.Unlock()
	return len(vfs.invalidateKernelCacheHooks) > 0
}

// invalidateKernelCacheForNode queues node. The pending set is keyed on node
// so a storm coalesces to one notify per node, and enqueuing never blocks.
func (vfs *VFS) invalidateKernelCacheForNode(node Node) {
	if node == nil {
		return
	}
	vfs.invalidateKernelCacheMu.Lock()
	if len(vfs.invalidateKernelCacheHooks) == 0 {
		vfs.invalidateKernelCacheMu.Unlock()
		return
	}
	vfs.invalidateKernelCachePending[node] = struct{}{}
	vfs.invalidateKernelCacheMu.Unlock()
	select {
	case vfs.invalidateKernelCacheWake <- struct{}{}:
	default:
	}
}

func (vfs *VFS) invalidateKernelCacheForPath(absPath string) {
	vfs.invalidateKernelCacheMu.Lock()
	empty := len(vfs.invalidateKernelCacheHooks) == 0
	vfs.invalidateKernelCacheMu.Unlock()
	if empty {
		return
	}
	if node := vfs.root.cachedNode(strings.Trim(absPath, "/")); node != nil {
		vfs.invalidateKernelCacheForNode(node)
	}
}

// invalidateKernelCacheDispatcher calls the hooks until the VFS context is cancelled.
func (vfs *VFS) invalidateKernelCacheDispatcher() {
	for {
		select {
		case <-vfs.ctx.Done():
			return
		case <-vfs.invalidateKernelCacheWake:
		}
		// Run the hooks under invalidateKernelCacheDispatchMu so an unsubscribing mount
		// can wait for us. invalidateKernelCacheMu only guards the pending-set swap, so
		// enqueuing never blocks behind a hook.
		vfs.invalidateKernelCacheDispatchMu.Lock()
		vfs.invalidateKernelCacheMu.Lock()
		pending := vfs.invalidateKernelCachePending
		vfs.invalidateKernelCachePending = make(map[Node]struct{}, len(pending))
		hooks := make([]InvalidateKernelCacheHook, 0, len(vfs.invalidateKernelCacheHooks))
		for _, fn := range vfs.invalidateKernelCacheHooks {
			hooks = append(hooks, fn)
		}
		vfs.invalidateKernelCacheMu.Unlock()
		for node := range pending {
			for _, fn := range hooks {
				fn(vfs.ctx, node)
			}
		}
		vfs.invalidateKernelCacheDispatchMu.Unlock()
	}
}
