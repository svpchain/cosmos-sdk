package cachemulti

import (
	"fmt"
	"io"
	"sync"

	dbm "github.com/cosmos/cosmos-db"

	"cosmossdk.io/store/cachekv"
	"cosmossdk.io/store/dbadapter"
	"cosmossdk.io/store/lockingkv"
	"cosmossdk.io/store/tracekv"
	"cosmossdk.io/store/types"
)

// storeNameCtxKey is the TraceContext metadata key that identifies
// the store which emitted a given trace.
const storeNameCtxKey = "store_name"

//----------------------------------------
// Store

// Store holds many branched stores.
// Implements MultiStore.
// NOTE: a Store (and MultiStores in general) should never expose the
// keys for the substores.
//
// A Store is either *eager* (built by NewFromKVStore / NewLockingFromKVStore: every
// registered store is branched up front, `parent == nil`) or *lazy* (built by
// CacheMultiStore / CacheMultiStoreWithLocking: `parent` points at the Store it
// branches and `stores` only contains the substores that have actually been
// accessed). Every transaction branches the multistore at least twice (ante
// handler + messages) and touches only a handful of the ~50 registered stores,
// so branching all of them eagerly used to dominate the per-tx allocation
// volume. Lazy branches are transparent to callers: GetKVStore/GetStore create
// the branch on first use, Write/Unlock only visit the branches that exist.
type Store struct {
	db     types.CacheKVStore
	stores map[types.StoreKey]types.CacheWrap
	keys   map[string]types.StoreKey

	// parent is the Store this lazy branch was created from; nil for eager stores.
	parent *Store
	// lazyMtx guards `stores` of a lazy branch. A branch is normally used by a
	// single goroutine, but the copy semantics of the value receiver make the map
	// shared between copies, so the cheap uncontended lock is kept for safety.
	lazyMtx *sync.Mutex

	traceWriter  io.Writer
	traceContext types.TraceContext
}

var (
	_ types.CacheMultiStore = Store{}
	_ types.LockingStore    = Store{}
)

// NewFromKVStore creates a new Store object from a mapping of store keys to
// CacheWrapper objects and a KVStore as the database. Each CacheWrapper store
// is a branched store.
func NewFromKVStore(
	store types.KVStore, stores map[types.StoreKey]types.CacheWrapper,
	keys map[string]types.StoreKey, traceWriter io.Writer, traceContext types.TraceContext,
) Store {
	cms := Store{
		db:           cachekv.NewStore(store),
		stores:       make(map[types.StoreKey]types.CacheWrap, len(stores)),
		keys:         keys,
		traceWriter:  traceWriter,
		traceContext: traceContext,
	}

	for key, store := range stores {
		cms.stores[key] = cms.branchStore(key, store)
	}

	return cms
}

// NewLockingFromKVStore creates a new Store object from a mapping of store keys to
// CacheWrapper objects and a KVStore as the database. Each CacheWrapper store
// is a branched store.
func NewLockingFromKVStore(
	store types.KVStore, stores map[types.StoreKey]types.CacheWrapper,
	keys map[string]types.StoreKey, traceWriter io.Writer, traceContext types.TraceContext,
) Store {
	cms := Store{
		db:           cachekv.NewStore(store),
		stores:       make(map[types.StoreKey]types.CacheWrap, len(stores)),
		keys:         keys,
		traceWriter:  traceWriter,
		traceContext: traceContext,
	}

	for key, store := range stores {
		if cms.TracingEnabled() {
			tctx := cms.traceContext.Clone().Merge(types.TraceContext{
				storeNameCtxKey: key.Name(),
			})

			store = tracekv.NewStore(store.(types.KVStore), cms.traceWriter, tctx)
		}
		if kvStoreKey, ok := key.(*types.KVStoreKey); ok && kvStoreKey.IsLocking() {
			cms.stores[key] = lockingkv.NewStore(store.(types.KVStore))
		} else {
			cms.stores[key] = cachekv.NewStore(store.(types.KVStore))
		}
	}

	return cms
}

// NewStore creates a new Store object from a mapping of store keys to
// CacheWrapper objects. Each CacheWrapper store is a branched store.
func NewStore(
	db dbm.DB, stores map[types.StoreKey]types.CacheWrapper, keys map[string]types.StoreKey,
	traceWriter io.Writer, traceContext types.TraceContext,
) Store {
	return NewFromKVStore(dbadapter.Store{DB: db}, stores, keys, traceWriter, traceContext)
}

// NewLockingStore creates a new Store object from a mapping of store keys to
// CacheWrapper objects. Each CacheWrapper store is a branched store.
func NewLockingStore(
	db dbm.DB, stores map[types.StoreKey]types.CacheWrapper, keys map[string]types.StoreKey,
	traceWriter io.Writer, traceContext types.TraceContext,
) Store {
	return NewLockingFromKVStore(dbadapter.Store{DB: db}, stores, keys, traceWriter, traceContext)
}

// branchStore wraps `store` (a substore of this multistore's parent) in a fresh
// cachekv branch, adding a tracing layer when tracing is enabled.
func (cms Store) branchStore(key types.StoreKey, store types.CacheWrapper) types.CacheWrap {
	if cms.TracingEnabled() {
		tctx := cms.traceContext.Clone().Merge(types.TraceContext{
			storeNameCtxKey: key.Name(),
		})

		store = tracekv.NewStore(store.(types.KVStore), cms.traceWriter, tctx)
	}
	return cachekv.NewStore(store.(types.KVStore))
}

// newLazyBranch returns a Store that branches `cms` substore by substore on first
// access. `eager` holds the branches that must exist from the start (locked stores).
func newLazyBranch(cms Store, eager map[types.StoreKey]types.CacheWrap) Store {
	if eager == nil {
		eager = make(map[types.StoreKey]types.CacheWrap, 8)
	}
	parent := cms
	return Store{
		db:           cachekv.NewStore(cms.db),
		stores:       eager,
		keys:         cms.keys,
		parent:       &parent,
		lazyMtx:      &sync.Mutex{},
		traceWriter:  cms.traceWriter,
		traceContext: cms.traceContext,
	}
}

func newCacheMultiStoreFromCMS(cms Store) Store {
	return newLazyBranch(cms, nil)
}

// getStore returns the branch of substore `key`, creating it from the parent's
// substore on first access for lazy branches. It returns nil if `key` is not
// registered.
func (cms Store) getStore(key types.StoreKey) types.CacheWrap {
	if key == nil {
		return nil
	}
	if cms.parent == nil {
		return cms.stores[key]
	}

	cms.lazyMtx.Lock()
	defer cms.lazyMtx.Unlock()
	if s, ok := cms.stores[key]; ok {
		return s
	}
	parentStore := cms.parent.getStore(key)
	if parentStore == nil {
		return nil
	}
	s := cms.branchStore(key, parentStore)
	cms.stores[key] = s
	return s
}

// SetTracer sets the tracer for the MultiStore that the underlying
// stores will utilize to trace operations. A MultiStore is returned.
func (cms Store) SetTracer(w io.Writer) types.MultiStore {
	cms.traceWriter = w
	return cms
}

// SetTracingContext updates the tracing context for the MultiStore by merging
// the given context with the existing context by key. Any existing keys will
// be overwritten. It is implied that the caller should update the context when
// necessary between tracing operations. It returns a modified MultiStore.
func (cms Store) SetTracingContext(tc types.TraceContext) types.MultiStore {
	if cms.traceContext != nil {
		for k, v := range tc {
			cms.traceContext[k] = v
		}
	} else {
		cms.traceContext = tc
	}

	return cms
}

// TracingEnabled returns if tracing is enabled for the MultiStore.
func (cms Store) TracingEnabled() bool {
	return cms.traceWriter != nil
}

// LatestVersion returns the branch version of the store
func (cms Store) LatestVersion() int64 {
	panic("cannot get latest version from branch cached multi-store")
}

// GetStoreType returns the type of the store.
func (cms Store) GetStoreType() types.StoreType {
	return types.StoreTypeMulti
}

// parallelWriteMinStores is the number of branched substores from which an eager
// Store writes its substores concurrently instead of one after the other.
const parallelWriteMinStores = 4

// Write calls Write on each underlying store.
//
// An eager Store (the block-level branch of the root multistore) writes its
// substores concurrently: the substores are independent (each wraps its own
// IAVL tree, inter-block cache and, when enabled, its own listener), and
// flushing a whole block's writes into ~40 IAVL trees is tens of milliseconds
// on the FinalizeBlock critical path where CheckTx is stalled. Lazy per-tx
// branches touch few stores and write into in-memory caches, so they stay
// sequential.
func (cms Store) Write() {
	cms.db.Write()
	if cms.lazyMtx != nil {
		cms.lazyMtx.Lock()
		defer cms.lazyMtx.Unlock()
	}
	if cms.parent == nil && len(cms.stores) >= parallelWriteMinStores {
		cms.writeParallel()
		return
	}
	for _, store := range cms.stores {
		store.Write()
	}
}

func (cms Store) writeParallel() {
	var (
		wg       sync.WaitGroup
		panicMtx sync.Mutex
		panicVal any
	)
	for _, store := range cms.stores {
		wg.Add(1)
		go func(s types.CacheWrap) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panicMtx.Lock()
					if panicVal == nil {
						panicVal = r
					}
					panicMtx.Unlock()
				}
			}()
			s.Write()
		}(store)
	}
	wg.Wait()
	if panicVal != nil {
		panic(panicVal)
	}
}

// Unlock calls Unlock on each underlying LockingStore.
func (cms Store) Unlock() {
	if cms.lazyMtx != nil {
		cms.lazyMtx.Lock()
		defer cms.lazyMtx.Unlock()
	}
	for _, store := range cms.stores {
		if s, ok := store.(types.LockingStore); ok {
			s.Unlock()
		}
	}
}

// Implements CacheWrapper.
func (cms Store) CacheWrap() types.CacheWrap {
	return cms.CacheMultiStore().(types.CacheWrap)
}

// CacheWrapWithTrace implements the CacheWrapper interface.
func (cms Store) CacheWrapWithTrace(_ io.Writer, _ types.TraceContext) types.CacheWrap {
	return cms.CacheWrap()
}

// Implements MultiStore.
func (cms Store) CacheMultiStore() types.CacheMultiStore {
	return newCacheMultiStoreFromCMS(cms)
}

// CacheMultiStoreWithLocking branches each store wrapping each store with a cachekv store if not locked or
// delegating to CacheWrapWithLocks if it is a LockingCacheWrapper.
//
// The locked stores are branched (and their locks acquired) right away; every
// other store is branched lazily on first access.
func (cms Store) CacheMultiStoreWithLocking(storeLocks map[types.StoreKey][][]byte) types.CacheMultiStore {
	eager := make(map[types.StoreKey]types.CacheWrap, len(storeLocks)+8)
	for key, lockKeys := range storeLocks {
		store := cms.getStore(key)
		if store == nil {
			panic(fmt.Sprintf("kv store with key %v has not been registered in stores", key))
		}
		eager[key] = store.(types.LockingCacheWrapper).CacheWrapWithLocks(lockKeys)
	}

	return newLazyBranch(cms, eager)
}

// CacheMultiStoreWithVersion implements the MultiStore interface. It will panic
// as an already cached multi-store cannot load previous versions.
//
// TODO: The store implementation can possibly be modified to support this as it
// seems safe to load previous versions (heights).
func (cms Store) CacheMultiStoreWithVersion(_ int64) (types.CacheMultiStore, error) {
	panic("cannot branch cached multi-store with a version")
}

// GetStore returns an underlying Store by key.
func (cms Store) GetStore(key types.StoreKey) types.Store {
	s := cms.getStore(key)
	if s == nil {
		panic(fmt.Sprintf("kv store with key %v has not been registered in stores", key))
	}
	return s.(types.Store)
}

// GetKVStore returns an underlying KVStore by key.
func (cms Store) GetKVStore(key types.StoreKey) types.KVStore {
	store := cms.getStore(key)
	if store == nil {
		panic(fmt.Sprintf("kv store with key %v has not been registered in stores", key))
	}
	return store.(types.KVStore)
}
