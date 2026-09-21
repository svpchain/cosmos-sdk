package cachemulti

import (
	"fmt"
	"testing"

	dbm "github.com/cosmos/cosmos-db"
	"github.com/stretchr/testify/require"

	"cosmossdk.io/store/dbadapter"
	"cosmossdk.io/store/types"
)

func TestStoreGetKVStore(t *testing.T) {
	require := require.New(t)

	s := Store{stores: map[types.StoreKey]types.CacheWrap{}}
	key := types.NewKVStoreKey("abc")
	errMsg := fmt.Sprintf("kv store with key %v has not been registered in stores", key)

	require.PanicsWithValue(errMsg,
		func() { s.GetStore(key) })

	require.PanicsWithValue(errMsg,
		func() { s.GetKVStore(key) })
}

// newTestStore builds an eager root multistore over in-memory substores.
func newTestStore(t *testing.T, keys ...types.StoreKey) (Store, map[types.StoreKey]types.KVStore) {
	t.Helper()
	parents := make(map[types.StoreKey]types.KVStore, len(keys))
	wrappers := make(map[types.StoreKey]types.CacheWrapper, len(keys))
	for _, k := range keys {
		s := dbadapter.Store{DB: dbm.NewMemDB()}
		parents[k] = s
		wrappers[k] = s
	}
	return NewStore(dbm.NewMemDB(), wrappers, nil, nil, nil), parents
}

func TestLazyBranchOnlyMaterialisesTouchedStores(t *testing.T) {
	keyA, keyB, keyC := types.NewKVStoreKey("a"), types.NewKVStoreKey("b"), types.NewKVStoreKey("c")
	root, parents := newTestStore(t, keyA, keyB, keyC)
	require.Len(t, root.stores, 3, "root branches are eager")

	branch := root.CacheMultiStore().(Store)
	require.NotNil(t, branch.parent)
	require.Empty(t, branch.stores, "nothing branched until first access")

	branch.GetKVStore(keyA).Set([]byte("k"), []byte("v"))
	require.Len(t, branch.stores, 1)
	require.Nil(t, root.GetKVStore(keyA).Get([]byte("k")), "not written through before Write")

	branch.Write()
	require.Equal(t, []byte("v"), root.GetKVStore(keyA).Get([]byte("k")))
	require.Nil(t, parents[keyA].Get([]byte("k")), "root itself is a branch of the parents")
	root.Write()
	require.Equal(t, []byte("v"), parents[keyA].Get([]byte("k")))

	// Untouched stores were never branched and never written.
	require.Len(t, branch.stores, 1)
	require.Nil(t, parents[keyB].Get([]byte("k")))

	// Unregistered keys still panic with the historical message.
	other := types.NewKVStoreKey("other")
	errMsg := fmt.Sprintf("kv store with key %v has not been registered in stores", other)
	require.PanicsWithValue(t, errMsg, func() { branch.GetKVStore(other) })
	require.PanicsWithValue(t, errMsg, func() { branch.GetStore(other) })
}

func TestLazyBranchOfLazyBranch(t *testing.T) {
	keyA, keyB := types.NewKVStoreKey("a"), types.NewKVStoreKey("b")
	root, _ := newTestStore(t, keyA, keyB)

	ante := root.CacheMultiStore().(Store)
	msgs := ante.CacheMultiStore().(Store)

	require.Empty(t, ante.stores)
	msgs.GetKVStore(keyB).Set([]byte("k"), []byte("v"))
	// Branching keyB in the grandchild materialises exactly that key in the child.
	require.Len(t, ante.stores, 1)
	require.Len(t, msgs.stores, 1)
	require.Equal(t, []byte("v"), msgs.GetKVStore(keyB).Get([]byte("k")))
	require.Nil(t, ante.GetKVStore(keyB).Get([]byte("k")))

	msgs.Write()
	require.Equal(t, []byte("v"), ante.GetKVStore(keyB).Get([]byte("k")))
	require.Nil(t, root.GetKVStore(keyB).Get([]byte("k")))
	ante.Write()
	require.Equal(t, []byte("v"), root.GetKVStore(keyB).Get([]byte("k")))

	// A sibling branch created from the same parent does not see unwritten data.
	sibling := root.CacheMultiStore()
	require.Equal(t, []byte("v"), sibling.GetKVStore(keyB).Get([]byte("k")))
	require.Nil(t, sibling.GetKVStore(keyA).Get([]byte("k")))
}

func TestEagerStoreWritesAllSubstoresInParallel(t *testing.T) {
	keys := make([]types.StoreKey, 0, parallelWriteMinStores+3)
	for i := 0; i < parallelWriteMinStores+3; i++ {
		keys = append(keys, types.NewKVStoreKey(fmt.Sprintf("s%d", i)))
	}
	root, parents := newTestStore(t, keys...)
	require.GreaterOrEqual(t, len(root.stores), parallelWriteMinStores)

	for i, k := range keys {
		for j := 0; j < 50; j++ {
			root.GetKVStore(k).Set([]byte(fmt.Sprintf("k%d", j)), []byte(fmt.Sprintf("v%d-%d", i, j)))
		}
	}
	for _, k := range keys {
		require.Nil(t, parents[k].Get([]byte("k0")))
	}

	root.Write()

	for i, k := range keys {
		for j := 0; j < 50; j++ {
			require.Equal(t, []byte(fmt.Sprintf("v%d-%d", i, j)), parents[k].Get([]byte(fmt.Sprintf("k%d", j))))
		}
	}
}

func TestLockingBranchEagerlyBranchesLockedStores(t *testing.T) {
	locking := types.NewKVStoreKey("locking").WithLocking()
	plain := types.NewKVStoreKey("plain")
	wrappers := map[types.StoreKey]types.CacheWrapper{
		locking: dbadapter.Store{DB: dbm.NewMemDB()},
		plain:   dbadapter.Store{DB: dbm.NewMemDB()},
	}
	root := NewLockingStore(dbm.NewMemDB(), wrappers, nil, nil, nil)

	lockKey := []byte("account-1")
	branch := root.CacheMultiStoreWithLocking(map[types.StoreKey][][]byte{
		locking: {lockKey},
	}).(Store)
	require.Len(t, branch.stores, 1, "only the locked store is branched up front")
	_, isLocking := branch.stores[locking].(types.LockingStore)
	require.True(t, isLocking)

	branch.GetKVStore(locking).Set(lockKey, []byte("v1"))
	branch.GetKVStore(plain).Set([]byte("p"), []byte("v2"))
	require.Len(t, branch.stores, 2)

	branch.Write()
	branch.Unlock()
	require.Equal(t, []byte("v1"), root.GetKVStore(locking).Get(lockKey))
	require.Equal(t, []byte("v2"), root.GetKVStore(plain).Get([]byte("p")))

	// The lock was released: a second branch over the same key must not block.
	done := make(chan struct{})
	go func() {
		defer close(done)
		b := root.CacheMultiStoreWithLocking(map[types.StoreKey][][]byte{locking: {lockKey}})
		b.(types.LockingStore).Unlock()
	}()
	<-done
}
