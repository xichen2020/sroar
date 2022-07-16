/*
 * Copyright 2021 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sroar

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIteratorBasic(t *testing.T) {
	n := uint64(1e5)
	bm := NewBitmap()
	for i := uint64(0); i < n; i++ {
		bm.Set(uint64(i))
	}

	var curr uint64
	it := bm.NewIterator()
	for v, ok := it.Next(); ok; v, ok = it.Next() {
		require.Equal(t, curr, v)
		curr++
	}
	require.Equal(t, n, curr)
}

func TestIteratorRanges(t *testing.T) {
	n := uint64(1e5)
	bm := NewBitmap()
	for i := uint64(1); i <= n; i++ {
		bm.Set(uint64(i))
	}

	iters := bm.NewRangeIterators(8)
	cnt := uint64(1)
	for idx := 0; idx < 8; idx++ {
		it := iters[idx]
		for v, ok := it.Next(); ok; v, ok = it.Next() {
			require.Equal(t, cnt, v)
			cnt++
		}
	}
}

func TestIteratorRandom(t *testing.T) {
	n := uint64(1e6)
	bm := NewBitmap()
	mp := make(map[uint64]struct{})
	var arr []uint64
	for i := uint64(1); i <= n; i++ {
		v := uint64(rand.Intn(int(n) * 5))
		if _, ok := mp[v]; ok {
			continue
		}
		mp[v] = struct{}{}
		arr = append(arr, v)
		bm.Set(uint64(v))
	}

	sort.Slice(arr, func(i, j int) bool {
		return arr[i] < arr[j]
	})

	it := bm.NewIterator()
	for i := uint64(0); i < uint64(len(arr)); i++ {
		v, ok := it.Next()
		require.True(t, ok)
		require.Equal(t, arr[i], v)
	}
	_, ok := it.Next()
	require.False(t, ok)
}

func TestIteratorWithRemoveKeys(t *testing.T) {
	b := NewBitmap()
	N := uint64(1e6)
	for i := uint64(0); i < N; i++ {
		b.Set(i)
	}

	b.RemoveRange(0, N)
	it := b.NewIterator()

	cnt := 0
	for _, ok := it.Next(); ok; _, ok = it.Next() {
		cnt++
	}
	require.Equal(t, 0, cnt)
}

func TestManyIterator(t *testing.T) {
	b := NewBitmap()
	for i := 0; i < int(1e6); i++ {
		b.Set(uint64(i))
	}

	mi := b.ManyIterator()
	buf := make([]uint64, 1000)

	i := 0
	for {
		got := mi.NextMany(buf)
		if got == 0 {
			break
		}
		require.Equal(t, 1000, got)
		require.Equal(t, uint64(i*1000), buf[0])
		i++
	}
}

func BenchmarkIterator(b *testing.B) {
	bm := NewBitmap()
	for i := 0; i < int(1e5); i++ {
		bm.Set(uint64(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := bm.NewIterator()
		for _, ok := it.Next(); ok; _, ok = it.Next() {
		}
	}
}
