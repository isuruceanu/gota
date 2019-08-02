package dataframe

import (
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/isuruceanu/gota/series"
)

//
// input for combineColumns() function
//
type combineColumnsInput struct {
	newCols []series.Series
}

//
// input for Join operations
//
type prepareJoinInput struct {
	a             *DataFrame // first input dataframe
	b             *DataFrame // second input dataframe
	keys          []string   // keys for join
	iKeysA        []int      // indexes from first dataframe for key columns
	iKeysB        []int      // indexes from second dataframe for key columns
	iNotKeysA     []int      // indexes from first dataframe for non-key columns
	iNotKeysB     []int      // indexes from second dataframe for non-key columns
	iCombinedCols tupleArr
	newCols       []series.Series
}

func prepareJoin(a, b DataFrame, compareFn combineFuncType, keys ...string) (prepareJoinInput, error) {
	iKeysA, iKeysB, errorArr := checkDataframesForJoins(a, b, keys...)
	if len(errorArr) != 0 {
		return prepareJoinInput{}, fmt.Errorf("%v", strings.Join(errorArr, "\n"))
	}

	aCols := a.columns
	bCols := b.columns

	// Initialize newCols
	var newCols []series.Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}

	var iCombinedCols tupleArr

	if compareFn != nil {
		for i := 0; i < a.ncols; i++ {
			if !inIntSlice(i, iKeysA) {
				for j := 0; j < b.ncols; j++ {
					if !inIntSlice(j, iKeysB) {
						if compareFn(aCols[i], bCols[j]) {
							iCombinedCols = append(iCombinedCols, tuple{i, j, -1, -1})
						}
					}
				}
			}
		}
	}

	var iNotKeysA []int
	for i := 0; i < a.ncols; i++ {
		if !inIntSlice(i, iKeysA) {
			iNotKeysA = append(iNotKeysA, i)
			newCols = append(newCols, aCols[i].Empty())
			if cIdx, cf := iCombinedCols.findTuple(i, findInA); cf {
				iCombinedCols[cIdx].rAIdx = len(newCols) - 1
			}
		}
	}
	var iNotKeysB []int
	for i := 0; i < b.ncols; i++ {
		if !inIntSlice(i, iKeysB) {
			iNotKeysB = append(iNotKeysB, i)
			newCols = append(newCols, bCols[i].Empty())
			if cIdx, cf := iCombinedCols.findTuple(i, findInB); cf {
				iCombinedCols[cIdx].rBIdx = len(newCols) - 1
			}
		}
	}

	//
	joinInput := prepareJoinInput{
		a:             &a,
		b:             &b,
		keys:          keys,
		iKeysA:        iKeysA,
		iKeysB:        iKeysB,
		iNotKeysA:     iNotKeysA,
		iNotKeysB:     iNotKeysB,
		iCombinedCols: iCombinedCols,
		newCols:       newCols,
	}

	return joinInput, nil
}

func prepareInnerJoinHashForCombineColumns(joinInput prepareJoinInput) combineColumnsInput {
	// prpare input..
	a, b := joinInput.a, joinInput.b
	keys := joinInput.keys
	iKeysA, iKeysB, iNotKeysA, iNotKeysB := joinInput.iKeysA, joinInput.iKeysB, joinInput.iNotKeysA, joinInput.iNotKeysB

	aCols := a.columns
	bCols := b.columns

	newCols := joinInput.newCols

	// 1. build hash
	// TODO: select table with min rows for hashing
	hashBucketsA := createHashBuckets(a, keys, iKeysA)

	// 2. probe hash
	var onKeysEqual onKeysEqualProbe = func(foundBucketValue hashBucketValueT, row int) {
		ii := 0
		for _, k := range iKeysA {
			elem := aCols[k].Elem(foundBucketValue.row)
			newCols[ii].Append(elem)
			ii++
		}

		for _, k := range iNotKeysA {
			elem := aCols[k].Elem(foundBucketValue.row)
			newCols[ii].Append(elem)
			ii++
		}

		for _, k := range iNotKeysB {
			elem := bCols[k].Elem(row)
			newCols[ii].Append(elem)
			ii++
		}
	}

	probeHashForDataframe(b, keys, iKeysB, hashBucketsA, defaultEmptyBucketProbe, onKeysEqual, defaultKeysNotFoundProbe)

	// result..
	return combineColumnsInput{
		newCols: newCols,
	}
}

func prepareOuterJoinHashForCombineColumns(joinInput prepareJoinInput) combineColumnsInput {
	// prpare input..
	a, b := joinInput.a, joinInput.b
	keys := joinInput.keys
	iKeysA, iKeysB, iNotKeysA, iNotKeysB := joinInput.iKeysA, joinInput.iKeysB, joinInput.iNotKeysA, joinInput.iNotKeysB

	aCols := a.columns
	bCols := b.columns

	newCols := joinInput.newCols

	{ // scope for buckets A -- find inner join AND NIL a
		// 1. build hash for A table
		hashBucketsA := createHashBuckets(a, keys, iKeysA)

		// 2. probe hash for B table
		copyOnEmpty := func(row int) {
			ii := 0
			for _, k := range iKeysB {
				elem := bCols[k].Elem(row)
				newCols[ii].Append(elem)
				ii++
			}
			for range iNotKeysA {
				newCols[ii].Append(nil)
				ii++
			}
			for _, k := range iNotKeysB {
				elem := bCols[k].Elem(row)
				newCols[ii].Append(elem)
				ii++
			}
		}

		var onKeysEqual onKeysEqualProbe = func(foundBucketValue hashBucketValueT, row int) {
			ii := 0
			for _, k := range iKeysA {
				elem := aCols[k].Elem(foundBucketValue.row)
				newCols[ii].Append(elem)
				ii++
			}

			for _, k := range iNotKeysA {
				elem := aCols[k].Elem(foundBucketValue.row)
				newCols[ii].Append(elem)
				ii++
			}

			for _, k := range iNotKeysB {
				elem := bCols[k].Elem(row)
				newCols[ii].Append(elem)
				ii++
			}
		}

		probeHashForDataframe(b, keys, iKeysB, hashBucketsA, copyOnEmpty, onKeysEqual, copyOnEmpty)

	} // scope for [hash table a]/[probe table b]

	{ // scope for buckets B -- find NIL b

		// 1. build hash for B table
		hashBucketsB := createHashBuckets(b, keys, iKeysB)

		// 2. probe hash for A table
		copyOnEmpty := func(row int) {
			ii := 0
			for _, k := range iKeysA {
				elem := aCols[k].Elem(row)
				newCols[ii].Append(elem)
				ii++
			}
			for _, k := range iNotKeysA {
				elem := aCols[k].Elem(row)
				newCols[ii].Append(elem)
				ii++
			}
			for range iNotKeysB {
				newCols[ii].Append(nil)
				ii++
			}
		}

		probeHashForDataframe(a, keys, iKeysA, hashBucketsB, copyOnEmpty, defaultKeysEqualProbe, copyOnEmpty)

	} // scope for [hash table b]/[probe table a]

	return combineColumnsInput{
		newCols: newCols,
	}
}

func prepareLeftJoinHashForCombineColumns(joinInput prepareJoinInput) combineColumnsInput {
	// prpare input..
	a, b := joinInput.a, joinInput.b
	keys := joinInput.keys
	iKeysA, iKeysB, iNotKeysA, iNotKeysB := joinInput.iKeysA, joinInput.iKeysB, joinInput.iNotKeysA, joinInput.iNotKeysB

	aCols := a.columns
	bCols := b.columns

	newCols := joinInput.newCols

	// 1. build hash for B table
	hashBucketsB := createHashBuckets(b, keys, iKeysB)

	// 2. probe hash for A table
	var onKeysEqual onKeysEqualProbe = func(foundBucketValue hashBucketValueT, row int) {
		ii := 0

		for _, k := range iKeysA {
			elem := aCols[k].Elem(row)
			newCols[ii].Append(elem)
			ii++
		}

		for _, k := range iNotKeysA {
			elem := aCols[k].Elem(row)
			newCols[ii].Append(elem)
			ii++
		}
		for _, k := range iNotKeysB {
			elem := bCols[k].Elem(foundBucketValue.row)
			newCols[ii].Append(elem)
			ii++
		}
	}

	var onEmptyBucketProbe = func(row int) {
		ii := 0
		for _, k := range iKeysA {
			elem := aCols[k].Elem(row)
			newCols[ii].Append(elem)
			ii++
		}

		for _, k := range iNotKeysA {
			elem := aCols[k].Elem(row)
			newCols[ii].Append(elem)
			ii++
		}

		for range iNotKeysB {
			newCols[ii].Append(nil)
			ii++
		}
	}

	onKeysNotFoundProbe := onEmptyBucketProbe

	probeHashForDataframe(a, keys, iKeysA, hashBucketsB, onEmptyBucketProbe, onKeysEqual, onKeysNotFoundProbe)

	// result..
	return combineColumnsInput{
		newCols: newCols,
	}
}

//
// Helper types/func for join hashing
//

type onEmptyBucketProbe func(row int)
type onKeysEqualProbe func(foundBucketValue hashBucketValueT, row int)
type onKeysNotFoundProbe func(row int)

// for performance; better than checking callbacks for nil; TODO: recheck perf
var defaultEmptyBucketProbe onEmptyBucketProbe = func(row int) { /* no-op */ }
var defaultKeysEqualProbe onKeysEqualProbe = func(foundBucketValue hashBucketValueT, row int) { /* no-op */ }
var defaultKeysNotFoundProbe onKeysNotFoundProbe = func(row int) { /* no-op */ }

func probeHashForDataframe(
	df *DataFrame, dfKeys []string, idxKeys []int,
	hashBuckets hashBucketsT,
	onEmpty onEmptyBucketProbe, onKeysEqual onKeysEqualProbe, onKeysNotFound onKeysNotFoundProbe) {

	cols := df.columns

	for i := 0; i < df.nrows; i++ {
		var keys []series.Element
		for k := range dfKeys {
			keys = append(keys, cols[idxKeys[k]].Elem(i))
		}

		hash := hashJoinCalculation(keys, hashBuckets.maxIndex)

		bucket := hashBuckets.buckets[hash]

		if bucket.empty() {
			// no keys matches..
			onEmpty(i)
			continue
		}

		foundB := false

		// check for collision..
		for _, bucketValue := range bucket {
			keysEquals := true

			for keyIdx, aElem := range bucketValue.keys {
				bElem := cols[idxKeys[keyIdx]].Elem(i)

				if !aElem.Eq(bElem) {
					keysEquals = false
					break
				}
			}

			if keysEquals {
				foundB = true
				onKeysEqual(bucketValue, i)
				continue
			}
		}

		if !foundB {
			onKeysNotFound(i)
			continue
		}
	}
}

type hashBucketValueT struct {
	keys []series.Element // source row keys
	row  int              // source row number
}

type hashIndexT int

type hashBucketT []hashBucketValueT

func (hb hashBucketT) empty() bool {
	return len(hb) == 0
}

type hashBucketsT struct {
	buckets  []hashBucketT
	maxIndex hashIndexT
}

func hashJoinCalculation(elements []series.Element, max hashIndexT) hashIndexT {
	var hash uint32

	for _, el := range elements {
		s := el.String()

		h := fnv.New32a()
		h.Write([]byte(s))
		hash += h.Sum32()
	}

	return hashIndexT(hash % uint32(max))
}

func newHashBuckets(requestedSize int) hashBucketsT {
	buckets := make([]hashBucketT, requestedSize, requestedSize)

	return hashBucketsT{
		buckets:  buckets,
		maxIndex: hashIndexT(len(buckets) - 1),
	}
}

func createHashBuckets(df *DataFrame, keys []string, idxKeys []int) hashBucketsT {
	hashBuckets := newHashBuckets(df.nrows)

	cols := df.columns

	for i := 0; i < df.nrows; i++ {
		var keysVal []series.Element

		for k := range keys {
			keyCol := cols[idxKeys[k]]
			keysVal = append(keysVal, keyCol.Elem(i))
		}

		hash := hashJoinCalculation(keysVal, hashBuckets.maxIndex)

		hashBuckets.buckets[hash] = append(hashBuckets.buckets[hash], hashBucketValueT{keys: keysVal, row: i})
	}

	return hashBuckets
}
