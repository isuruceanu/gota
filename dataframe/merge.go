package dataframe

import (
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/isuruceanu/gota/series"
)

type combineFuncType func(a, b series.Series) bool
type combineHeaderBuilderFuncType func(a, b series.Series) (string, interface{}, bool)

// Merge struct definition
type Merge struct {
	a                     DataFrame
	b                     DataFrame
	keys                  []string
	combine               bool
	combineCompareFn      combineFuncType
	combineResultHeaderFn combineHeaderBuilderFuncType
}

// Merge returns a Merge struct for containing ifo about merge
func (df DataFrame) Merge(b DataFrame, keys ...string) Merge {
	return Merge{a: df, b: b, keys: keys}
}

// WithCombine specify to merge same columns into one
func (m Merge) WithCombine(fn func(aSerie, bSerie series.Series) bool) Merge {
	m.combine = true
	m.combineCompareFn = fn
	return m
}

func (m Merge) WithResultHeader(fn func(a, b series.Series) (string, interface{}, bool)) Merge {
	m.combineResultHeaderFn = fn
	return m
}

func (m Merge) OuterJoin() DataFrame {
	if m.combine {
		return m.a.outerJoinWithCombine(m.b, m.combineCompareFn, m.combineResultHeaderFn, m.keys...)
	}
	return m.a.outerJoinWithCombine(m.b, nil, nil, m.keys...)
}

func (m Merge) RightJoin() DataFrame {
	if m.combine {
		return m.a.rightJoinWithCombine(m.b, m.combineCompareFn, m.combineResultHeaderFn, m.keys...)
	}
	return m.a.rightJoinWithCombine(m.b, nil, nil, m.keys...)
}

func (m Merge) InnerJoin() DataFrame {
	if m.combine {
		return m.a.innerJoinWithCombine(m.b, m.combineCompareFn, m.combineResultHeaderFn, m.keys...)
	}

	return m.a.innerJoinWithCombine(m.b, nil, nil, m.keys...)
}

func (m Merge) LeftJoin() DataFrame {
	if m.combine {
		return m.a.leftJoinWithCombine(m.b, m.combineCompareFn, m.combineResultHeaderFn, m.keys...)
	}
	return m.a.leftJoinWithCombine(m.b, nil, nil, m.keys...)
}

type tuple struct {
	aIdx  int
	bIdx  int
	rAIdx int
	rBIdx int
}

type tupleArr []tuple

func (t tupleArr) findTuple(val int, fn func(int, tuple) bool) (int, bool) {
	for idx, v := range t {
		if fn(val, v) {
			return idx, true
		}
	}
	return -1, false
}

func (df DataFrame) outerJoinWithCombine(b DataFrame,
	compareFn combineFuncType,
	combineHeaderBuilder combineHeaderBuilderFuncType,
	keys ...string) DataFrame {

	iKeysA, iKeysB, errorArr := checkDataframesForJoins(df, b, keys...)
	if len(errorArr) != 0 {
		return DataFrame{Err: fmt.Errorf(strings.Join(errorArr, "\n"))}
	}

	aCols := df.columns
	bCols := b.columns
	// Initialize newCols
	var newCols []series.Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}

	var iCombinedCols tupleArr

	if compareFn != nil {
		for i := 0; i < df.ncols; i++ {
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
	for i := 0; i < df.ncols; i++ {
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

	// Fill newCols
	for i := 0; i < df.nrows; i++ {
		matched := false
		for j := 0; j < b.nrows; j++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
				ii := 0
				for _, k := range iKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysB {
					elem := bCols[k].Elem(j)
					newCols[ii].Append(elem)
					ii++
				}
			}
		}
		if !matched {
			ii := 0
			for _, k := range iKeysA {
				elem := aCols[k].Elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for _, k := range iNotKeysA {
				elem := aCols[k].Elem(i)
				newCols[ii].Append(elem)
				ii++
			}
			for _ = range iNotKeysB {
				newCols[ii].Append(nil)
				ii++
			}
		}
	}
	for j := 0; j < b.nrows; j++ {
		matched := false
		for i := 0; i < df.nrows; i++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
			}
		}
		if !matched {
			ii := 0
			for _, k := range iKeysB {
				elem := bCols[k].Elem(j)
				newCols[ii].Append(elem)
				ii++
			}
			for _ = range iNotKeysA {
				newCols[ii].Append(nil)
				ii++
			}
			for _, k := range iNotKeysB {
				elem := bCols[k].Elem(j)
				newCols[ii].Append(elem)
				ii++
			}
		}
	}
	newCols = combineColumns(iCombinedCols, newCols, combineHeaderBuilder)
	return New(newCols...)
}

func (df DataFrame) rightJoinWithCombine(b DataFrame, compareFn combineFuncType,
	combineHeaderBuilder combineHeaderBuilderFuncType,
	keys ...string) DataFrame {

	iKeysA, iKeysB, errorArr := checkDataframesForJoins(df, b, keys...)
	if len(errorArr) != 0 {
		return DataFrame{Err: fmt.Errorf(strings.Join(errorArr, "\n"))}
	}

	aCols := df.columns
	bCols := b.columns
	// Initialize newCols
	var newCols []series.Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}

	var iCombinedCols tupleArr

	if compareFn != nil {
		for i := 0; i < df.ncols; i++ {
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
	for i := 0; i < df.ncols; i++ {
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

	// Fill newCols
	var yesmatched []struct{ i, j int }
	var nonmatched []int
	for j := 0; j < b.nrows; j++ {
		matched := false
		for i := 0; i < df.nrows; i++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
				yesmatched = append(yesmatched, struct{ i, j int }{i, j})
			}
		}
		if !matched {
			nonmatched = append(nonmatched, j)
		}
	}
	for _, v := range yesmatched {
		i := v.i
		j := v.j
		ii := 0
		for _, k := range iKeysA {
			elem := aCols[k].Elem(i)
			newCols[ii].Append(elem)
			ii++
		}
		for _, k := range iNotKeysA {
			elem := aCols[k].Elem(i)
			newCols[ii].Append(elem)
			ii++
		}
		for _, k := range iNotKeysB {
			elem := bCols[k].Elem(j)
			newCols[ii].Append(elem)
			ii++
		}
	}
	for _, j := range nonmatched {
		ii := 0
		for _, k := range iKeysB {
			elem := bCols[k].Elem(j)
			newCols[ii].Append(elem)
			ii++
		}
		for _ = range iNotKeysA {
			newCols[ii].Append(nil)
			ii++
		}
		for _, k := range iNotKeysB {
			elem := bCols[k].Elem(j)
			newCols[ii].Append(elem)
			ii++
		}
	}
	newCols = combineColumns(iCombinedCols, newCols, combineHeaderBuilder)
	return New(newCols...)
}

// InnerJoin returns a DataFrame containing the inner join of two DataFrames.
func (df DataFrame) innerJoinWithCombine(b DataFrame, compareFn combineFuncType,
	combineHeaderBuilder combineHeaderBuilderFuncType,
	keys ...string) DataFrame {

	iKeysA, iKeysB, errorArr := checkDataframesForJoins(df, b, keys...)
	if len(errorArr) != 0 {
		return DataFrame{Err: fmt.Errorf("%v", strings.Join(errorArr, "\n"))}
	}

	aCols := df.columns
	bCols := b.columns
	// Initialize newCols
	var newCols []series.Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}

	var iCombinedCols tupleArr

	if compareFn != nil {
		for i := 0; i < df.ncols; i++ {
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
	for i := 0; i < df.ncols; i++ {
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

	// Fill newCols
	for i := 0; i < df.nrows; i++ {
		for j := 0; j < b.nrows; j++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				ii := 0
				for _, k := range iKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysB {
					elem := bCols[k].Elem(j)
					newCols[ii].Append(elem)
					ii++
				}
			}
		}
	}

	newCols = combineColumns(iCombinedCols, newCols, combineHeaderBuilder)
	return New(newCols...)
}

func (df DataFrame) leftJoinWithCombine(b DataFrame, compareFn combineFuncType,
	combineHeaderBuilder combineHeaderBuilderFuncType,
	keys ...string) DataFrame {

	iKeysA, iKeysB, errorArr := checkDataframesForJoins(df, b, keys...)

	if len(errorArr) != 0 {
		return DataFrame{Err: fmt.Errorf(strings.Join(errorArr, "\n"))}
	}

	aCols := df.columns
	bCols := b.columns
	// Initialize newCols
	var newCols []series.Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}

	var iCombinedCols tupleArr

	if compareFn != nil {
		for i := 0; i < df.ncols; i++ {
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
	for i := 0; i < df.ncols; i++ {
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

	// Fill newCols
	for i := 0; i < df.nrows; i++ {
		matched := false
		for j := 0; j < b.nrows; j++ {
			match := true
			for k := range keys {
				aElem := aCols[iKeysA[k]].Elem(i)
				bElem := bCols[iKeysB[k]].Elem(j)
				match = match && aElem.Eq(bElem)
			}
			if match {
				matched = true
				ii := 0
				for _, k := range iKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}

				for _, k := range iNotKeysA {
					elem := aCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}
				for _, k := range iNotKeysB {
					elem := bCols[k].Elem(j)
					newCols[ii].Append(elem)
					ii++
				}
			}
		}
		if !matched {
			ii := 0
			for _, k := range iKeysA {
				elem := aCols[k].Elem(i)
				newCols[ii].Append(elem)
				ii++
			}

			for _, k := range iNotKeysA {
				elem := aCols[k].Elem(i)
				newCols[ii].Append(elem)
				ii++
			}

			for range iNotKeysB {
				newCols[ii].Append(nil)
				ii++
			}
		}
	}

	newCols = combineColumns(iCombinedCols, newCols, combineHeaderBuilder)

	return New(newCols...)
}

func combineColumns(iCombinedCols tupleArr, newCols []series.Series, headerBuilderFn combineHeaderBuilderFuncType) []series.Series {
	for _, c := range iCombinedCols {
		if c.rAIdx == -1 || c.rBIdx == -1 {
			continue
		}

		cobCol := newCols[c.rAIdx].Combine(newCols[c.rBIdx])

		if cobCol.Err == nil {

			if headerBuilderFn != nil {
				name, otherInfo, ignore := headerBuilderFn(newCols[c.rAIdx], newCols[c.rBIdx])
				if !ignore {
					cobCol.Name = name
					cobCol.OtherInfo = otherInfo
				}
			}

			newCols[c.rAIdx] = cobCol
		}
	}
	result := []series.Series{}

	for idx, s := range newCols {
		if _, ok := iCombinedCols.findTuple(idx, findInRB); ok {
			continue
		}
		result = append(result, s)
	}

	return result
}

func checkDataframesForJoins(a, b DataFrame, keys ...string) ([]int, []int, []string) {
	if len(keys) == 0 {
		return nil, nil, []string{"join keys not specified"}
	}
	// Check that we have all given keys in both DataFrames
	errorArr := []string{}
	var iKeysA []int
	var iKeysB []int
	for _, key := range keys {
		i := a.ColIndex(key)
		if i < 0 {
			errorArr = append(errorArr, fmt.Sprint("can't find key \"", key, "\" on left DataFrame"))
		}
		iKeysA = append(iKeysA, i)
		j := b.ColIndex(key)
		if j < 0 {
			errorArr = append(errorArr, fmt.Sprint("can't find key \"", key, "\" on right DataFrame"))
		}
		iKeysB = append(iKeysB, j)
	}
	return iKeysA, iKeysB, errorArr
}

var (
	findInA = func(val int, t tuple) bool {
		return val == t.aIdx
	}

	findInB = func(val int, t tuple) bool {
		return val == t.bIdx
	}

	findInRB = func(val int, t tuple) bool {
		return val == t.rBIdx
	}
)

// InnerJoin returns a DataFrame containing the inner join of two DataFrames.
func (df DataFrame) innerJoinHashWithCombine(b DataFrame, compareFn combineFuncType, combineHeaderBuilder combineHeaderBuilderFuncType, keys ...string) DataFrame {

	iKeysA, iKeysB, errorArr := checkDataframesForJoins(df, b, keys...)
	if len(errorArr) != 0 {
		return DataFrame{Err: fmt.Errorf("%v", strings.Join(errorArr, "\n"))}
	}

	aCols := df.columns
	bCols := b.columns

	// Initialize newCols
	var newCols []series.Series
	for _, i := range iKeysA {
		newCols = append(newCols, aCols[i].Empty())
	}

	var iCombinedCols tupleArr

	if compareFn != nil {
		for i := 0; i < df.ncols; i++ {
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
	for i := 0; i < df.ncols; i++ {
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
		a:         &df,
		b:         &b,
		keys:      keys,
		iKeysA:    iKeysA,
		iKeysB:    iKeysB,
		iNotKeysA: iNotKeysA,
		iNotKeysB: iNotKeysB,
		newCols:   newCols,
	}
	combineColumnsInput := prepareInnerJoinHashForCombineColumns(joinInput)
	newCols = combineColumns(iCombinedCols, combineColumnsInput.newCols, combineHeaderBuilder)

	return New(newCols...)
}

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
	a         *DataFrame // first input dataframe
	b         *DataFrame // second input dataframe
	keys      []string   // keys for join
	iKeysA    []int      // indexes from first dataframe for key columns
	iKeysB    []int      // indexes from second dataframe for key columns
	iNotKeysA []int      // indexes from first dataframe for non-key columns
	iNotKeysB []int      // indexes from second dataframe for non-key columns
	newCols   []series.Series
}

func prepareInnerJoinHashForCombineColumns(joinInput prepareJoinInput) combineColumnsInput {
	// prpare input..
	a, b := joinInput.a, joinInput.b
	keys := joinInput.keys
	iKeysA, iKeysB, iNotKeysA, iNotKeysB := joinInput.iKeysA, joinInput.iKeysB, joinInput.iNotKeysA, joinInput.iNotKeysB

	aCols := a.columns
	bCols := b.columns

	newCols := joinInput.newCols

	//
	// types for join
	//

	// TODO: select table with min rows for hashing
	type keysT struct {
		values []series.Element
		row    int
	}
	type keyValuesT []keysT
	type hashHolderT [1024 * 64]keyValuesT // TODO: dynamic length
	type hashIndexT int

	hashCalculation := func(elements []series.Element, max hashIndexT) hashIndexT {
		var hash uint32

		for _, el := range elements {
			s := el.String()

			h := fnv.New32a()
			h.Write([]byte(s))
			hash += h.Sum32()
		}

		return hashIndexT(hash % uint32(max))
	}

	// 1. build hash
	var hashBuckets hashHolderT
	var maxIndex hashIndexT = hashIndexT(len(hashBuckets) - 1)

	for i := 0; i < a.nrows; i++ {
		var keysA []series.Element
		for k := range keys {
			keysA = append(keysA, aCols[iKeysA[k]].Elem(i))
		}

		hashA := hashCalculation(keysA, maxIndex)

		newKv := keysT{values: keysA, row: i}
		hashBuckets[hashA] = append(hashBuckets[hashA], newKv)
	}

	// 2. probe hash
	for i := 0; i < b.nrows; i++ {
		var keysB []series.Element
		for k := range keys {
			keysB = append(keysB, bCols[iKeysB[k]].Elem(i))
		}

		hashB := hashCalculation(keysB, maxIndex)
		buckets := hashBuckets[hashB]

		if len(buckets) == 0 {
			continue // no key matches
		}

		// check for collision..
		for _, aKeyValues := range buckets {
			areEquals := true

			for keyIdx, aElem := range aKeyValues.values {
				bElem := bCols[iKeysB[keyIdx]].Elem(i)

				if !aElem.Eq(bElem) {
					areEquals = false
					break
				}
			}

			if areEquals {
				ii := 0
				for _, k := range iKeysA {
					elem := aCols[k].Elem(aKeyValues.row)
					newCols[ii].Append(elem)
					ii++
				}

				for _, k := range iNotKeysA {
					elem := aCols[k].Elem(aKeyValues.row)
					newCols[ii].Append(elem)
					ii++
				}

				for _, k := range iNotKeysB {
					elem := bCols[k].Elem(i)
					newCols[ii].Append(elem)
					ii++
				}

				continue
			}

		}
	}

	return combineColumnsInput{
		newCols: newCols,
	}
}
