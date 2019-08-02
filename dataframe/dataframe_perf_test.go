package dataframe

import (
	"testing"
	"time"
)

func loadDataframesForPerformance(t *testing.T) (DataFrame, DataFrame) {
	df1 := readCvsFile("r35k-c73.csv") // ID: 21..35000
	df2 := readCvsFile("r30k-c4.csv")  // ID: 1..30000

	if df1.Err != nil {
		t.Errorf("df1 not loaded!")
		return df1, df2
	}

	if df2.Err != nil {
		t.Errorf("df2 not loaded!")
		return df1, df2
	}

	t.Logf("\nInput Df1, Rows: %v, Cols: %v ..\n", df1.Nrow(), df1.Ncol())
	t.Logf("\nInput Df2, Rows: %v, Cols: %v ..\n", df2.Nrow(), df2.Ncol())

	return df1, df2
}

//
// cmd-line: go test -timeout 1200s github.com/isuruceanu/gota/dataframe -run ^TestDataFrame_InnerJoinHash_Performance$ -test.v
//
// Rename this test for using - remove starting underscore (was renamed only because long-running).
//
func _TestDataFrame_InnerJoinHash_Performance(t *testing.T) {
	df1, df2 := loadDataframesForPerformance(t)

	for i := 0; i < 5; i++ {
		const keyField = "ID"

		startJoinHash := time.Now()
		hashResult := df1.InnerJoinHash(df2, keyField)
		elapsedJoinHash := time.Since(startJoinHash)

		startJoin := time.Now()
		joinResult := df1.InnerJoin(df2, keyField)
		elapsedJoin := time.Since(startJoin)

		// writeCsvFile("InnerJoinHash.csv", hashResult)

		if hashResult.Err != nil {
			t.Errorf("InnerJoinHash failed: %v", hashResult.Err)
			return
		}

		if joinResult.Err != nil {
			t.Errorf("InnerJoin failed: %v", joinResult.Err)
			return
		}

		if hashResult.Nrow() != joinResult.Nrow() {
			t.Errorf("Different rows count! InnerJoinHash: %v, InnerJoin: %v", hashResult.Nrow(), joinResult.Nrow())
			return
		}

		if hashResult.Ncol() != joinResult.Ncol() {
			t.Errorf("Different cols count! InnerJoinHash: %v, InnerJoin: %v", hashResult.Ncol(), joinResult.Ncol())
			return
		}

		t.Logf("\nInnerJoinHash: %v, InnerJoin: %v\n", elapsedJoinHash, elapsedJoin)
	}
}

//
// cmd-line: go test -timeout 1200s github.com/isuruceanu/gota/dataframe -run ^TestDataFrame_OuterJoinHash_Performance$ -test.v
//
// Rename this test for using - remove starting underscore (was renamed only because long-running).
//
func _TestDataFrame_OuterJoinHash_Performance(t *testing.T) {
	df1, df2 := loadDataframesForPerformance(t)

	for i := 0; i < 3; i++ {
		const keyField = "ID"

		startJoinHash := time.Now()
		hashResult := df1.OuterJoinHash(df2, keyField)
		elapsedJoinHash := time.Since(startJoinHash)

		startJoin := time.Now()
		joinResult := df1.OuterJoin(df2, keyField)
		elapsedJoin := time.Since(startJoin)

		// writeCsvFile("OuterJoinHash.csv", hashResult)

		if hashResult.Err != nil {
			t.Errorf("OuterJoinHash failed: %v", hashResult.Err)
			return
		}

		if joinResult.Err != nil {
			t.Errorf("OuterJoin failed: %v", joinResult.Err)
			return
		}

		if hashResult.Nrow() != joinResult.Nrow() {
			t.Errorf("Different rows count! OuterJoinHash: %v, OuterJoin: %v", hashResult.Nrow(), joinResult.Nrow())
			return
		}

		if hashResult.Ncol() != joinResult.Ncol() {
			t.Errorf("Different cols count! OuterJoinHash: %v, OuterJoin: %v", hashResult.Ncol(), joinResult.Ncol())
			return
		}

		t.Logf("\nOuterJoinHash: %v, OuterJoin: %v\n", elapsedJoinHash, elapsedJoin)
	}
}

//
// cmd-line: go test -timeout 1200s github.com/isuruceanu/gota/dataframe -run ^TestDataFrame_LeftJoinHash_Performance$ -test.v
//
// Rename this test for using - remove starting underscore (was renamed only because long-running).
//
func _TestDataFrame_LeftJoinHash_Performance(t *testing.T) {
	df1, df2 := loadDataframesForPerformance(t)

	for i := 0; i < 3; i++ {
		const keyField = "ID"

		startJoinHash := time.Now()
		hashResult := df1.LeftJoinHash(df2, keyField)
		elapsedJoinHash := time.Since(startJoinHash)

		startJoin := time.Now()
		joinResult := df1.LeftJoin(df2, keyField)
		elapsedJoin := time.Since(startJoin)

		// writeCsvFile("LeftJoinHash.csv", hashResult)

		if hashResult.Err != nil {
			t.Errorf("LeftJoinHash failed: %v", hashResult.Err)
			return
		}

		if joinResult.Err != nil {
			t.Errorf("LeftJoin failed: %v", joinResult.Err)
			return
		}

		if hashResult.Nrow() != joinResult.Nrow() {
			t.Errorf("Different rows count! LeftJoinHash: %v, LeftJoin: %v", hashResult.Nrow(), joinResult.Nrow())
			return
		}

		if hashResult.Ncol() != joinResult.Ncol() {
			t.Errorf("Different cols count! LeftJoinHash: %v, LeftJoin: %v", hashResult.Ncol(), joinResult.Ncol())
			return
		}

		t.Logf("\nLeftJoinHash: %v, LeftJoin: %v\n", elapsedJoinHash, elapsedJoin)
	}
}

//
// cmd-line: go test -timeout 1200s github.com/isuruceanu/gota/dataframe -run ^TestDataFrame_RightJoinHash_Performance$ -test.v
//
// Rename this test for using - remove starting underscore (was renamed only because long-running).
//
func _TestDataFrame_RightJoinHash_Performance(t *testing.T) {
	df1, df2 := loadDataframesForPerformance(t)

	for i := 0; i < 3; i++ {
		const keyField = "ID"

		startJoinHash := time.Now()
		hashResult := df1.RightJoinHash(df2, keyField)
		elapsedJoinHash := time.Since(startJoinHash)

		startJoin := time.Now()
		joinResult := df1.RightJoin(df2, keyField)
		elapsedJoin := time.Since(startJoin)

		// writeCsvFile("RightJoinHash.csv", hashResult)

		if hashResult.Err != nil {
			t.Errorf("RightJoinHash failed: %v", hashResult.Err)
			return
		}

		if joinResult.Err != nil {
			t.Errorf("RightJoin failed: %v", joinResult.Err)
			return
		}

		if hashResult.Nrow() != joinResult.Nrow() {
			t.Errorf("Different rows count! RightJoinHash: %v, RightJoin: %v", hashResult.Nrow(), joinResult.Nrow())
			return
		}

		if hashResult.Ncol() != joinResult.Ncol() {
			t.Errorf("Different cols count! RightJoinHash: %v, RightJoin: %v", hashResult.Ncol(), joinResult.Ncol())
			return
		}

		t.Logf("\nRightJoinHash: %v, RightJoin: %v\n", elapsedJoinHash, elapsedJoin)
	}
}
