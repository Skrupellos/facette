package utils

import (
	"testing"
	"time"
)

func Test_TimeApplyRange(test *testing.T) {
	refTime := time.Now()

	if result, _ := TimeApplyRange(refTime, "-1h"); result != refTime.Add(-1*time.Hour) {
		test.Logf("\nExpected %#v\nbut got  %#v", refTime.Add(-1*time.Hour), result)
		test.Fail()
	}

	if result, _ := TimeApplyRange(refTime, "2mo"); result != refTime.AddDate(0, 2, 0) {
		test.Logf("\nExpected %#v\nbut got  %#v", refTime.AddDate(0, 2, 0), result)
		test.Fail()
	}

	if result, _ := TimeApplyRange(refTime, "-1y 3h 126s"); result != refTime.AddDate(-1, 0, 0).
		Add(-3*time.Hour-126*time.Second) {
		test.Logf("\nExpected %#v\nbut got  %#v", refTime.AddDate(-1, 0, 0).Add(-3*time.Hour+126*time.Second), result)
		test.Fail()
	}

	if result, _ := TimeApplyRange(refTime, "3d 1h 6m"); result != refTime.AddDate(0, 0, 3).
		Add(time.Hour+6*time.Minute) {
		test.Logf("\nExpected %#v\nbut got  %#v", refTime.AddDate(0, 0, 3).Add(time.Hour+6*time.Minute), result)
		test.Fail()
	}
}

func Test_DurationToRange(test *testing.T) {
	refTime := time.Now()

	if result := DurationToRange(refTime.Sub(refTime.Add(time.Hour))); result != "-1h" {
		test.Logf("\nExpected %#v\nbut got  %#v", "-1h", result)
		test.Fail()
	}

	if result := DurationToRange(refTime.Sub(refTime.AddDate(0, 0, 60)) * -1); result != "60d" {
		test.Logf("\nExpected %#v\nbut got  %#v", "60d", result)
		test.Fail()
	}

	if result := DurationToRange(refTime.Sub(refTime.AddDate(0, 0, 1).Add(3*time.Hour +
		126*time.Second))); result != "-1d 3h 2m 6s" {
		test.Logf("\nExpected %#v\nbut got  %#v", "-1d 3h 2m 6s", result)
		test.Fail()
	}

	if result := DurationToRange(refTime.Sub(refTime.AddDate(0, 0, 3).Add(time.Hour+
		6*time.Minute)) * -1); result != "3d 1h 6m" {
		test.Logf("\nExpected %#v\nbut got  %#v", "3d 1h 6m", result)
		test.Fail()
	}
}
