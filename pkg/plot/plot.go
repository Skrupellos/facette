// Package plot provides plot-related types and methods.
package plot

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// Plot represents a graph plot.
type Plot struct {
	Time  time.Time `json:"time"`
	Value Value     `json:"value"`
}

// MarshalJSON handles JSON marshalling of the Plot type.
func (plot Plot) MarshalJSON() ([]byte, error) {
	return json.Marshal([2]interface{}{int(plot.Time.Unix()), plot.Value})
}

// UnmarshalJSON handles JSON marshalling of the Plot type.
func (plot *Plot) UnmarshalJSON(data []byte) error {
	var input [2]float64

	if err := json.Unmarshal(data, &input); err != nil {
		return err
	}

	plot.Time = time.Unix(int64(input[0]), 0)
	plot.Value = Value(input[1])

	return nil
}

// Value represents a graph plot value.
type Value float64

// MarshalJSON handles JSON marshalling of the Value type.
func (value Value) MarshalJSON() ([]byte, error) {
	// Handle NaN and near-zero values marshalling
	if math.IsNaN(float64(value)) {
		return json.Marshal(nil)
	} else if math.Exp(float64(value)) == 1 {
		return json.Marshal(0)
	}

	return json.Marshal(float64(value))
}

// IsNaN reports whether the Value is an IEEE 754 “not-a-number” value.
func (value Value) IsNaN() bool {
	return math.IsNaN(float64(value))
}

// Query represents a plot query to a connector.
type Query struct {
	Group     *QueryGroup
	StartTime time.Time
	EndTime   time.Time
	Sample    int
}

func (plotQuery *Query) String() string {
	return fmt.Sprintf(
		"Query{StartTime:%s EndTime:%s Sample:%d Group:%s}",
		plotQuery.StartTime.String(),
		plotQuery.EndTime.String(),
		plotQuery.Sample,
		plotQuery.Group,
	)
}

// QueryGroup represents a plot query operation group.
type QueryGroup struct {
	Type    int
	Series  []*QuerySeries
	Options map[string]interface{}
}

func (queryGroup *QueryGroup) String() string {
	return fmt.Sprintf(
		"QueryGroup{Type:%d Options:%v Series:[%s]}",
		queryGroup.Type,
		queryGroup.Options,
		func(series []*QuerySeries) string {
			seriesStrings := make([]string, len(series))
			for i, entry := range series {
				seriesStrings[i] = fmt.Sprintf("%s", entry)
			}

			return strings.Join(seriesStrings, ", ")
		}(queryGroup.Series),
	)
}

// QuerySeries represents a series entry in a QueryGroup.
type QuerySeries struct {
	Metric  *QueryMetric
	Options map[string]interface{}
}

func (QuerySeries *QuerySeries) String() string {
	return fmt.Sprintf(
		"QuerySeries{Metric:%s Options:%v}",
		QuerySeries.Metric,
		QuerySeries.Options,
	)
}

// QueryMetric represents a metric entry in a QuerySeries.
type QueryMetric struct {
	Name   string
	Origin string
	Source string
}

func (queryMetric *QueryMetric) String() string {
	return fmt.Sprintf(
		"QueryMetric{Name:\"%s\" Source:\"%s\" Origin:\"%s\"}",
		queryMetric.Name,
		queryMetric.Source,
		queryMetric.Origin,
	)
}

// Series represents a series of plots.
type Series struct {
	Name    string
	Plots   []Plot
	Step    int
	Summary map[string]Value
}

// Downsample applies a sampling function on a series of plots, reducing the number of points.
func (series *Series) Downsample(startTime, endTime time.Time, sample, consolidationType int) {
	consolidatedSeries, _ := ConsolidateSeries([]Series{*series}, startTime, endTime, sample, consolidationType)
	*series = consolidatedSeries[0]
}

// Scale applies a factor on a series of plots.
func (series *Series) Scale(factor Value) {
	for i := range series.Plots {
		if !series.Plots[i].Value.IsNaN() {
			series.Plots[i].Value *= factor
		}
	}
}

// Summarize calculates the min/max/average/last and percentile values of a series of plots, and stores the results
// into the Summary map.
func (series Series) Summarize(percentiles []float64) {
	var (
		min, max, total Value
		nValidPlots     int64
		nPlots          = len(series.Plots)
	)

	if nPlots > 0 {
		min = series.Plots[0].Value
		series.Summary["last"] = series.Plots[nPlots-1].Value
	}

	for i := range series.Plots {
		if !series.Plots[i].Value.IsNaN() && series.Plots[i].Value < min || min.IsNaN() {
			min = series.Plots[i].Value
		}

		if series.Plots[i].Value > max {
			max = series.Plots[i].Value
		}

		if !series.Plots[i].Value.IsNaN() {
			total += series.Plots[i].Value
			nValidPlots++
		}
	}

	series.Summary["min"] = min
	series.Summary["max"] = max
	series.Summary["avg"] = total / Value(nValidPlots)

	if len(percentiles) > 0 {
		series.Percentiles(percentiles)
	}
}

// Percentiles calculates the percentile values of a series of plots.
func (series Series) Percentiles(percentiles []float64) {
	var set []float64

	for i := range series.Plots {
		if !series.Plots[i].Value.IsNaN() {
			set = append(set, float64(series.Plots[i].Value))
		}
	}

	if len(percentiles) == 0 {
		return
	}

	setSize := len(set)
	if setSize == 0 {
		return
	}

	sort.Float64s(set)

	for _, percentile := range percentiles {
		percentileString := fmt.Sprintf("%gth", percentile)

		rank := (percentile / 100) * float64(setSize+1)
		rankInt := int(rank)
		rankFrac := rank - float64(rankInt)

		if rank <= 0.0 {
			series.Summary[percentileString] = Value(set[0])
			continue
		} else if rank-1.0 >= float64(setSize) {
			series.Summary[percentileString] = Value(set[setSize-1])
			continue
		}

		series.Summary[percentileString] = Value(set[rankInt-1] + rankFrac*(set[rankInt]-set[rankInt-1]))
	}
}
