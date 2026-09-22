// Copyright (c) 2013 - Max Persson <max@looplab.se>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fsm

import (
	"fmt"
	"reflect"
	"sync"
)

// StepDirection identifies which side of a step a record belongs to.
type StepDirection int

const (
	// StepIn is the input side of a step.
	StepIn StepDirection = iota
	// StepOut is the output side of a step.
	StepOut
)

// String returns a human readable name of the direction.
func (d StepDirection) String() string {
	if d == StepIn {
		return "in"
	}
	return "out"
}

// StepIO describes the expected value and/or error of one side of a step.
// A side may carry both a value and an error at the same time.
type StepIO struct {
	Value    interface{}
	Err      error
	HasValue bool
	HasErr   bool
}

// StepRecord is an immutable snapshot of one recorded side of a step.
// Values are deep copied and errors are reduced to their concrete type and
// message at record time, so later state changes cannot alter the record.
type StepRecord struct {
	// Seq is the registration order of the record within the trace.
	Seq int
	// Step is the caller supplied step number.
	Step int
	// Direction tells whether this is the in or the out side of the step.
	Direction StepDirection
	// Value is the snapshot of the recorded value, valid when HasValue is set.
	Value interface{}
	// ErrType is the concrete type of the recorded error, valid when HasErr is set.
	ErrType string
	// ErrText is the full message of the recorded error, valid when HasErr is set.
	ErrText  string
	HasValue bool
	HasErr   bool
}

// StepTrace collects per step in/out records in registration order.
// It is safe for concurrent use.
type StepTrace struct {
	mu      sync.Mutex
	records []StepRecord
}

// NewStepTrace creates an empty StepTrace.
func NewStepTrace() *StepTrace {
	return &StepTrace{}
}

// RecordIn snapshots the input side of the given step.
func (t *StepTrace) RecordIn(step int, value interface{}, err error) {
	t.record(step, StepIn, value, err)
}

// RecordOut snapshots the output side of the given step.
func (t *StepTrace) RecordOut(step int, value interface{}, err error) {
	t.record(step, StepOut, value, err)
}

func (t *StepTrace) record(step int, dir StepDirection, value interface{}, err error) {
	rec := StepRecord{
		Step:      step,
		Direction: dir,
		Value:     snapshotValue(value),
		HasValue:  value != nil,
	}
	if err != nil {
		rec.HasErr = true
		rec.ErrType = reflect.TypeOf(err).String()
		rec.ErrText = err.Error()
	}
	t.mu.Lock()
	rec.Seq = len(t.records)
	t.records = append(t.records, rec)
	t.mu.Unlock()
}

// Records returns a copy of all records in registration order. Records with
// the same step number are all kept; none overwrites another.
func (t *StepTrace) Records() []StepRecord {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]StepRecord, len(t.records))
	copy(out, t.records)
	return out
}

// Complete reports whether both the in and the out side of the given step
// have been recorded.
func (t *StepTrace) Complete(step int) bool {
	var hasIn, hasOut bool
	for _, rec := range t.Records() {
		if rec.Step != step {
			continue
		}
		if rec.Direction == StepIn {
			hasIn = true
		} else {
			hasOut = true
		}
	}
	return hasIn && hasOut
}

// Pending returns the step numbers that have only one side recorded, in
// registration order and without duplicates.
func (t *StepTrace) Pending() []int {
	records := t.Records()
	seen := map[int]bool{}
	var order []int
	for _, rec := range records {
		if !seen[rec.Step] {
			seen[rec.Step] = true
			order = append(order, rec.Step)
		}
	}
	var pending []int
	for _, step := range order {
		var hasIn, hasOut bool
		for _, rec := range records {
			if rec.Step != step {
				continue
			}
			if rec.Direction == StepIn {
				hasIn = true
			} else {
				hasOut = true
			}
		}
		if hasIn != hasOut {
			pending = append(pending, step)
		}
	}
	return pending
}

// ExpectedStep describes the expected in and out of one step.
type ExpectedStep struct {
	Step int
	In   StepIO
	Out  StepIO
}

// Mismatch describes a single divergence between the expected and the
// recorded trace. It always carries the step number and the direction of
// the side that did not match.
type Mismatch struct {
	Step      int
	Direction StepDirection
	Field     string
	Expected  string
	Actual    string
}

// String renders the mismatch with step number, direction and field.
func (m Mismatch) String() string {
	return fmt.Sprintf("step %d %s %s: expected %s, got %s",
		m.Step, m.Direction, m.Field, m.Expected, m.Actual)
}

// Verify compares the recorded trace against the expected steps, one side at
// a time. Expected steps are matched to records by step number and order of
// occurrence, never by map iteration or by position in the trace. A step
// whose out side was never recorded is reported as missing, not as passed.
func (t *StepTrace) Verify(expected []ExpectedStep) []Mismatch {
	records := t.Records()
	used := make([]bool, len(records))
	var mismatches []Mismatch

	find := func(step int, dir StepDirection) int {
		for i, rec := range records {
			if !used[i] && rec.Step == step && rec.Direction == dir {
				used[i] = true
				return i
			}
		}
		return -1
	}

	for _, exp := range expected {
		mismatches = append(mismatches, verifySide(records, find, exp.Step, StepIn, exp.In)...)
		mismatches = append(mismatches, verifySide(records, find, exp.Step, StepOut, exp.Out)...)
	}
	for i, rec := range records {
		if !used[i] {
			mismatches = append(mismatches, Mismatch{
				Step:      rec.Step,
				Direction: rec.Direction,
				Field:     "record",
				Expected:  "<none>",
				Actual:    renderRecord(rec),
			})
		}
	}
	return mismatches
}

func verifySide(records []StepRecord, find func(step int, dir StepDirection) int, step int, dir StepDirection, exp StepIO) []Mismatch {
	idx := find(step, dir)
	if idx < 0 {
		return []Mismatch{{
			Step:      step,
			Direction: dir,
			Field:     "record",
			Expected:  renderIO(exp),
			Actual:    "<missing>",
		}}
	}
	rec := records[idx]
	var mismatches []Mismatch
	if exp.HasValue != rec.HasValue || (exp.HasValue && !reflect.DeepEqual(exp.Value, rec.Value)) {
		mismatches = append(mismatches, Mismatch{
			Step:      step,
			Direction: dir,
			Field:     "value",
			Expected:  renderValue(exp.Value, exp.HasValue),
			Actual:    renderValue(rec.Value, rec.HasValue),
		})
	}
	expType, expText := renderErr(exp.Err, exp.HasErr)
	if exp.HasErr != rec.HasErr || (exp.HasErr && (expType != rec.ErrType || expText != rec.ErrText)) {
		mismatches = append(mismatches, Mismatch{
			Step:      step,
			Direction: dir,
			Field:     "error",
			Expected:  renderErrString(expType, expText, exp.HasErr),
			Actual:    renderErrString(rec.ErrType, rec.ErrText, rec.HasErr),
		})
	}
	return mismatches
}

func renderIO(io StepIO) string {
	errType, errText := renderErr(io.Err, io.HasErr)
	return fmt.Sprintf("value=%s error=%s",
		renderValue(io.Value, io.HasValue),
		renderErrString(errType, errText, io.HasErr))
}

func renderRecord(rec StepRecord) string {
	return fmt.Sprintf("value=%s error=%s",
		renderValue(rec.Value, rec.HasValue),
		renderErrString(rec.ErrType, rec.ErrText, rec.HasErr))
}

func renderValue(value interface{}, ok bool) string {
	if !ok {
		return "<none>"
	}
	return fmt.Sprintf("%#v", value)
}

func renderErr(err error, ok bool) (string, string) {
	if !ok || err == nil {
		return "", ""
	}
	return reflect.TypeOf(err).String(), err.Error()
}

func renderErrString(errType, errText string, ok bool) string {
	if !ok {
		return "<none>"
	}
	return fmt.Sprintf("%s: %s", errType, errText)
}

// snapshotValue deep copies maps and slices so that later mutations of the
// recorded state cannot change the stored record.
func snapshotValue(value interface{}) interface{} {
	switch v := value.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(v))
		for key, item := range v {
			out[key] = snapshotValue(item)
		}
		return out
	case map[string]string:
		out := make(map[string]string, len(v))
		for key, item := range v {
			out[key] = item
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(v))
		for i, item := range v {
			out[i] = snapshotValue(item)
		}
		return out
	case []string:
		out := make([]string, len(v))
		copy(out, v)
		return out
	default:
		return value
	}
}
