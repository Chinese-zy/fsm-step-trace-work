package fsm

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestStepTraceAllStepsMatch(t *testing.T) {
	trace := NewStepTrace()
	trace.RecordIn(1, "idle", nil)
	trace.RecordOut(1, "running", nil)
	trace.RecordIn(2, "running", nil)
	trace.RecordOut(2, "done", nil)

	mismatches := trace.Verify([]ExpectedStep{
		{Step: 1, In: StepIO{Value: "idle", HasValue: true}, Out: StepIO{Value: "running", HasValue: true}},
		{Step: 2, In: StepIO{Value: "running", HasValue: true}, Out: StepIO{Value: "done", HasValue: true}},
	})
	if len(mismatches) != 0 {
		t.Fatalf("expected no mismatches, got %v", mismatches)
	}
}

func TestStepTraceEmptyInputMismatch(t *testing.T) {
	trace := NewStepTrace()
	trace.RecordIn(1, "", nil)
	trace.RecordOut(1, "running", nil)

	mismatches := trace.Verify([]ExpectedStep{
		{Step: 1, In: StepIO{Value: "idle", HasValue: true}, Out: StepIO{Value: "running", HasValue: true}},
	})
	if len(mismatches) != 1 {
		t.Fatalf("expected 1 mismatch, got %v", mismatches)
	}
	m := mismatches[0]
	if m.Step != 1 || m.Direction != StepIn || m.Field != "value" {
		t.Fatalf("mismatch does not point at step 1 in value: %v", m)
	}
}

func TestStepTraceFirstStepFailure(t *testing.T) {
	boom := errors.New("boom")
	trace := NewStepTrace()
	trace.RecordIn(1, "idle", nil)
	trace.RecordOut(1, nil, boom)

	mismatches := trace.Verify([]ExpectedStep{
		{Step: 1, In: StepIO{Value: "idle", HasValue: true}, Out: StepIO{Value: "running", HasValue: true}},
	})
	if len(mismatches) != 2 {
		t.Fatalf("expected 2 mismatches, got %v", mismatches)
	}
	m := mismatches[1]
	if m.Step != 1 || m.Direction != StepOut || m.Field != "error" {
		t.Fatalf("mismatch does not point at step 1 out error: %v", m)
	}
	if !strings.Contains(m.String(), "step 1 out") || !strings.Contains(m.String(), "boom") {
		t.Fatalf("mismatch text lacks step, direction or original error: %q", m.String())
	}
}

func TestStepTraceSnapshotSurvivesStateSwap(t *testing.T) {
	state := map[string]interface{}{"phase": "running"}
	trace := NewStepTrace()
	trace.RecordIn(1, state, nil)
	trace.RecordOut(1, state, nil)

	// State is swapped out halfway through the run.
	state["phase"] = "corrupted"

	mismatches := trace.Verify([]ExpectedStep{
		{
			Step: 1,
			In:   StepIO{Value: map[string]interface{}{"phase": "running"}, HasValue: true},
			Out:  StepIO{Value: map[string]interface{}{"phase": "running"}, HasValue: true},
		},
	})
	if len(mismatches) != 0 {
		t.Fatalf("snapshot changed after state swap: %v", mismatches)
	}
}

func TestStepTraceDuplicateStepNumbersKept(t *testing.T) {
	trace := NewStepTrace()
	trace.RecordIn(1, "first", nil)
	trace.RecordOut(1, "a", nil)
	trace.RecordIn(1, "second", nil)
	trace.RecordOut(1, "b", nil)

	records := trace.Records()
	if len(records) != 4 {
		t.Fatalf("expected 4 records, got %d", len(records))
	}
	if records[0].Value != "first" || records[2].Value != "second" {
		t.Fatalf("duplicate step records overwrote each other: %v", records)
	}

	mismatches := trace.Verify([]ExpectedStep{
		{Step: 1, In: StepIO{Value: "first", HasValue: true}, Out: StepIO{Value: "a", HasValue: true}},
		{Step: 1, In: StepIO{Value: "second", HasValue: true}, Out: StepIO{Value: "b", HasValue: true}},
	})
	if len(mismatches) != 0 {
		t.Fatalf("expected no mismatches, got %v", mismatches)
	}
}

func TestStepTraceValueAndErrorKeptTogether(t *testing.T) {
	boom := errors.New("boom")
	trace := NewStepTrace()
	trace.RecordIn(1, "idle", nil)
	trace.RecordOut(1, "partial", boom)

	records := trace.Records()
	out := records[1]
	if !out.HasValue || out.Value != "partial" || !out.HasErr || out.ErrText != "boom" {
		t.Fatalf("record did not keep both value and error: %+v", out)
	}

	mismatches := trace.Verify([]ExpectedStep{
		{Step: 1, In: StepIO{Value: "idle", HasValue: true}, Out: StepIO{Value: "partial", HasValue: true, Err: boom, HasErr: true}},
	})
	if len(mismatches) != 0 {
		t.Fatalf("expected no mismatches, got %v", mismatches)
	}
}

func TestStepTraceErrorMatchedByTypeAndText(t *testing.T) {
	inner := errors.New("boom")
	wrapped := fmt.Errorf("outer: %w", inner)

	trace := NewStepTrace()
	trace.RecordIn(1, "idle", nil)
	trace.RecordOut(1, nil, wrapped)

	mismatches := trace.Verify([]ExpectedStep{
		{Step: 1, In: StepIO{Value: "idle", HasValue: true}, Out: StepIO{Err: inner, HasErr: true}},
	})
	if len(mismatches) != 1 {
		t.Fatalf("wrapped error must not match inner error, got %v", mismatches)
	}
	if mismatches[0].Field != "error" {
		t.Fatalf("expected error mismatch, got %v", mismatches[0])
	}

	// Same message but different concrete type must not match either.
	trace2 := NewStepTrace()
	trace2.RecordIn(1, "idle", nil)
	trace2.RecordOut(1, nil, InvalidEventError{Event: "boom", State: "boom"})
	mismatches2 := trace2.Verify([]ExpectedStep{
		{Step: 1, In: StepIO{Value: "idle", HasValue: true}, Out: StepIO{Err: errors.New("event boom is invalid in state boom"), HasErr: true}},
	})
	if len(mismatches2) != 1 || mismatches2[0].Field != "error" {
		t.Fatalf("error type difference must be reported, got %v", mismatches2)
	}
}

func TestStepTraceOutOfOrderStepsKeepTheirNumbers(t *testing.T) {
	trace := NewStepTrace()
	// Step 2 is observed before step 1 arrives.
	trace.RecordIn(2, "running", nil)
	trace.RecordOut(2, "done", nil)
	trace.RecordIn(1, "idle", nil)
	trace.RecordOut(1, "running", nil)

	mismatches := trace.Verify([]ExpectedStep{
		{Step: 1, In: StepIO{Value: "idle", HasValue: true}, Out: StepIO{Value: "running", HasValue: true}},
		{Step: 2, In: StepIO{Value: "running", HasValue: true}, Out: StepIO{Value: "done", HasValue: true}},
	})
	if len(mismatches) != 0 {
		t.Fatalf("out of order steps mismatched: %v", mismatches)
	}

	records := trace.Records()
	if records[0].Step != 2 || records[2].Step != 1 {
		t.Fatalf("records not in registration order: %v", records)
	}
}

func TestStepTraceIncompleteStepStaysPending(t *testing.T) {
	trace := NewStepTrace()
	trace.RecordIn(1, "idle", nil)
	trace.RecordIn(2, "running", nil)
	trace.RecordOut(2, "done", nil)

	if trace.Complete(1) {
		t.Fatal("step 1 without out must not be complete")
	}
	if !trace.Complete(2) {
		t.Fatal("step 2 should be complete")
	}
	pending := trace.Pending()
	if len(pending) != 1 || pending[0] != 1 {
		t.Fatalf("expected pending [1], got %v", pending)
	}

	mismatches := trace.Verify([]ExpectedStep{
		{Step: 1, In: StepIO{Value: "idle", HasValue: true}, Out: StepIO{Value: "running", HasValue: true}},
		{Step: 2, In: StepIO{Value: "running", HasValue: true}, Out: StepIO{Value: "done", HasValue: true}},
	})
	if len(mismatches) != 1 {
		t.Fatalf("expected exactly the missing out of step 1, got %v", mismatches)
	}
	if mismatches[0].Step != 1 || mismatches[0].Direction != StepOut || mismatches[0].Actual != "<missing>" {
		t.Fatalf("missing out not reported as such: %v", mismatches[0])
	}

	// Re-running the same trace must not auto-complete the pending step.
	trace.RecordIn(1, "idle", nil)
	if trace.Complete(1) {
		t.Fatal("re-recorded in must not complete the step")
	}
}

func TestStepTraceOutAndNextInAreSeparateRecords(t *testing.T) {
	trace := NewStepTrace()
	trace.RecordOut(1, "running", nil)
	trace.RecordIn(2, "running", nil)

	records := trace.Records()
	if len(records) != 2 {
		t.Fatalf("equal out/in values must stay two records, got %v", records)
	}
	if records[0].Direction != StepOut || records[1].Direction != StepIn {
		t.Fatalf("directions merged: %v", records)
	}
}

func TestStepTraceRegistrationOrderNotMapOrder(t *testing.T) {
	trace := NewStepTrace()
	for _, step := range []int{3, 1, 2, 5, 4} {
		trace.RecordIn(step, step, nil)
	}
	records := trace.Records()
	for i, step := range []int{3, 1, 2, 5, 4} {
		if records[i].Step != step || records[i].Seq != i {
			t.Fatalf("records not in registration order: %v", records)
		}
	}
}

func TestStepTraceRerunProducesIdenticalRecords(t *testing.T) {
	run := func() []StepRecord {
		trace := NewStepTrace()
		trace.RecordIn(1, "idle", nil)
		trace.RecordOut(1, "running", errors.New("boom"))
		trace.RecordIn(2, map[string]interface{}{"phase": "running"}, nil)
		trace.RecordOut(2, []string{"done"}, nil)
		return trace.Records()
	}
	first := run()
	second := run()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("rerun records differ:\n%v\n%v", first, second)
	}
}

func TestStepTraceUnexpectedExtraRecord(t *testing.T) {
	trace := NewStepTrace()
	trace.RecordIn(1, "idle", nil)
	trace.RecordOut(1, "running", nil)
	trace.RecordOut(1, "extra", nil)

	mismatches := trace.Verify([]ExpectedStep{
		{Step: 1, In: StepIO{Value: "idle", HasValue: true}, Out: StepIO{Value: "running", HasValue: true}},
	})
	if len(mismatches) != 1 || mismatches[0].Field != "record" {
		t.Fatalf("extra record not reported, got %v", mismatches)
	}
}
