package fsm

import (
	"context"
	"errors"
	"testing"
)

// newTracedFSM builds a fixed FSM used by every test: no clocks, no network.
//
//	closed --open--> open --close--> closed
//	open   --explode--> (leave_open callback cancels with a wrapped error)
func newTracedFSM(callbacks Callbacks) (*FSM, *StepTracer) {
	f := NewFSM("closed", Events{
		{Name: "open", Src: []string{"closed"}, Dst: "open"},
		{Name: "close", Src: []string{"open"}, Dst: "closed"},
		{Name: "explode", Src: []string{"open"}, Dst: "closed"},
	}, callbacks)
	return f, NewStepTracer(f)
}

func TestAllStepsHappyPath(t *testing.T) {
	f, tr := newTracedFSM(nil)
	tr.Register(1, StepExpectation{"closed", "open", "open", nil})
	tr.Register(2, StepExpectation{"open", "close", "closed", nil})

	tr.StartRun()
	if err := tr.Step(context.Background(), 1, "open"); err != nil {
		t.Fatalf("step 1: %v", err)
	}
	if err := tr.Step(context.Background(), 2, "close"); err != nil {
		t.Fatalf("step 2: %v", err)
	}

	if !tr.AllMatched() {
		t.Fatalf("expected every step matched, report=%v", tr.Report())
	}
	if got := f.Current(); got != "closed" {
		t.Fatalf("fsm state = %q, want closed", got)
	}
}

func TestEmptyInputRecordedAsInputMismatch(t *testing.T) {
	_, tr := newTracedFSM(nil)
	// Expected event is non-empty; an empty input must be visible as an
	// exact "" value, not skipped or treated as pass.
	tr.Register(1, StepExpectation{"closed", "open", "closed", nil})

	tr.StartRun()
	_ = tr.Step(context.Background(), 1, "")

	rec := tr.Records()[0]
	if rec.InputOK() {
		t.Fatal("empty input wrongly matched")
	}
	found := false
	for _, m := range rec.Mismatches() {
		if m.Side == SideInput && m.Field == "event" && m.Got == "" && m.Want == "open" {
			found = true
		}
	}
	if !found {
		t.Fatalf("empty input not recorded verbatim: %v", rec.Mismatches())
	}
	if tr.AllMatched() {
		t.Fatal("AllMatched must be false when the first step fails")
	}
}

func TestFirstStepFailureDoesNotPass(t *testing.T) {
	_, tr := newTracedFSM(nil)
	// Event "ghost" does not exist; current state stays closed.
	tr.Register(1, StepExpectation{
		FromState: "closed", Event: "ghost", ToState: "closed",
		WantErr: &ErrExpectation{
			Type:    "fsm.UnknownEventError",
			Message: "event ghost does not exist",
		},
	})

	tr.StartRun()
	err := tr.Step(context.Background(), 1, "ghost")
	if err == nil {
		t.Fatal("expected failure on the first step")
	}
	if !tr.AllMatched() {
		t.Fatalf("a correctly-expected first-step failure must reconcile: %v", tr.Report())
	}
}

func TestSwallowedErrorVisible(t *testing.T) {
	_, tr := newTracedFSM(nil)
	tr.Register(1, StepExpectation{"closed", "open", "open", nil})
	tr.Register(2, StepExpectation{"open", "open", "open", nil})

	tr.StartRun()
	if err := tr.Step(context.Background(), 1, "open"); err != nil {
		t.Fatal(err)
	}
	// From state "open", event "open" is invalid: the state stays open AND
	// an error is returned. Expecting success must surface the error.
	if err := tr.Step(context.Background(), 2, "open"); err == nil {
		t.Fatal("expected InvalidEventError")
	}

	rec := tr.Records()[1]
	sawError := false
	for _, m := range rec.Mismatches() {
		if m.Side == SideOutput && m.Field == "error-presence" {
			sawError = true
		}
	}
	if !sawError {
		t.Fatalf("error was swallowed despite state also being produced: %v", rec.Mismatches())
	}
}

func TestFrozenStepUnaffectedByLaterStateSwap(t *testing.T) {
	f, tr := newTracedFSM(nil)
	tr.Register(1, StepExpectation{"closed", "open", "open", nil})
	tr.Register(2, StepExpectation{"open", "close", "closed", nil})

	tr.StartRun()
	if err := tr.Step(context.Background(), 1, "open"); err != nil {
		t.Fatal(err)
	}

	// Rip the state out from under the run before step 2.
	f.SetState("mystery")

	if err := tr.Step(context.Background(), 2, "close"); err == nil {
		t.Fatal("expected failure after state swap")
	}

	first := tr.Records()[0]
	if !first.InputOK() || !first.OutputOK() {
		t.Fatal("already-reconciled step 1 changed after later state swap")
	}
	second := tr.Records()[1]
	if second.InputOK() {
		t.Fatalf("step 2 input must show swapped state: %v", second.Mismatches())
	}
}

func TestDuplicateStepNumberKeepsBothOccurrences(t *testing.T) {
	_, tr := newTracedFSM(nil)
	// Step 1 registered twice with DIFFERENT inputs: both must be kept and
	// the second reconciliation must not overwrite the first.
	tr.Register(1, StepExpectation{"closed", "open", "open", nil})
	tr.Register(1, StepExpectation{"open", "close", "closed",
		&ErrExpectation{"fsm.InvalidEventError",
			"event close inappropriate in current state open"}})

	tr.StartRun()
	if err := tr.Step(context.Background(), 1, "open"); err != nil {
		t.Fatal(err)
	}
	// Second occurrence actually succeeds (open -> closed); its expectation
	// wrongly demands an error, so its output conclusion must differ.
	if err := tr.Step(context.Background(), 1, "close"); err != nil {
		t.Fatalf("second occurrence should transition cleanly, got %v", err)
	}

	recs := tr.Records()
	if len(recs) != 2 {
		t.Fatalf("want 2 records, got %d", len(recs))
	}
	if recs[0].Occurrence != 0 || recs[1].Occurrence != 1 {
		t.Fatalf("occurrences not preserved: %+v %+v", recs[0], recs[1])
	}
	if !recs[0].InputOK() || !recs[0].OutputOK() {
		t.Fatal("first occurrence verdict overwritten")
	}
	if !recs[1].InputOK() {
		t.Fatalf("second occurrence input should match: %v", recs[1].Mismatches())
	}
	if recs[1].OutputOK() {
		t.Fatal("second occurrence must keep its own output conclusion")
	}
	if recs[0].gotEvent != "open" || recs[1].gotEvent != "close" {
		t.Fatal("both differing inputs must be retained")
	}
}

func TestErrorTypeAndExactMessageWrappedDistinct(t *testing.T) {
	inner := errors.New("boom detail")
	_, tr := newTracedFSM(Callbacks{
		"leave_open": func(_ context.Context, e *Event) {
			e.Cancel(inner)
		},
	})

	tr.Register(1, StepExpectation{"closed", "open", "open", nil})
	tr.Register(2, StepExpectation{"open", "explode", "open",
		&ErrExpectation{"fsm.CanceledError",
			"transition canceled with error: boom detail"}})

	tr.StartRun()
	if err := tr.Step(context.Background(), 1, "open"); err != nil {
		t.Fatal(err)
	}
	err := tr.Step(context.Background(), 2, "explode")
	var canceled CanceledError
	if !errors.As(err, &canceled) {
		t.Fatalf("want CanceledError wrapping inner, got %T %v", err, err)
	}
	if !tr.AllMatched() {
		t.Fatalf("wrapped error exact match failed: %v", tr.Report())
	}

	// The inner error alone is not the same error: type differs and the
	// exact text differs (no wrapper prefix), so substring matching would
	// wrongly pass.
	tr.Register(3, StepExpectation{"open", "explode", "open",
		&ErrExpectation{"*errors.errorString", "boom detail"}})
	if err := tr.Step(context.Background(), 3, "explode"); err == nil {
		t.Fatal("expected another failure")
	}
	rec := tr.Records()[2]
	if rec.OutputOK() {
		t.Fatal("wrapped error must not compare equal to the inner error")
	}
	fields := map[string]bool{}
	for _, m := range rec.Mismatches() {
		if m.Side == SideOutput {
			fields[m.Field] = true
		}
	}
	if !fields["error-type"] || !fields["error-message"] {
		t.Fatalf("want both error-type and error-message mismatches: %v", rec.Mismatches())
	}
}

func TestLaterStepBeforeEarlierRejected(t *testing.T) {
	_, tr := newTracedFSM(nil)
	tr.Register(1, StepExpectation{"closed", "open", "open", nil})
	tr.Register(2, StepExpectation{"open", "close", "closed", nil})

	tr.StartRun()
	if err := tr.Step(context.Background(), 2, "close"); err == nil {
		t.Fatal("later step must be rejected before step 1 arrives")
	}
	rec1 := tr.Records()[0]
	if rec1.InputDone() {
		t.Fatal("later step must not be filed as the first step")
	}

	if err := tr.Step(context.Background(), 9, "open"); err == nil {
		t.Fatal("unregistered step number must be rejected")
	}
}

func TestInputWithoutOutputStaysUnfinished(t *testing.T) {
	f, tr := newTracedFSM(nil)
	tr.Register(1, StepExpectation{"closed", "open", "open", nil})
	tr.Register(2, StepExpectation{"open", "close", "closed", nil})
	tr.Register(3, StepExpectation{"closed", "open", "open", nil})

	tr.StartRun()
	if err := tr.Step(context.Background(), 1, "open"); err != nil {
		t.Fatal(err)
	}
	// Run ends after one step: steps 2 and 3 never receive any output.

	if unfinished := tr.Unfinished(); len(unfinished) != 2 {
		t.Fatalf("want 2 unfinished steps, got %v", unfinished)
	}
	if tr.AllMatched() {
		t.Fatal("other steps succeeding must not mark an unfinished step passed")
	}

	// Replay only step 1: unfinished steps must not auto-complete, and the
	// frozen step-1 record must stay identical.
	f.SetState("closed")
	tr.StartRun()
	if err := tr.Step(context.Background(), 1, "open"); err != nil {
		t.Fatal(err)
	}
	if len(tr.Unfinished()) != 2 {
		t.Fatalf("replay auto-completed unfinished steps: %v", tr.Unfinished())
	}
	if tr.AllMatched() {
		t.Fatal("AllMatched after replay despite unfinished steps")
	}
}

func TestReplayProducesIdenticalRecords(t *testing.T) {
	f, tr := newTracedFSM(nil)
	tr.Register(1, StepExpectation{"closed", "open", "open", nil})
	tr.Register(2, StepExpectation{"open", "close", "closed", nil})

	snapshot := func() []StepMismatch {
		f.SetState("closed")
		tr.StartRun()
		if err := tr.Step(context.Background(), 1, "open"); err != nil {
			t.Fatal(err)
		}
		if err := tr.Step(context.Background(), 2, "close"); err != nil {
			t.Fatal(err)
		}
		return tr.Report()
	}

	first := snapshot()
	second := snapshot()
	if len(first) != len(second) {
		t.Fatalf("replay differs: %v vs %v", first, second)
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("replay mismatch at %d:\n%+v\n%+v", i, first[i], second[i])
		}
	}

	for _, rec := range tr.Records() {
		if !rec.InputDone() || !rec.OutputDone() || !rec.InputOK() || !rec.OutputOK() {
			t.Fatalf("record not stably reconciled: %+v", rec)
		}
	}
}

func TestReportMentionsStepNumberSideAndOrder(t *testing.T) {
	_, tr := newTracedFSM(nil)
	tr.Register(1, StepExpectation{"closed", "open", "open", nil})
	// Step 2 actually succeeds; the expectation wrongly demands an error.
	tr.Register(2, StepExpectation{"open", "close", "closed",
		&ErrExpectation{"fsm.UnknownEventError", "event ghost does not exist"}})
	tr.Register(3, StepExpectation{"closed", "open", "open", nil})

	tr.StartRun()
	if err := tr.Step(context.Background(), 1, "open"); err != nil {
		t.Fatal(err)
	}
	_ = tr.Step(context.Background(), 2, "close")

	report := tr.Report()
	if len(report) == 0 {
		t.Fatal("expected mismatches in report")
	}
	for _, m := range report {
		if m.StepNo != 2 {
			t.Fatalf("report must carry step number: %+v", m)
		}
		if m.Side != SideOutput {
			t.Fatalf("report must carry side (input/output): %+v", m)
		}
	}

	// Records follow registration order, not map iteration order.
	order := make([]int, 0, len(tr.Records()))
	for _, rec := range tr.Records() {
		order = append(order, rec.StepNo)
	}
	wantOrder := []int{1, 2, 3}
	for i := range wantOrder {
		if order[i] != wantOrder[i] {
			t.Fatalf("records not in registration order: %v", order)
		}
	}
}

func TestAdjacentOutputInputAreTwoRecordsNotMerged(t *testing.T) {
	_, tr := newTracedFSM(nil)
	tr.Register(1, StepExpectation{"closed", "open", "open", nil})
	tr.Register(2, StepExpectation{"open", "close", "closed", nil})

	tr.StartRun()
	if err := tr.Step(context.Background(), 1, "open"); err != nil {
		t.Fatal(err)
	}
	if err := tr.Step(context.Background(), 2, "close"); err != nil {
		t.Fatal(err)
	}

	// Step 1's output state ("open") equals step 2's input state but they
	// must be two separately frozen records with independent input/output
	// verdicts.
	recs := tr.Records()
	if len(recs) != 2 {
		t.Fatalf("want 2 records, got %d", len(recs))
	}
	if recs[0].gotToState != "open" || recs[1].gotFromState != "open" {
		t.Fatal("boundary states not retained on both sides")
	}
	if recs[0] == recs[1] {
		t.Fatal("adjacent steps merged into one record")
	}
}
