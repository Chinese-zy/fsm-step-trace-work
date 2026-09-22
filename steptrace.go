// Step-trace support: per-step input/output reconciliation for FSM runs.
//
// This file is intentionally additive; none of the pre-existing FSM
// implementation is modified.

package fsm

import (
	"context"
	"fmt"
)

// StepSide identifies which side of a single step reconciliation failed.
type StepSide int

const (
	// SideNone means no mismatch was recorded for the side.
	SideNone StepSide = iota
	// SideInput means the step's input (state + event) did not match.
	SideInput
	// SideOutput means the step's output (state + error) did not match.
	SideOutput
)

func (s StepSide) String() string {
	switch s {
	case SideInput:
		return "input"
	case SideOutput:
		return "output"
	default:
		return "none"
	}
}

// ErrExpectation describes an expected error by concrete type and exact
// message text. A wrapped error is not equal to its inner error: Type must be
// the exact concrete type of the returned error and Message its exact Error()
// text; an outer wrapper therefore fails both fields.
type ErrExpectation struct {
	// Type is the concrete error type name, matched exactly (e.g.
	// "fsm.CanceledError").
	Type string
	// Message is the exact error.Error() text, never a substring.
	Message string
}

// ErrorMismatch records an error expectation that did not match. Substring
// matching is never used; both type and exact text must agree.
type ErrorMismatch struct {
	GotType string
	GotMsg  string
	Want    ErrExpectation
}

// StepMismatch describes how one side of one step differed from what was
// expected.
type StepMismatch struct {
	// StepNo is the step number under which the step was registered.
	StepNo int
	// Occurrence distinguishes repeat registrations of the same StepNo.
	Occurrence int
	// Side says whether the input or output did not match.
	Side StepSide
	// Field identifies the exact mismatching field: "state", "event",
	// "error-type", "error-message" or "error-presence".
	Field string
	// Got is what the run actually produced ("" for an empty/absent value).
	Got string
	// Want is what was expected.
	Want string
	// ErrorMismatch is populated for "error-type" and "error-message".
	ErrorMismatch *ErrorMismatch
}

func (m StepMismatch) String() string {
	return fmt.Sprintf("step %d (occurrence %d) %s mismatch on %s: got %q, want %q",
		m.StepNo, m.Occurrence, m.Side, m.Field, m.Got, m.Want)
}

// StepExpectation is one expected step: the input the FSM must receive and
// the output it must produce. WantErr nil means the step must succeed.
type StepExpectation struct {
	// FromState is the state the FSM must be in before the event.
	FromState string
	// Event is the event name fed in.
	Event string
	// ToState is the state the FSM must be in after the event.
	ToState string
	// WantErr, when non-nil, is the exact error (type + message) expected.
	WantErr *ErrExpectation
}

// StepRecord is the frozen, append-only evidence of one registered step
// occurrence. Once written, a record is never mutated by later runs: input
// and output snapshots are frozen, and reconciliation verdicts live on the
// record rather than being re-read from live FSM state, so swapping the FSM
// state halfway through cannot change an already-reconciled step.
type StepRecord struct {
	// StepNo is the step number under which it was registered.
	StepNo int
	// Occurrence distinguishes repeat registrations of the same StepNo.
	Occurrence int
	// Order is the global registration position (0-based).
	Order int
	// Step is the registered expected input/output.
	Step StepExpectation

	inputDone  bool
	outputDone bool

	gotFromState string
	gotEvent     string
	gotToState   string
	gotErr       error

	inputOK  bool
	outputOK bool

	mismatches []StepMismatch
}

// InputDone reports whether the input side has ever been observed.
func (r *StepRecord) InputDone() bool { return r.inputDone }

// OutputDone reports whether the output side has ever been observed. A step
// with input but no output is unfinished, even if every other step passes.
func (r *StepRecord) OutputDone() bool { return r.outputDone }

// InputOK reports the frozen input-side verdict.
func (r *StepRecord) InputOK() bool { return r.inputOK }

// OutputOK reports the frozen output-side verdict.
func (r *StepRecord) OutputOK() bool { return r.outputOK }

// Mismatches returns a copy of the frozen per-field mismatches.
func (r *StepRecord) Mismatches() []StepMismatch {
	out := make([]StepMismatch, len(r.mismatches))
	copy(out, r.mismatches)
	return out
}

// StepTracer records and reconciles FSM runs one step at a time. Records are
// kept in registration order, not map iteration order. Registering a step
// number more than once appends a new occurrence and never overwrites an
// earlier conclusion; occurrences with different inputs are both kept.
type StepTracer struct {
	machine *FSM

	// records is an append-only, registration-ordered ledger.
	records []*StepRecord

	// byNo indexes records by step number, preserving occurrence order.
	byNo map[int][]*StepRecord

	// seen counts observed occurrences per step number in the current run.
	seen map[int]int
}

// NewStepTracer wraps an existing FSM. The FSM itself is not modified.
func NewStepTracer(f *FSM) *StepTracer {
	return &StepTracer{
		machine: f,
		byNo:    make(map[int][]*StepRecord),
		seen:    make(map[int]int),
	}
}

// Register appends a step expectation under the given step number and returns
// the global registration position. A step number may be registered more than
// once; each registration is a distinct occurrence with its own input and
// output evidence and a later occurrence never overwrites an earlier one.
func (t *StepTracer) Register(stepNo int, step StepExpectation) int {
	occurrence := len(t.byNo[stepNo])
	order := len(t.records)
	rec := &StepRecord{
		StepNo:     stepNo,
		Occurrence: occurrence,
		Order:      order,
		Step:       step,
	}
	t.records = append(t.records, rec)
	t.byNo[stepNo] = append(t.byNo[stepNo], rec)
	return order
}

// StartRun begins (or restarts) a replay of the same fixed input. Frozen
// evidence from earlier runs is preserved; an unfinished step is not
// auto-completed merely because the run starts again.
func (t *StepTracer) StartRun() {
	t.seen = make(map[int]int)
}

// Step feeds one event for the given step number into the FSM and reconciles
// it against the next unobserved occurrence registered for that number. The
// input (state before, event) and output (state after, error) are recorded as
// two separate sides: when the previous step's output state equals this
// step's input state, they stay two distinct ledger entries, never merged.
//
// An unregistered number, an occurrence beyond what was registered, or a
// later step seen while a smaller registered step is unobserved is rejected
// and never silently filed as the first step.
func (t *StepTracer) Step(ctx context.Context, stepNo int, event string) error {
	if err := t.checkOrder(stepNo); err != nil {
		return err
	}

	occ := t.seen[stepNo]
	group := t.byNo[stepNo]
	if occ >= len(group) {
		return fmt.Errorf("step %d observed %d times but only %d occurrences registered",
			stepNo, occ+1, len(group))
	}
	rec := group[occ]

	gotFrom := t.machine.Current()
	err := t.machine.Event(ctx, event)
	gotTo := t.machine.Current()

	// Input side is frozen independently of the output.
	if !rec.inputDone {
		rec.inputDone = true
		rec.gotFromState = gotFrom
		rec.gotEvent = event
		rec.inputOK = t.reconcileInput(rec, gotFrom, event)
	}

	// Output side is frozen separately. Value and error are both kept; a
	// step that yields a state and an error never has the error swallowed.
	if !rec.outputDone {
		rec.outputDone = true
		rec.gotToState = gotTo
		rec.gotErr = err
		rec.outputOK = t.reconcileOutput(rec, gotTo, err)
	}

	t.seen[stepNo]++
	return err
}

// checkOrder enforces that later step numbers are not observed before
// smaller, already-registered ones, and that observed numbers were actually
// registered.
func (t *StepTracer) checkOrder(stepNo int) error {
	if len(t.byNo[stepNo]) == 0 {
		return fmt.Errorf("step %d observed but never registered", stepNo)
	}
	for no := range t.byNo {
		if no < stepNo && t.seen[no] == 0 {
			return fmt.Errorf("step %d observed before earlier step %d",
				stepNo, no)
		}
	}
	return nil
}

func (t *StepTracer) reconcileInput(rec *StepRecord, gotFrom, gotEvent string) bool {
	ok := true
	if gotFrom != rec.Step.FromState {
		ok = false
		rec.mismatches = append(rec.mismatches, StepMismatch{
			StepNo: rec.StepNo, Occurrence: rec.Occurrence,
			Side: SideInput, Field: "state",
			Got: gotFrom, Want: rec.Step.FromState,
		})
	}
	if gotEvent != rec.Step.Event {
		ok = false
		rec.mismatches = append(rec.mismatches, StepMismatch{
			StepNo: rec.StepNo, Occurrence: rec.Occurrence,
			Side: SideInput, Field: "event",
			Got: gotEvent, Want: rec.Step.Event,
		})
	}
	return ok
}

func (t *StepTracer) reconcileOutput(rec *StepRecord, gotTo string, gotErr error) bool {
	ok := true
	if gotTo != rec.Step.ToState {
		ok = false
		rec.mismatches = append(rec.mismatches, StepMismatch{
			StepNo: rec.StepNo, Occurrence: rec.Occurrence,
			Side: SideOutput, Field: "state",
			Got: gotTo, Want: rec.Step.ToState,
		})
	}

	wantErr := rec.Step.WantErr
	switch {
	case wantErr == nil && gotErr != nil:
		ok = false
		rec.mismatches = append(rec.mismatches, StepMismatch{
			StepNo: rec.StepNo, Occurrence: rec.Occurrence,
			Side: SideOutput, Field: "error-presence",
			Got: gotErr.Error(), Want: "",
		})
	case wantErr != nil && gotErr == nil:
		ok = false
		rec.mismatches = append(rec.mismatches, StepMismatch{
			StepNo: rec.StepNo, Occurrence: rec.Occurrence,
			Side: SideOutput, Field: "error-presence",
			Got: "", Want: wantErr.Message,
		})
	case wantErr != nil && gotErr != nil:
		gotType := concreteErrName(gotErr)
		gotMsg := gotErr.Error()
		if gotType != wantErr.Type {
			ok = false
			rec.mismatches = append(rec.mismatches, StepMismatch{
				StepNo: rec.StepNo, Occurrence: rec.Occurrence,
				Side: SideOutput, Field: "error-type",
				Got: gotType, Want: wantErr.Type,
				ErrorMismatch: &ErrorMismatch{
					GotType: gotType, GotMsg: gotMsg, Want: *wantErr,
				},
			})
		}
		if gotMsg != wantErr.Message {
			ok = false
			rec.mismatches = append(rec.mismatches, StepMismatch{
				StepNo: rec.StepNo, Occurrence: rec.Occurrence,
				Side: SideOutput, Field: "error-message",
				Got: gotMsg, Want: wantErr.Message,
				ErrorMismatch: &ErrorMismatch{
					GotType: gotType, GotMsg: gotMsg, Want: *wantErr,
				},
			})
		}
	}
	return ok
}

// Records returns the ledger in registration order.
func (t *StepTracer) Records() []*StepRecord {
	out := make([]*StepRecord, len(t.records))
	copy(out, t.records)
	return out
}

// Report returns every recorded mismatch in registration order, each tagged
// with its step number and input/output side. Empty input is represented as
// the empty string, not skipped.
func (t *StepTracer) Report() []StepMismatch {
	var out []StepMismatch
	for _, rec := range t.records {
		out = append(out, rec.mismatches...)
	}
	return out
}

// Unfinished returns the StepNo/Occurrence of steps whose output was never
// observed. A step with input but no output stays unfinished even if all
// other steps pass, and replaying the run does not auto-complete it.
func (t *StepTracer) Unfinished() []StepKey {
	var out []StepKey
	for _, rec := range t.records {
		if !rec.outputDone {
			out = append(out, StepKey{rec.StepNo, rec.Occurrence})
		}
	}
	return out
}

// StepKey identifies one occurrence of a step number.
type StepKey struct {
	StepNo     int
	Occurrence int
}

// AllMatched is true only when every registered step occurrence has both
// sides observed and every one matched. It never passes on the strength of
// the last step alone.
func (t *StepTracer) AllMatched() bool {
	for _, rec := range t.records {
		if !rec.inputDone || !rec.outputDone ||
			!rec.inputOK || !rec.outputOK {
			return false
		}
	}
	return true
}

// concreteErrName returns the exact concrete error type name with package,
// e.g. "fsm.CanceledError". A wrapped error keeps its own outer type, so an
// extra wrapper never compares equal to the inner error.
func concreteErrName(err error) string {
	return fmt.Sprintf("%T", err)
}
