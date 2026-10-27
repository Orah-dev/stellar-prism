package cli

import (
	"fmt"
	"strings"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// ruleEngine wraps compiled stream.ExpressionFilter predicates behind a small
// name-based dispatch the CLI can wire from flags or config files without
// re-parsing the expression language itself.
//
// The engine is deliberately stateless: compilation is done once by the caller,
// and Match is a pure function of the record. That keeps the evaluator
// offline-first and trivially testable without a network or a process.
type ruleEngine struct {
	filters []stream.ExpressionFilter
}

// NewRuleEngine builds an engine from already-compiled expressions. A nil or
// empty input yields an engine that passes every event through.
func NewRuleEngine(filters ...stream.ExpressionFilter) *ruleEngine {

	return &ruleEngine{filters: append([]stream.ExpressionFilter(nil), filters...)}
}

// Match returns true when the record satisfies every compiled rule. A record
// that any rule rejects is dropped; a rule that errors on a particular record
// rejects it and reports the underlying reason through the returned error.
func (e *ruleEngine) Match(record stream.Record) (bool, error) {
	if len(e.filters) == 0 {
		return true, nil
	}
	for _, f := range e.filters {
		if f == nil {
			continue
		}
		ok, err := f.Match(record)
		if err != nil {
			return false, fmt.Errorf("rule %q: %w", f.Expression(), err)
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// Expressions returns the source expressions this engine was built from, in
// the order they were given to NewRuleEngine.
func (e *ruleEngine) Expressions() []string {
	out := make([]string, len(e.filters))
	for i, f := range e.filters {
		if f == nil {
			out[i] = ""
			continue
		}
		out[i] = f.Expression()
	}
	return out
}

// CompileRules is a thin wrapper around stream.CompileExpression for
// consumer code that prefers an explicit compile step. It parses the
// expression once and returns a rule engine holding the compiled form.
func CompileRules(exprs ...string) (*ruleEngine, error) {
	if len(exprs) == 0 {
		return NewRuleEngine(), nil
	}
	compiled := make([]stream.ExpressionFilter, 0, len(exprs))
	for _, expr := range exprs {
		expr = strings.TrimSpace(expr)
		if expr == "" {
			continue
		}
		f, err := stream.CompileExpression(expr)
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, f)
	}
	return NewRuleEngine(compiled...), nil
}
