package clickhouseprometheusv2

import (
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

var updateGolden = flag.Bool("update", false, "rewrite the classification golden file")

const goldenFile = "testdata/classification_golden.json"

// corpusFile is the conformance corpus the integration suite replays; the
// golden freezes how the classifier routes every one of its expressions.
const corpusFile = "../../../tests/integration/testdata/promqltestcorpus/corpus.json"

type goldenEntry struct {
	Expr    string `json:"expr"`
	StartMs int64  `json:"start_ms"`
	EndMs   int64  `json:"end_ms"`
	StepMs  int64  `json:"step_ms"`
	// Plan is the routing decision: "full" (whole query in ClickHouse),
	// "hybrid" (units substituted, engine on top), "fallback" (engine over
	// the native querier).
	Plan string `json:"plan"`
	// Units is the substituted-unit count for hybrid plans.
	Units int `json:"units,omitempty"`
	// Reason is the coarse fallback bucket (fallbackShape).
	Reason string `json:"reason,omitempty"`
}

// TestClassificationGolden freezes the classifier's routing decision for
// every (expression, grid) of the conformance corpus. Routing is a
// correctness surface of its own: a change that silently sends rate() to the
// engine path costs the pushdown, and one that silently starts transpiling a
// shape never proven equivalent risks wrong numbers — both must show up in
// review as a diff of this file, with the corpus suite's clickhousev2 leg
// judging whether the new routing still returns the reference answers.
//
// Regenerate after intentional classifier changes:
//
//	go test ./pkg/prometheus/clickhouseprometheusv2 -run TestClassificationGolden -update
func TestClassificationGolden(t *testing.T) {
	raw, err := os.ReadFile(corpusFile)
	require.NoError(t, err)

	var corpus struct {
		Cases []struct {
			Expr    string `json:"expr"`
			StartMs int64  `json:"start_ms"`
			EndMs   int64  `json:"end_ms"`
			StepMs  int64  `json:"step_ms"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &corpus))
	require.NotEmpty(t, corpus.Cases)

	promParser := parser.NewParser(parser.Options{})
	seen := map[goldenEntry]bool{}
	var entries []goldenEntry
	for _, c := range corpus.Cases {
		key := goldenEntry{Expr: c.Expr, StartMs: c.StartMs, EndMs: c.EndMs, StepMs: c.StepMs}
		if seen[key] {
			continue
		}
		seen[key] = true

		expr, err := promParser.ParseExpr(c.Expr)
		require.NoError(t, err, "corpus expression must parse: %q", c.Expr)

		entry := key
		plan, ok := classify(expr, gridContext{startMs: c.StartMs, endMs: c.EndMs, stepMs: c.StepMs})
		switch {
		case ok && plan.full:
			entry.Plan = "full"
		case ok:
			entry.Plan = "hybrid"
			entry.Units = len(plan.units)
		default:
			entry.Plan = "fallback"
			entry.Reason = fallbackShape(expr)
		}
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]
		if a.Expr != b.Expr {
			return a.Expr < b.Expr
		}
		if a.StartMs != b.StartMs {
			return a.StartMs < b.StartMs
		}
		if a.EndMs != b.EndMs {
			return a.EndMs < b.EndMs
		}
		return a.StepMs < b.StepMs
	})

	got, err := json.MarshalIndent(entries, "", "  ")
	require.NoError(t, err)
	got = append(got, '\n')

	if *updateGolden {
		require.NoError(t, os.MkdirAll(filepath.Dir(goldenFile), 0o755))
		require.NoError(t, os.WriteFile(goldenFile, got, 0o644))
		return
	}

	want, err := os.ReadFile(goldenFile)
	require.NoError(t, err, "golden missing — generate it with -update")
	require.Equal(t, string(want), string(got),
		"classification routing changed; if intentional, regenerate with -update and justify the diff in review")
}
