package baseapp

import (
	"sync"
	"time"

	"github.com/cosmos/cosmos-sdk/telemetry"
)

type abciTimingStat struct {
	count           uint64
	totalNanosecond uint64
	maxNanosecond   uint64
}

type abciTimingSnapshot struct {
	count uint64
	total time.Duration
	avg   time.Duration
	max   time.Duration
}

func (s *abciTimingStat) observe(duration time.Duration) {
	nanoseconds := uint64(duration)
	s.count++
	s.totalNanosecond += nanoseconds
	if nanoseconds > s.maxNanosecond {
		s.maxNanosecond = nanoseconds
	}
}

func (s *abciTimingStat) snapshotAndReset() abciTimingSnapshot {
	count := s.count
	total := time.Duration(s.totalNanosecond)
	max := time.Duration(s.maxNanosecond)
	s.count = 0
	s.totalNanosecond = 0
	s.maxNanosecond = 0
	avg := time.Duration(0)
	if count > 0 {
		avg = total / time.Duration(count)
	}
	return abciTimingSnapshot{count: count, total: total, avg: avg, max: max}
}

type abciTimingRecorder struct {
	mu              sync.Mutex
	checkTx         abciTimingStat
	prepareProposal abciTimingStat
	finalizeBlock   abciTimingStat
	commit          abciTimingStat
}

func (app *BaseApp) measureABCITiming(metricKey string, start time.Time) {
	duration := time.Since(start)
	telemetry.ModuleMeasureSince("baseapp", start, metricKey)

	app.abciTimings.mu.Lock()
	defer app.abciTimings.mu.Unlock()

	switch metricKey {
	case telemetry.MetricKeyCheckTx:
		app.abciTimings.checkTx.observe(duration)
	case telemetry.MetricKeyPrepareProposal:
		app.abciTimings.prepareProposal.observe(duration)
	case telemetry.MetricKeyFinalizeBlock:
		app.abciTimings.finalizeBlock.observe(duration)
	case telemetry.MetricKeyCommit:
		app.abciTimings.commit.observe(duration)
	}
}

// logABCITimings emits a single per-block summary. CheckTx includes RecheckTx
// calls and is measured over the window since the previous commit.
func (app *BaseApp) logABCITimings(height int64) {
	app.abciTimings.mu.Lock()
	defer app.abciTimings.mu.Unlock()

	checkTx := app.abciTimings.checkTx.snapshotAndReset()
	prepareProposal := app.abciTimings.prepareProposal.snapshotAndReset()
	finalizeBlock := app.abciTimings.finalizeBlock.snapshotAndReset()
	commit := app.abciTimings.commit.snapshotAndReset()

	app.logger.Info("baseapp ABCI timing window",
		"height", height,
		"check_tx_count", checkTx.count,
		"check_tx_total", checkTx.total,
		"check_tx_avg", checkTx.avg,
		"check_tx_max", checkTx.max,
		"prepare_proposal_count", prepareProposal.count,
		"prepare_proposal_total", prepareProposal.total,
		"prepare_proposal_avg", prepareProposal.avg,
		"prepare_proposal_max", prepareProposal.max,
		"finalize_block_count", finalizeBlock.count,
		"finalize_block_total", finalizeBlock.total,
		"finalize_block_avg", finalizeBlock.avg,
		"finalize_block_max", finalizeBlock.max,
		"commit_count", commit.count,
		"commit_total", commit.total,
		"commit_avg", commit.avg,
		"commit_max", commit.max,
	)
}
