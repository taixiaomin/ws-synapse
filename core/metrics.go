package core

// nilMetrics is used internally when no MetricsCollector is configured.
// All calls are no-ops.
type nilMetrics struct{}

func (nilMetrics) IncConnections() {}
func (nilMetrics) DecConnections() {}
func (nilMetrics) IncMessagesIn()  {}
func (nilMetrics) IncMessagesOut() {}
func (nilMetrics) IncDrops()       {}
func (nilMetrics) IncErrors()      {}
func (nilMetrics) IncReconnects()  {}

// metricsOrNop returns the provided collector, or a no-op if nil.
func metricsOrNop(m MetricsCollector) MetricsCollector {
	if m != nil {
		return m
	}
	return nilMetrics{}
}

