package rules

import (
	"encoding/json"
	"log/slog"
	"os"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// FileStore implements the AlertStore interface.
type FileStore struct {
	logger       *slog.Logger
	alertsByRule map[uint64][]*Alert
	// protects the `alertsByRule` map.
	stateMtx         sync.RWMutex
	path             string
	storeInitErrors  prometheus.Counter
	alertStoreErrors *prometheus.CounterVec
}

type FileData struct {
	Alerts map[uint64][]*Alert `json:"alerts"`
}

func NewFileStore(l *slog.Logger, storagePath string, registerer prometheus.Registerer) *FileStore {
	s := &FileStore{
		logger:       l,
		alertsByRule: make(map[uint64][]*Alert),
		path:         storagePath,
	}
	s.storeInitErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "alert_store_init_errors_total",
			Help:      "The total number of errors starting alert store.",
		},
	)
	s.alertStoreErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "rule_group_alert_store_errors_total",
			Help:      "The total number of errors in alert store.",
		},
		[]string{"rule_group"},
	)
	s.initState(registerer)
	return s
}

// initState reads the state from file storage into the alertsByRule map.
func (s *FileStore) initState(registerer prometheus.Registerer) {
	if registerer != nil {
		registerer.MustRegister(s.alertStoreErrors, s.storeInitErrors)
	}
	file, err := os.OpenFile(s.path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		s.logger.Error("Failed reading alerts state from file", "err", err)
		s.storeInitErrors.Inc()
		return
	}
	defer file.Close()

	var data *FileData
	err = json.NewDecoder(file).Decode(&data)
	if err != nil {
		data = nil
		s.logger.Error("msg", "Failed reading alerts state from file", "err", err)
		s.storeInitErrors.Inc()
	}
	alertsByRule := make(map[uint64][]*Alert)
	if data != nil && data.Alerts != nil {
		alertsByRule = data.Alerts
	}
	s.alertsByRule = alertsByRule
}

// GetAlerts returns the stored alerts for an alerting rule
// Alert state is read from the in memory map which is populated during initialization.
func (s *FileStore) GetAlerts(key uint64) (map[uint64]*Alert, error) {
	s.stateMtx.RLock()
	defer s.stateMtx.RUnlock()

	restoredAlerts, ok := s.alertsByRule[key]
	if !ok {
		return nil, nil
	}
	alerts := make(map[uint64]*Alert)
	for _, alert := range restoredAlerts {
		if alert == nil {
			continue
		}
		h := alert.Labels.Hash()
		alerts[h] = alert
	}
	return alerts, nil
}

// SetAlerts updates the stateByRule map and writes state to file storage.
func (s *FileStore) SetAlerts(key uint64, groupKey string, alerts []*Alert) error {
	s.stateMtx.Lock()
	defer s.stateMtx.Unlock()

	// Update in memory
	if alerts != nil {
		s.alertsByRule[key] = alerts
	} else {
		return nil
	}
	// flush in memory state to file storage
	file, err := os.Create(s.path)
	if err != nil {
		s.alertStoreErrors.WithLabelValues(groupKey).Inc()
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	data := FileData{
		Alerts: s.alertsByRule,
	}
	err = encoder.Encode(data)
	if err != nil {
		s.alertStoreErrors.WithLabelValues(groupKey).Inc()
		return err
	}
	return nil
}
