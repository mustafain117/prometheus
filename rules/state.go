package rules

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

type State struct {
	alerts []*Alert
	key    uint64
}

type AlertStore struct {
	msgc        chan *State
	logger      log.Logger
	storectx    context.Context
	stateByRule map[uint64][]*Alert
	//protects the `stateByRule` map
	stateMtx sync.RWMutex
}

func NewAlertStore(ctx context.Context, l log.Logger, msgc chan *State) AlertStore {
	s := AlertStore{
		storectx:    ctx,
		logger:      l,
		msgc:        msgc,
		stateByRule: make(map[uint64][]*Alert), // TODO: read file state on init
	}
	s.initState()
	return s
}

func (s *AlertStore) Start() {
	go func() {
		err := s.run()
		if err != nil {
			level.Error(s.logger).Log("msg", "error running alert store", "err", err)
		}
	}()
}

func (s *AlertStore) run() error {
	for {
		select {
		case msg := <-s.msgc:
			go func(msg *State) error {
				level.Info(s.logger).Log("msg", "saving state", "key", msg.key, "alerts", len(msg.alerts))
				s.saveState2(msg.key, msg.alerts)
				return nil
			}(msg)
		case <-s.storectx.Done():
			level.Info(s.logger).Log("msg", "storectx.Done()")
			return nil
		}
	}
}

func (s *AlertStore) saveState(key uint64, alerts []*Alert) {
	time.Sleep(60 * time.Second)
	// Update in memory
	s.stateByRule[key] = alerts
	// Store to disk
	data, err := json.Marshal(s.stateByRule)
	if err != nil {
		level.Error(s.logger).Log("Error marshalling JSON", err)
		return
	}
	// Write JSON data to a file
	err = os.WriteFile("data/alerts.json", data, 0o644)
	if err != nil {
		level.Error(s.logger).Log("Error writing to file", err)
		return
	}
}

func (s *AlertStore) saveState2(key uint64, alerts []*Alert) {
	s.stateMtx.Lock()
	defer s.stateMtx.Unlock()
	// Update in memory
	if alerts != nil {
		s.stateByRule[key] = alerts
	}

	file, err := os.Create("data/alerts.json")
	if err != nil {
		level.Error(s.logger).Log("Error writing to file", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	err = encoder.Encode(s.stateByRule)
	if err != nil {
		level.Error(s.logger).Log("Error writing to file", err)
	}
}

func (s *AlertStore) initState() {
	file, err := os.Open("data/alerts.json")
	if err != nil {
		level.Error(s.logger).Log("Error writing to file", err)
	}
	defer file.Close()

	var stateByRule map[uint64][]*Alert
	err = json.NewDecoder(file).Decode(&stateByRule)
	if err != nil {
		level.Error(s.logger).Log("Error writing to file", err)
	}
	if stateByRule == nil {
		level.Info(s.logger).Log("msg", "stateByRule==nil")
		stateByRule = make(map[uint64][]*Alert)
	}
	s.stateByRule = stateByRule
	level.Info(s.logger).Log("msg", "read state success", "len(stateByRule)", len(stateByRule))
}

func (s *AlertStore) GetAlerts(rulekey uint64) map[uint64]*Alert {
	s.stateMtx.RLock()
	defer s.stateMtx.RUnlock()
	restoredAlerts, ok := s.stateByRule[rulekey]
	if !ok {
		return nil
	}
	alerts := make(map[uint64]*Alert)
	for _, alert := range restoredAlerts {
		level.Info(s.logger).Log("alert", alert)
		if alert == nil {
			continue
		}
		h := alert.Labels.Hash()
		alerts[h] = alert
	}
	level.Info(s.logger).Log("msg", "returning", "len", len(alerts))

	return alerts
}
