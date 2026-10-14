/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"maps"
	"sync"
	"time"

	"github.com/brv52/pg-operator/internal/pg"
)

// fakeInstance is the SQL-visible state of one instance in fakePG.
type fakeInstance struct {
	reachable  bool
	inRecovery bool
	timeline   int64
	receive    pg.LSN
	replay     pg.LSN
	streaming  bool
	lastMsgAge time.Duration
	replicas   []pg.ReplicaConn
	settings   map[string]string
	slots      []string
}

// fakePG is an in-memory pg.Client keyed by instance name. Tests set the
// state each instance should report and inspect the calls the controller
// made.
type fakePG struct {
	mu        sync.Mutex
	instances map[string]*fakeInstance
	promoted  []string
	fenced    []string
	detached  []string
}

var _ pg.Client = (*fakePG)(nil)

func newFakePG() *fakePG { return &fakePG{instances: map[string]*fakeInstance{}} }

var errUnreachable = errors.New("connection refused")

func (f *fakePG) set(name string, fn func(*fakeInstance)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i, ok := f.instances[name]
	if !ok {
		i = &fakeInstance{settings: map[string]string{}, timeline: 1, lastMsgAge: -1}
		f.instances[name] = i
	}
	fn(i)
}

func (f *fakePG) primary(name string, lsn pg.LSN, replicas ...string) {
	f.set(name, func(i *fakeInstance) {
		i.reachable, i.inRecovery, i.receive, i.replay = true, false, lsn, lsn
		i.replicas = nil
		for _, r := range replicas {
			i.replicas = append(i.replicas, pg.ReplicaConn{ApplicationName: r, State: "streaming", SyncState: "quorum", ReplyAge: 0})
		}
	})
}

func (f *fakePG) standby(name string, lsn pg.LSN, streaming bool) {
	f.set(name, func(i *fakeInstance) {
		i.reachable, i.inRecovery, i.receive, i.replay, i.streaming = true, true, lsn, lsn, streaming
		i.lastMsgAge = 0
		if !streaming {
			i.lastMsgAge = 30 * time.Second
		}
	})
}

func (f *fakePG) down(name string) {
	f.set(name, func(i *fakeInstance) { i.reachable = false })
}

func (f *fakePG) get(name string) *fakeInstance {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.instances[name]
}

func (f *fakePG) Observe(_ context.Context, t pg.Target) pg.Observation {
	f.mu.Lock()
	defer f.mu.Unlock()
	o := pg.Observation{Name: t.Name}
	i, ok := f.instances[t.Name]
	if !ok || !i.reachable || t.Host == "" {
		o.Err = errUnreachable
		return o
	}
	o.Reachable = true
	o.InRecovery = i.inRecovery
	o.Timeline = i.timeline
	o.ReceiveLSN, o.ReplayLSN = i.receive, i.replay
	o.Streaming = i.streaming
	o.LastMsgAge = i.lastMsgAge
	o.Replicas = append([]pg.ReplicaConn(nil), i.replicas...)
	o.Settings = maps.Clone(i.settings)
	return o
}

func (f *fakePG) Promote(_ context.Context, t pg.Target, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	i, ok := f.instances[t.Name]
	if !ok || !i.reachable {
		return errUnreachable
	}
	if i.inRecovery {
		f.promoted = append(f.promoted, t.Name)
		i.inRecovery = false
		i.timeline++
		i.streaming = false
	}
	return nil
}

func (f *fakePG) Fence(_ context.Context, t pg.Target) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	i, ok := f.instances[t.Name]
	if !ok || !i.reachable {
		return errUnreachable
	}
	f.fenced = append(f.fenced, t.Name)
	i.settings["default_transaction_read_only"] = "on"
	return nil
}

func (f *fakePG) ApplySettings(_ context.Context, t pg.Target, s map[string]string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i, ok := f.instances[t.Name]
	if !ok || !i.reachable {
		return nil, errUnreachable
	}
	var changed []string
	for k, v := range s {
		if i.settings[k] != v {
			i.settings[k] = v
			changed = append(changed, k)
		}
	}
	if conninfo, ok := s["primary_conninfo"]; ok && conninfo == "" {
		i.streaming = false
		f.detached = append(f.detached, t.Name)
	}
	return changed, nil
}

func (f *fakePG) EnsureSlots(_ context.Context, t pg.Target, want []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	i, ok := f.instances[t.Name]
	if !ok || !i.reachable {
		return errUnreachable
	}
	i.slots = append([]string(nil), want...)
	return nil
}

func (f *fakePG) Checkpoint(_ context.Context, t pg.Target) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if i, ok := f.instances[t.Name]; !ok || !i.reachable {
		return errUnreachable
	}
	return nil
}

func (f *fakePG) promotedNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.promoted...)
}

func (f *fakePG) fencedNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.fenced...)
}
