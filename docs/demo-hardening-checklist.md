# Repliqia Demo Hardening Checklist

Goal: stabilize the demo flow end-to-end (orchestrator -> nodes -> sync -> UI).

## Critical

- [x] Fix PUT payload contract mismatch between UI and backend.
  - Symptom: writes store `{}` or wrong value shape.
  - Files: `interface/gui/src/api.js`, `repliqia/api/server.py`

- [x] Fix orchestrator sync no-op behavior when peer URLs are not configured on nodes.
  - Symptom: sync reports success/noop, replicas do not converge.
  - Files: `repliqia/orchestrator.py`, `repliqia/api/server.py`

- [x] Remove stale `--storage` usage from restart flow.
  - Symptom: restart can return 200 then node exits with argument error.
  - Files: `repliqia/orchestrator.py`

- [x] Ensure demo reset clears active node DB files under `repliqia/data/*`.
  - Symptom: data leaks across demo resets.
  - Files: `repliqia/orchestrator.py`

- [x] Fix conflict payload shape mismatch in UI conflict panel.
  - Symptom: conflict panel may render incorrectly or crash when conflicts exist.
  - Files: `interface/gui/src/hooks/useConflictsPoll.js`, `interface/gui/src/components/ConflictPanel.jsx`

- [x] Align event schema between backend broadcasts and UI listeners.
  - Symptom: event log/refresh hooks miss updates (`operation` vs `operation_completed`).
  - Files: `repliqia/orchestrator.py`, `interface/gui/src/components/EventLog.jsx`, `interface/gui/src/components/ConflictPanel.jsx`, `interface/gui/src/components/VectorClockViz.jsx`

## High

- [x] Make API client tolerant of 204/empty-body responses.
  - Symptom: DELETE may surface as client parsing error.
  - Files: `interface/gui/src/api.js`

- [x] Remove unreachable duplicate proxy code blocks from orchestrator.
  - Symptom: dead code hides intended behavior and increases maintenance risk.
  - Files: `repliqia/orchestrator.py`

- [x] Relax SYNC form validation to not require key input.
  - Symptom: operator cannot run pure node-to-node sync from panel.
  - Files: `interface/gui/src/components/OperationPanel.jsx`

- [x] Improve node refresh cadence after lifecycle actions.
  - Symptom: UI appears stale after start/stop/restart.
  - Files: `interface/gui/src/hooks/useNodesLight.js`, `interface/gui/src/App.jsx`

## Medium

- [x] Consider removing `stdout/stderr=PIPE` or consuming logs continuously for child processes.
  - Symptom: long sessions can risk subprocess output backpressure.
  - Files: `repliqia/orchestrator.py`
