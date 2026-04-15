import {
  startNode,
  stopNode,
  restartNode,
  proxyPut,
  proxyGet,
  proxySync,
  proxyConflicts,
  resetDemo,
} from '../api'

async function assertApiSuccess(promise, context) {
  const result = await promise
  if (result?.error) {
    throw new Error(`${context}: ${result.error}`)
  }
  return result?.data
}

/**
 * Helper to ensure a node is started, ignoring 409 if already exists
 */
async function ensureNode(node_id) {
  const { error } = await startNode(node_id, { n: 3, r: 2, w: 2 })
  // 409 = already exists, that's fine
  if (error && !error.includes('already exists')) {
    throw new Error(`Could not start node ${node_id}: ${error}`)
  }
}

async function prepareScenario(requiredNodes = []) {
  await assertApiSuccess(resetDemo(), 'Could not reset demo')

  for (const nodeId of requiredNodes) {
    await ensureNode(nodeId)
  }
}

async function runPut(node_id, key, value) {
  await assertApiSuccess(proxyPut(node_id, key, value), `PUT ${node_id}/${key} failed`)
}

async function runGet(node_id, key) {
  await assertApiSuccess(proxyGet(node_id, key), `GET ${node_id}/${key} failed`)
}

async function runSync(node_id, peer_id, key = null) {
  await assertApiSuccess(proxySync(node_id, peer_id, key), `SYNC ${node_id}->${peer_id} failed`)
}

async function runConflicts(node_id) {
  await assertApiSuccess(proxyConflicts(node_id), `CONFLICTS ${node_id} failed`)
}

async function runStopNode(node_id) {
  await assertApiSuccess(stopNode(node_id), `STOP ${node_id} failed`)
}

async function runRestartNode(node_id) {
  await assertApiSuccess(restartNode(node_id), `RESTART ${node_id} failed`)
}

/**
 * Demo Scenarios
 * 
 * Each scenario is an array of steps.
 * Each step has:
 *   - id: unique identifier
 *   - narration: what to say aloud
 *   - action: async (api) => void, calls API functions
 *   - highlight: optional, component selector to highlight
 */

export const scenarios = {
  sequential: [
    {
      id: 1,
      narration:
        'Node A starts with empty state. Sequential updates need only one active node.',
      action: async () => {
        await prepareScenario(['A'])
      },
      highlight: 'node-grid',
    },
    {
      id: 2,
      narration:
        "Node A writes user:1 = Alice. Its local clock advances to {A:1}.",
      action: async () => {
        await runPut('A', 'user:1', { name: 'Alice', ver: 1 })
      },
      highlight: 'nodecard-A',
    },
    {
      id: 3,
      narration:
        "A writes again — ver:2. Clock advances to {A:2}. Same node, sequential.",
      action: async () => {
        await runPut('A', 'user:1', { name: 'Alice', ver: 2 })
      },
      highlight: 'nodecard-A',
    },
    {
      id: 4,
      narration:
        'GET on A returns a single version. Two sequential writes on one node never conflict.',
      action: async () => {
        await runGet('A', 'user:1')
      },
      highlight: 'operation-panel',
    },
    {
      id: 5,
      narration:
        'Clock {A:2} dominates {A:1}. The system sees causal relationship — not concurrency.',
      action: async () => {
        // Informational, no API call
      },
      highlight: 'vector-clock-viz',
    },
  ],
  
  concurrent: [
    {
      id: 1,
      narration: 'Nodes A and B are isolated. Neither knows what the other will write.',
      action: async () => {
        await prepareScenario(['A', 'B'])
      },
      highlight: 'node-grid',
    },
    {
      id: 2,
      narration: 'A writes user:1 = Alice. Clock: {A:1}. B has no idea.',
      action: async () => {
        await runPut('A', 'user:1', { name: 'Alice' })
      },
      highlight: 'nodecard-A',
    },
    {
      id: 3,
      narration: 'B writes user:1 = Bob. Clock: {B:1}. A has no idea. Two clocks, no overlap.',
      action: async () => {
        await runPut('B', 'user:1', { name: 'Bob' })
      },
      highlight: 'nodecard-B',
    },
    {
      id: 4,
      narration:
        "Compare {A:1} and {B:1}: A has A:1 > B's A:0, but B has B:1 > A's B:0. Neither dominates. Concurrent.",
      action: async () => {
        // Informational
      },
      highlight: 'vector-clock-viz',
    },
    {
      id: 5,
      narration:
        'Trigger sync — A sends its state to B. B detects concurrency. Both versions stored as siblings.',
      action: async () => {
        await runSync('A', 'B')
      },
      highlight: 'operation-panel',
    },
    {
      id: 6,
      narration:
        'Conflict panel now shows two siblings: Alice {A:1} and Bob {B:1}. No data was lost.',
      action: async () => {
        await runConflicts('B')
      },
      highlight: 'conflict-panel',
    },
    {
      id: 7,
      narration:
        'Resolve conflict with an app policy: choose a canonical value and write a reconciled version on B.',
      action: async () => {
        await runPut('B', 'user:1', {
          name: 'Bob',
          resolution: 'manual-policy',
          resolved_from: ['Alice', 'Bob'],
          note: 'single canonical profile',
        })
      },
      highlight: 'operation-panel',
    },
    {
      id: 8,
      narration:
        'Sync resolved version from B back to A so the cluster converges on one version.',
      action: async () => {
        await runSync('B', 'A')
      },
      highlight: 'operation-panel',
    },
    {
      id: 9,
      narration:
        'Check conflicts again on B. Conflict list should now be empty for user:1.',
      action: async () => {
        await runConflicts('B')
      },
      highlight: 'conflict-panel',
    },
    {
      id: 10,
      narration:
        'Final GET on A returns one resolved value (no siblings). This is conflict handling, not just detection.',
      action: async () => {
        await runGet('A', 'user:1')
      },
      highlight: 'operation-panel',
    },
  ],
  
  failure: [
    {
      id: 1,
      narration: 'Healthy two-node cluster (A, B). A has user:1 = Alice {A:1}.',
      action: async () => {
        await prepareScenario(['A', 'B'])
        await runPut('A', 'user:1', { name: 'Alice' })
      },
      highlight: 'nodecard-A',
    },
    {
      id: 2,
      narration: 'Node B goes offline. Simulating a network partition or crash.',
      action: async () => {
        await runStopNode('B')
      },
      highlight: 'nodecard-B',
    },
    {
      id: 3,
      narration:
        'A writes user:1 = Alice v2 while B is down. B misses this entirely.',
      action: async () => {
        await runPut('A', 'user:1', { name: 'Alice', ver: 2 })
      },
      highlight: 'nodecard-A',
    },
    {
      id: 4,
      narration: 'A writes a second key. B still offline. Divergence grows.',
      action: async () => {
        await runPut('A', 'user:2', { name: 'Dave' })
      },
      highlight: 'operation-panel',
    },
    {
      id: 5,
      narration: "B comes back online. Its clock is still at zero — it missed everything.",
      action: async () => {
        await runRestartNode('B')
      },
      highlight: 'nodecard-B',
    },
    {
      id: 6,
      narration:
        'A syncs to B. B receives both writes. {A:2} dominates B\'s empty knowledge. No conflict.',
      action: async () => {
        await runSync('A', 'B')
      },
      highlight: 'operation-panel',
    },
    {
      id: 7,
      narration:
        'B reads user:1 — returns Alice ver:2 correctly. Eventual consistency achieved.',
      action: async () => {
        await runGet('B', 'user:1')
      },
      highlight: 'operation-panel',
    },
  ],
}

export const scenarioNames = {
  sequential: 'Sequential Update',
  concurrent: 'Concurrent Conflict',
  failure: 'Node Failure & Recovery',
}
