/**
 * API Layer for Repliqia Orchestrator
 * 
 * All functions return { data, error }
 * - data: response from server (null on error)
 * - error: error message (null on success)
 */

const BASE_URL = ''

async function fetchJson(url, options = {}) {
  try {
    const response = await fetch(url, {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
      ...options,
    })

    const raw = await response.text()
    let data = null
    if (raw) {
      try {
        data = JSON.parse(raw)
      } catch {
        data = { message: raw }
      }
    }
    
    if (!response.ok) {
      return {
        data: null,
        error: data?.error || data?.message || `HTTP ${response.status}`,
      }
    }
    
    return { data, error: null }
  } catch (err) {
    return {
      data: null,
      error: err.message || 'Network error',
    }
  }
}

// ============================================================================
// NODE LIFECYCLE
// ============================================================================

export async function startNode(node_id, options = {}) {
  const {
    n = 3,
    r = 2,
    w = 2,
  } = options
  
  return fetchJson(`${BASE_URL}/nodes/start`, {
    method: 'POST',
    body: JSON.stringify({
      node_id: node_id.toUpperCase(),
      n,
      r,
      w,
    }),
  })
}

export async function stopNode(node_id) {
  return fetchJson(`${BASE_URL}/nodes/${node_id.toUpperCase()}/stop`, {
    method: 'POST',
  })
}

export async function restartNode(node_id) {
  return fetchJson(`${BASE_URL}/nodes/${node_id.toUpperCase()}/restart`, {
    method: 'POST',
  })
}

export async function getNodes() {
  return fetchJson(`${BASE_URL}/nodes`, {
    method: 'GET',
  })
}

export async function deleteNode(node_id) {
  return fetchJson(`${BASE_URL}/nodes/${node_id.toUpperCase()}`, {
    method: 'DELETE',
  })
}

// ============================================================================
// KVSTORE OPERATIONS
// ============================================================================

export async function proxyPut(node_id, key, value) {
  return fetchJson(`${BASE_URL}/proxy/${node_id.toUpperCase()}/kvstore/${key}`, {
    method: 'PUT',
    body: JSON.stringify({ value }),
  })
}

export async function proxyGet(node_id, key) {
  return fetchJson(`${BASE_URL}/proxy/${node_id.toUpperCase()}/kvstore/${key}`, {
    method: 'GET',
  })
}

export async function proxyDelete(node_id, key) {
  return fetchJson(`${BASE_URL}/proxy/${node_id.toUpperCase()}/kvstore/${key}`, {
    method: 'DELETE',
  })
}

// ============================================================================
// REPLICATION & SYNC
// ============================================================================

export async function proxySync(node_id, peer_id, key = null) {
  const payload = key ? { key } : {}

  return fetchJson(`${BASE_URL}/proxy/${node_id.toUpperCase()}/sync/${peer_id.toUpperCase()}`, {
    method: 'POST',
    body: JSON.stringify(payload),
  })
}

// ============================================================================
// STATE & INSPECTION
// ============================================================================

export async function proxyConflicts(node_id) {
  return fetchJson(`${BASE_URL}/proxy/${node_id.toUpperCase()}/conflicts`, {
    method: 'GET',
  })
}

export async function proxyState(node_id) {
  return fetchJson(`${BASE_URL}/proxy/${node_id.toUpperCase()}/node/state`, {
    method: 'GET',
  })
}

export async function proxyClock(node_id) {
  return fetchJson(`${BASE_URL}/proxy/${node_id.toUpperCase()}/node/clock`, {
    method: 'GET',
  })
}

// ============================================================================
// DEMO
// ============================================================================

export async function resetDemo() {
  return fetchJson(`${BASE_URL}/demo/reset`, {
    method: 'POST',
  })
}
