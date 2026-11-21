const BASE_URL = "https://data-api.polymarket.com";

function retryDelay(response, attempt) {
  const header = response.headers?.get?.("retry-after");
  if (header) {
    const seconds = Number(header);
    if (Number.isFinite(seconds) && seconds >= 0) return seconds * 1000;
    const date = Date.parse(header);
    if (Number.isFinite(date)) return Math.max(0, date - Date.now());
  }
  return Math.min(20_000, 500 * (2 ** attempt));
}

const defaultSleep = ms => new Promise(resolve => setTimeout(resolve, ms));

export async function requestJson(path, params = {}, options = {}) {
  const fetchImpl = options.fetchImpl ?? fetch;
  const sleepImpl = options.sleepImpl ?? defaultSleep;
  const attempts = options.attempts ?? 5;
  const url = new URL(path, BASE_URL);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") url.searchParams.set(key, String(value));
  }

  let lastError;
  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      // Every network request must be bounded. Without a default timeout,
      // one stalled Polymarket response can hold a worker forever and prevent
      // the scan from ever reaching JSON validation or publication.
      const signal = options.signal ?? AbortSignal.timeout(options.timeoutMs ?? 30_000);
      const response = await fetchImpl(url, {
        headers: { Accept: "application/json", "User-Agent": "PolymarketScannerV2/1.0" },
        signal
      });
      if (response.status === 429 || response.status === 503 || response.status >= 500) {
        const delay = retryDelay(response, attempt);
        await sleepImpl(delay);
        throw new Error(`Retryable Polymarket API response HTTP ${response.status}`);
      }
      if (!response.ok) {
        const body = await response.text();
        throw new Error(`Polymarket API HTTP ${response.status}: ${body.slice(0, 300)}`);
      }
      const payload = await response.json();
      if (!payload || typeof payload !== "object" || !("data" in payload)) {
        throw new Error(`Unexpected Polymarket API envelope from ${url.pathname}`);
      }
      return payload;
    } catch (error) {
      lastError = error;
      if (/Polymarket API HTTP 4\d\d:/.test(String(error?.message ?? error))) break;
      if (attempt < attempts - 1 && !String(error?.message ?? error).startsWith("Retryable Polymarket API response")) {
        await sleepImpl(Math.min(20_000, 500 * (2 ** attempt)));
      }
    }
  }
  throw lastError ?? new Error("Polymarket API request failed");
}

/**
 * Cursor-only pagination. The API's next_cursor is authoritative; never
 * invent offsets or continue when has_more is true but the cursor is absent.
 */
export async function fetchAllPages(path, params = {}, options = {}) {
  const request = options.request ?? requestJson;
  const rows = [];
  const seenCursors = new Set();
  const maxPages = options.maxPages ?? 1000;
  let cursor = null;
  let pages = 0;

  while (true) {
    if (++pages > maxPages) throw new Error(`Pagination safety limit exceeded for ${path}`);
    const pageParams = { ...params };
    if (cursor) pageParams.cursor = cursor;
    const payload = await request(path, pageParams, options);
    const pageRows = payload.data;
    if (pageRows === null) return rows;
    if (!Array.isArray(pageRows)) throw new Error(`Expected data[] from ${path}`);
    rows.push(...pageRows);

    // Discovery can stop a leaderboard stream as soon as its unique-wallet
    // quota is met. Other history callers omit stopWhen and still paginate fully.
    if (typeof options.stopWhen === "function" && options.stopWhen(pageRows, rows, payload)) {
      return rows;
    }

    const pagination = payload.pagination ?? {};
    if (pagination.has_more !== true) return rows;
    const next = pagination.next_cursor;
    if (typeof next !== "string" || !next) {
      throw new Error(`API reported has_more without next_cursor for ${path}; refusing partial history`);
    }
    if (seenCursors.has(next)) throw new Error(`Repeated cursor while paging ${path}`);
    seenCursors.add(next);
    cursor = next;
  }
}
