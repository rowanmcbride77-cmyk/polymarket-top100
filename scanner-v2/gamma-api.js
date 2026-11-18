const BASE = "https://gamma-api.polymarket.com";
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

export async function gammaJson(path, params = {}, options = {}) {
  const fetchImpl = options.fetchImpl ?? fetch;
  const url = new URL(path, BASE);
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") url.searchParams.set(key, String(value));
  }
  const attempts = options.attempts ?? 4;
  let lastError;
  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      const response = await fetchImpl(url, { headers: { Accept: "application/json" }, signal: options.signal });
      if (response.status === 429 || response.status === 503 || response.status >= 500) {
        const retry = Number(response.headers?.get?.("retry-after"));
        await sleep(Number.isFinite(retry) && retry >= 0 ? retry * 1000 : Math.min(15000, 500 * 2 ** attempt));
        throw new Error(`Retryable Gamma response HTTP ${response.status}`);
      }
      if (!response.ok) throw new Error(`Gamma API HTTP ${response.status}: ${(await response.text()).slice(0, 250)}`);
      return await response.json();
    } catch (error) {
      lastError = error;
      if (/Gamma API HTTP 4\d\d:/.test(String(error?.message ?? error))) break;
      if (attempt < attempts - 1 && !String(error?.message ?? error).startsWith("Retryable Gamma")) {
        await sleep(Math.min(15000, 500 * 2 ** attempt));
      }
    }
  }
  throw lastError ?? new Error("Gamma API request failed");
}

export async function fetchGammaMarkets(conditionIds, options = {}) {
  const request = options.gammaJson ?? gammaJson;
  const unique = [...new Set((conditionIds ?? []).map(value => String(value).trim().toLowerCase()).filter(Boolean))];
  const markets = [];
  for (let i = 0; i < unique.length; i += 100) {
    const batch = unique.slice(i, i + 100);
    const params = { limit: 1000, condition_ids: batch.join(",") };
    const payload = await request("/markets", params, options);
    const rows = Array.isArray(payload) ? payload : Array.isArray(payload?.data) ? payload.data : [];
    if (!Array.isArray(rows)) throw new Error("Unexpected Gamma markets response");
    markets.push(...rows);
  }
  return markets;
}
