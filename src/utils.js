export function buildCredentials(apiKey, secret) {
  return `
-----BEGIN NATS USER JWT-----
${apiKey}
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
${secret}
------END USER NKEY SEED------

*************************************************************`;
}

import { JSONCodec } from "nats.ws";
import axios from "axios";

const _codec = JSONCodec();

// ─── HTTP history (influx-db-service) ─────────────────────────────────────
//
// The history endpoints moved off NATS streaming onto the influx-db-service
// REST API. Auth mirrors OTA: exchange the NATS credential for a short-lived
// HS256 bearer + base URL via accounts.user.get_http_token (service "influx"),
// cached on ctx and shared by every history() method, refetched on a 401/403.

async function ensureInfluxAuth(ctx, force = false) {
  if (!force && ctx._influxToken && ctx._influxUrl) {
    return { token: ctx._influxToken, url: ctx._influxUrl };
  }

  let reply;
  try {
    const res = await ctx.natsClient.request(
      "accounts.user.get_http_token",
      _codec.encode({ jwt: ctx.apiKey, service: "influx" }),
      { timeout: 20_000 },
    );
    reply = res.json();
  } catch (err) {
    throw new Error(`get_http_token (influx) failed: ${err.message}`);
  }

  if (reply?.status !== "HTTP_TOKEN_SUCCESS" || !reply?.data?.token || !reply?.data?.http_url) {
    const reason = reply?.msg || reply?.data?.msg || reply?.status || "unknown error";
    throw new Error(`get_http_token (influx) failed: ${reason}`);
  }

  ctx._influxToken = reply.data.token;
  ctx._influxUrl = reply.data.http_url.replace(/\/+$/, "");
  return { token: ctx._influxToken, url: ctx._influxUrl };
}

// Pull a readable message out of the influx-db-service error envelope.
function readHistoryError(body, httpStatus) {
  const d = body?.data;

  const errorMessage =
    d?.code ||
    d?.message ||
    (Array.isArray(d?.errors) ? d.errors.join(", ") : `HTTP ${httpStatus}`);

  return { status: d?.code, errorMessage };
}

/**
 * POST a history query to the influx-db-service and collect ALL pages.
 *
 * The REST endpoint paginates (limit/offset, has_more/next_offset); this loops
 * through every page so the caller gets the full range, matching the old NATS
 * streaming behavior. Each frame is the endpoint's raw row — for events,
 * { <name>: { value, timestamp } }.
 *
 * Returns { frames } on success, or { error, status, errorMessage, frames } on
 * failure (frames = whatever arrived before the error). Callers check
 * result.error, then aggregate result.frames.
 */
export async function httpHistory(ctx, path, payload, { pageLimit = 10_000 } = {}) {
  const frames = [];

  let offset = 0;

  // Fetch the token once up front; only re-fetch if a page comes back 401/403.
  let { token, url } = await ensureInfluxAuth(ctx);
  let triedRefresh = false;

  while (true) {
    let res;
    try {
      res = await axios.post(
        `${url}${path}`,
        { ...payload, limit: pageLimit, offset },
        {
          headers: { Authorization: `Bearer ${token}` },
          validateStatus: () => true,
        },
      );
    } catch (err) {
      const errorMessage = err.code || err.message || "network error";
      return { error: true, errorMessage, frames };
    }

    // Token expired / invalid: refresh once, then retry the same page.
    if ((res.status === 401 || res.status === 403) && !triedRefresh) {
      triedRefresh = true;
      ({ token, url } = await ensureInfluxAuth(ctx, true));
      continue;
    }

    const body = res.data;

    if (res.status !== 200 || !body?.status) {
      return { error: true, ...readHistoryError(body, res.status), frames };
    }

    const { frames: pageFrames, page } = body.data;

    for (const frame of pageFrames ?? []) {
      frames.push(frame);
    }

    if (!page?.has_more) {
      break;
    }

    offset = page.next_offset;
    triedRefresh = false; // allow one refresh per page if a long run outlives the token
  }

  return { frames };
}

export function topicPatternMatcher(patternA, patternB) {
  const a = patternA.split(".");
  const b = patternB.split(".");

  let i = 0,
    j = 0;
  let starAi = -1,
    starAj = -1;
  let starBi = -1,
    starBj = -1;

  while (i < a.length || j < b.length) {
    const tokA = a[i];
    const tokB = b[j];

    if (tokA === ">") {
      if (i !== a.length - 1) return false;
      if (j >= b.length) return false;
      starAi = i++;
      starAj = ++j;
      continue;
    }
    if (tokB === ">") {
      if (j !== b.length - 1) return false;
      if (i >= a.length) return false;
      starBi = j++;
      starBj = ++i;
      continue;
    }

    const singleWildcard =
      (tokA === "*" && j < b.length) || (tokB === "*" && i < a.length);

    if ((tokA !== undefined && tokA === tokB) || singleWildcard) {
      i++;
      j++;
      continue;
    }

    if (starAi !== -1) {
      j = ++starAj;
      continue;
    }
    if (starBi !== -1) {
      i = ++starBj;
      continue;
    }

    return false;
  }

  return true;
}
