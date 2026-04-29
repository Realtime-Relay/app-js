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

/**
 * Drives the streaming-history protocol against the db_manager service.
 *
 * Wire protocol:
 *   1. Generate a stream_token (uuid).
 *   2. Subscribe to `export.<orgID>.<env>.<token>` BEFORE sending the request,
 *      to avoid the race where frames arrive before the subscription is set up.
 *   3. Send the NATS request including stream_token in the payload.
 *   4. Server replies with one of three statuses:
 *        *_FETCH_STREAM_STARTED      → server is waiting on ready signal
 *        *_FETCH_SUCCESS_NO_STREAM   → no data; no frames will be published
 *        *_FETCH_FAILURE             → validation or query error
 *   5. On STREAM_STARTED: publish anything to `data.ready_subject` to signal
 *      readiness; server begins streaming.
 *   6. Receive frames until `last: true`. Each frame's shape is endpoint-
 *      specific.
 *
 * Returns:
 *   { status: <reply status>, data: <reply.data | undefined>, frames: [...] }
 *
 * Options:
 *   - onFrame(frame): if provided, frames are dispatched to this callback as
 *     they arrive AND collected. Use for live UI streaming.
 *   - requestTimeoutMs: timeout for the initial NATS request (default 20000).
 *   - idleTimeoutMs: max time between frames before aborting (default 30000).
 */
import { JSONCodec } from "nats.ws";
import {
  decode as msgpackDecode,
  encode as msgpackEncode,
} from "@msgpack/msgpack";

const _codec = JSONCodec();

export async function streamHistory(
  ctx,
  requestSubject,
  payload,
  { onFrame, requestTimeoutMs = 20_000, idleTimeoutMs = 30_000 } = {},
) {
  const streamToken = crypto.randomUUID();
  const exportSubject = `import.${ctx.orgID}.${ctx.env}.history.${streamToken}`;

  // Subscribe BEFORE sending the request so we never miss a frame.
  const sub = ctx.natsClient.subscribe(exportSubject);

  let res;
  try {
    res = await ctx.natsClient.request(
      requestSubject,
      _codec.encode({ ...payload, stream_token: streamToken }),
      { timeout: requestTimeoutMs },
    );
  } catch (err) {
    sub.unsubscribe();
    throw err;
  }

  let decoded;
  try {
    decoded = msgpackDecode(res.data);
  } catch {
    // Some endpoints reply with JSONCodec on errors, msgpack on success.
    decoded = JSON.parse(new TextDecoder().decode(res.data));
  }

  const status = decoded?.status;

  // No data — nothing will be published. Clean up and return.
  if (typeof status === "string" && status.endsWith("_NO_STREAM")) {
    sub.unsubscribe();
    return { status, data: decoded.data, frames: [] };
  }

  // Anything other than STREAM_STARTED: failure.
  if (typeof status !== "string" || !status.endsWith("_STREAM_STARTED")) {
    sub.unsubscribe();
    return { status, data: decoded?.data, frames: [], error: true };
  }

  const readySubject = decoded.data?.ready_subject;
  if (!readySubject) {
    sub.unsubscribe();
    throw new Error(
      `${requestSubject} replied STREAM_STARTED without ready_subject`,
    );
  }

  // Signal ready: server begins publishing now. Body is unused server-side,
  // but nats.ws expects a Uint8Array payload — send an empty msgpack object.
  ctx.natsClient.publish(readySubject, msgpackEncode({}));

  // Collect frames until `last: true` or idle timeout.
  const frames = [];
  let idleTimer;
  let timedOut = false;

  const resetIdle = () => {
    clearTimeout(idleTimer);
    idleTimer = setTimeout(() => {
      timedOut = true;
      try {
        sub.unsubscribe();
      } catch {}
    }, idleTimeoutMs);
  };

  resetIdle();

  try {
    for await (const msg of sub) {
      resetIdle();
      const frame = msgpackDecode(msg.data);
      frames.push(frame);

      if (onFrame) {
        try {
          onFrame(frame);
        } catch (cbErr) {
          ctx.logger?.error?.("onFrame callback threw", cbErr);
        }
      }

      if (frame.last) {
        break;
      }
    }
  } finally {
    clearTimeout(idleTimer);
  }

  if (timedOut) {
    const last = frames[frames.length - 1];
    if (!last || !last.last) {
      throw new Error(
        `streamHistory: idle timeout after ${idleTimeoutMs}ms on ${requestSubject}`,
      );
    }
  }

  // The final frame may carry an error sentinel from the server (mid-stream
  // failure). Surface it so the caller can decide what to do.
  const last = frames[frames.length - 1];
  if (last && last.error) {
    return { status, frames, error: true, errorMessage: last.error };
  }

  return { status, frames };
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
