# Session-token auth (`ripclone auth login`)

The backend authenticates every request with a shared server token: clients send
`Authorization: Ripclone <sha256(token)>` (vanilla git uses Basic with the same
hash). That works, but it means the long-lived secret is stored on the client and
sent on every request.

Session tokens add a second, short-lived credential on top of the same model —
they don't replace it, and they don't introduce user accounts (multi-tenant
identity stays in `ripclone-cloud`).

## Flow

1. `ripclone auth login` opens a browser to `GET /login` on the configured
   server, passing a one-shot loopback callback.
2. The page asks for the **server token** (the raw `RIPCLONE_SERVER_TOKEN`). On
   submit, `POST /v1/auth/login` verifies `sha256(secret)` against the configured
   hash (constant-time) and mints a short-lived **HS256 JWT**.
3. With a loopback callback, the browser is redirected back to the CLI's
   localhost listener, which captures the token automatically. Headless? The page
   shows the token to paste into the CLI instead.
4. The CLI saves the JWT in the ripclone token file keyed per server and sends
   it as `Authorization: Bearer <jwt>` on subsequent requests.
5. `POST /v1/auth/refresh` (authenticated) mints a fresh token before expiry. The
   re-issued token keeps the **same absolute session deadline** as the original,
   so a refresh chain can't outlive the session cap — once the deadline passes you
   must log in again. (The CLI re-logs in rather than auto-refreshing; the endpoint
   is there for clients/automation that want a sliding session.)

`ripclone auth logout` removes the saved token; `ripclone auth status` shows
whether one is saved and when it expires.

Precedence: an explicit env server token (`RIPCLONE_SERVER_TOKEN[_HASH]`) always wins; otherwise a valid saved session token is
preferred over the saved login token.

## Signing key

The JWT signing key must be unknown to clients (they hold the token *hash*), so it
is **never** derived from the hash:

- `RIPCLONE_JWT_SECRET` if set, else
- `HMAC-SHA256(raw RIPCLONE_SERVER_TOKEN, "ripclone-jwt-signing-v1")`.

If only `RIPCLONE_SERVER_TOKEN_HASH` is configured (no raw token, no JWT secret),
issuance is **disabled** — `/v1/auth/login` returns 503 and `Bearer` tokens are
rejected — so the server never signs with material a client already holds. Set
`RIPCLONE_JWT_SECRET` to enable session tokens in that deployment.

## Config

| Env | Meaning |
|---|---|
| `RIPCLONE_JWT_SECRET` | Explicit HS256 signing secret (hashed to a 32-byte key; warns if shorter than 32 chars). Falls back to deriving from the raw server token. |
| `RIPCLONE_JWT_TTL_SECS` | Token lifetime (default 3600). |
| `RIPCLONE_JWT_SESSION_MAX_SECS` | Absolute session lifetime — the hard cap a refresh can't extend past (default 86400; floored at the TTL). |

## Security notes

- The login page and the login exchange are unauthenticated (they mint a
  credential by proving the secret) but rate-limited against brute force; the
  secret check is constant-time.
- The callback is only ever a **loopback** address (`127.0.0.1` / `localhost` /
  `[::1]`); userinfo (`…@host`) and control characters are rejected, and query
  values are percent-encoded, so the minted token can't be redirected to an
  external host or used to split the response.
- Tokens are stateless (no server-side revocation list), so leak containment
  comes from the short TTL plus the absolute session cap (`sxp` claim) that bounds
  how long a token can be refreshed. Keep both short for sensitive deployments.
- The login redirect and token page are served `Cache-Control: no-store`.
