# mailbox-e2e — black-box acceptance gate for `devcloud-mailbox`

Run from the repo root: `bash scripts/mailbox-e2e.sh` (needs python3, curl, cargo; docker for the MailHog oracle). Every check prints `[PASS]/[FAIL]/[SKIP]/[INFO] AC-xx ...`; exit 1 on any FAIL.

- **Stages:** build (release by default) → start the binary on free 127.0.0.1 ports with temp storage → start `mailhog/mailhog:v1.0.1` (`--platform linux/amd64`) as the DIFF oracle → `harness.py` → SIGTERM exit check → AC-24 grep ban → optional Docker and asset stages.
- **harness.py** (stdlib only, written from the spec): DIFF against MailHog for v1/v2/search/download/part/SSE (AC-03..09, 17, 26), FIX decoding/attachment/HTML-header checks (AC-10..12a), strict auth (AC-14), CORS/traversal (AC-15), hostile MIME and limits (AC-21/22), Host allowlist and 403 guidance (AC-23/30), ephemeral mode (AC-29), header sweep (AC-24), restart and startup failures (AC-02/25), embedded assets (AC-27), timing (AC-19), log hygiene (AC-18).
- `CARGO_OFFLINE=1`: build with `--offline`. `E2E_CARGO_PROFILE=debug`: test a debug build (AC-19 budgets assume release).
- `E2E_SLOW=1`: SSE heartbeat ≤30 s, HTTP header timeout 10 s, SMTP idle 300 s, SSE slot release (adds ~6 min).
- `E2E_DOCKER=1`: build `services/mailbox/Dockerfile`; check non-root, EXPOSE, VOLUME, binds, round trip, `docker stop` with 2 SSE clients, restart persistence, log hygiene and read-only `/data`, `-e DEVCLOUD_MAILBOX_EPHEMERAL=true` restart (AC-01/02/18/25/29). `E2E_DOCKER_MULTIARCH=1` adds the buildx amd64+arm64 build.
- `E2E_ASSETS=1`: `npm ci && npm run build` in `web/mailbox`, then require byte-identical `services/mailbox/assets/ui` (AC-27).
- `E2E_ALLOW_SKIP=1`: accept a run without docker (DIFF checks SKIP); otherwise a missing oracle fails the run.
- Not automated here: AC-12/12a/13/13a (browser, hub), AC-16/28 (`cargo test --workspace`, `scripts/mail-autoloop/verify.sh`), AC-20 (README review).
- Self-test: `python3 scripts/mailbox-e2e/harness.py --only compat --ours-smtp P --ours-http P --oracle-smtp Q --oracle-http Q` against two MailHog containers. Only the ADR-mandated divergences should fail.
