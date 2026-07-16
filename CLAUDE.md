# Fandora

Fan + Pandora. AI influencer platform — Instagram-style unified account model + AI character DM.

**websocket-server** — real-time push service for nadfun.com; consumes the observer's internal event stream, aggregates Pyth oracle prices for native and quote tokens, and distributes live trade/token/price data to connected browser and mobile clients over WebSocket; deployed across four AWS ECS regions (Tokyo, Singapore, Paris, Ohio).

## 🔴 Absolute rule — Test-Driven Development

**Every code change goes through TDD via `superpowers:test-driven-development` (or `/tdd`). No "add tests later".** Only doc/config/pure-scaffold changes (no code) are exempt.

- Coverage target: **80%+** (unit + integration + E2E as appropriate).
- Pre-commit: `superpowers:verification-before-completion` — run the narrowest `cargo test` scope that covers the change. Strategy lives in `docs/backend/testing-strategy.md`.
- Role split under model routing (next section): **the Worker (Opus) writes the tests and the implementation; a dispatched code-review subagent reviews the diff; the Advisor adjudicates findings and runs the tests itself.** The TDD discipline itself (RED before GREEN) is unchanged.

This rule overrides every other workflow rule in this file.

## 🔴 Absolute rule — Model routing (Advisor / Worker)

The orchestrating session — whatever model it runs on — is the **Advisor**. Its job is judgment, not implementation labor: it does not write or edit production code or tests itself, except for trivial finishing touches (see Boundaries).

**The Advisor (you, the main session) does directly:**
- Requirements analysis, task decomposition, design decisions.
- Writing task briefs for the Worker.
- Verifying results: dispatch the code-review subagent (see below), adjudicate its findings, and run the tests yourself.
- Final verification, git commits, branch docs, `chang.md`/`complete.md` lifecycle, PR authoring, reporting to the user — mechanical execution/record-keeping of decisions already made, not a delegated implementation role.

**Delegated to the Worker (Opus subagent):**
- All implementation labor — writing and editing code, writing tests, everything.
- Dispatch via the Agent tool with `model: "opus"`.
- Independent tasks are dispatched in parallel.

**Code review is ALWAYS a subagent (2026-07-05 revision):**
- Every code-diff review — task-level and whole-PR — is dispatched to a dedicated code-review subagent (Agent tool, `model: "opus"`, fresh context). Never reviewed solely by the Advisor inline, and never by the Worker that wrote the diff.
- The review dispatch carries: the diff handed as a file (not pasted), the task brief/plan, the binding constraints and named risks, and asks for severity-graded findings (`[P1]`/`[P2]`/`[P3]`).
- The Advisor adjudicates the findings and still runs the covering tests itself — the reviewer's report plus green tests form the gate; neither alone is sufficient.

**Brief requirements** — write the brief so the Worker never has to re-discover what you already know:
- Carry the context you (the Advisor) already gathered.
- Include file paths, project conventions, known pitfalls (e.g. the `RUSTC_WRAPPER=` prefix required on every cargo/git invocation — sccache breaks in this sandbox otherwise; do not modify a test file/fixture/snapshot/seed/mock without explicit authorization in the brief), and completion criteria (which tests must pass).
- State the filesystem boundary: the Worker must not read `~/.claude/`, `~/.agents/`, `.claude/`, or `agents/`.

**Boundaries:**
- Do not take the Worker's completion report at face value. Gate on the dispatched review's findings plus tests you ran yourself before approving.
- A failed verification goes back to the Worker as a fix brief. Direct fixes by the Advisor are allowed only for trivial finishing touches.
- Work where delegation overhead exceeds the task itself (e.g. a one- or two-line fix) may be done directly by the Advisor.

## 🔴 Absolute rule — Subagent code review before every PR

**Every diff is code-reviewed by a dispatched review subagent before opening/merging a PR — the Worker's own report is never the gate (self-attestation is not a gate), and the Advisor signing off alone inline is not a gate either (2026-07-05 revision: code review is ALWAYS a subagent dispatch).**

Procedure: dispatch a code-review subagent (Opus, fresh context — not the Worker that wrote the diff) on the FULL diff (`git diff origin/backend...HEAD`), handed as a file, together with the plan/brief and named risks. It reviews hunk by hunk — correctness, safety boundaries, test-intent (Rule 9), conventions — and returns severity-graded findings (`[P1]`/`[P2]`/`[P3]`). The Advisor adjudicates the findings and runs (or re-runs) the *whole* covering test suite itself, not just the changed files' tests (a routing/contract change can break far-away tests). **Record the reviewer's verdict + findings and the Advisor's adjudication in the PR body's 리뷰 이력 section and the branch concept doc.** A PR with no written review record does not count as reviewed.

A `[P1]`/CRITICAL finding = GATE FAIL = do not open/merge the PR until fixed.

- Fix trivial findings yourself; send anything larger back to the Worker as a fix brief (per the Model routing Boundaries above), then re-dispatch the reviewer on the amended diff.
- Do not run the gstack `/review` skill on the same diff twice.

Specs/design docs/written plans before they move to implementation remain **Advisor-direct** verification (the always-subagent rule above covers CODE diffs): the Advisor reads and verifies them itself.

Re-verification before merge is NOT mandatory for every incremental commit landing on an open PR — judge case-by-case.

## 🔴 Absolute rule — Test session cleanup

**When a test-involving task wraps up (before reporting "done" / before commit / before PR / before hand-off), run the full cleanup protocol below in order.** Scope is **task wrap-up**, NOT after every `cargo test` invocation — TDD red/green/refactor loops do not cargo-clean between iterations.

Order (sequential; surface any non-zero exit to the user — Rule 12 "Fail loud"):

1. **Kill leftover cargo/test child processes**
   ```bash
   pkill -f 'cargo test' 2>/dev/null || true
   pkill -f 'target/debug/deps/' 2>/dev/null || true
   pkill -f 'target/release/deps/' 2>/dev/null || true
   ```
2. **Stop Claude-spawned Ollama eval process only** (system Ollama / brew service / other sessions' Ollama — leave alone, per `docs/backend/testing-strategy.md`)
   ```bash
   test -f /tmp/fandora-ollama.pid && kill "$(cat /tmp/fandora-ollama.pid)" 2>/dev/null && rm /tmp/fandora-ollama.pid || true
   ```
3. **Stop dev DB + Redis**
   ```bash
   docker compose down
   ```

**Do NOT `cargo clean`.** Keep `target/` warm — incremental builds are the whole point. A full clean forces a multi-minute recompile on the next build for no benefit. (Reversed 2026-06-08: the old protocol cleaned `target/`; it caused repeated wasteful rebuilds.)

Overrides:
- If the user explicitly says "leave DB up" — skip that step and note it.
- If a long-running task (background eval, ongoing migration, separate worktree using the same DB) would be killed by cleanup — surface that and ask before proceeding.

## 🔴 Absolute rule — Every task gets its own branch AND its own worktree

**Never work in the repo's primary working tree, and never commit onto a long-lived branch (`main`/`front`/`mobile`/`backend`). Before touching a tracked file, create a new branch off the freshly fetched base and check it out in a dedicated git worktree. All work for that task happens inside that worktree.**

Use `superpowers:using-git-worktrees`. Worktree location convention: `~/.config/superpowers/worktrees/backend/<slug>`.

```bash
RUSTC_WRAPPER= git fetch origin
RUSTC_WRAPPER= git worktree add -b <type>/<slug> \
  ~/.config/superpowers/worktrees/backend/<slug> origin/backend
```

One task = one branch = one worktree. Do not stack an unrelated change onto a branch that already has an open PR — it pollutes that PR's diff and blocks its merge.

Why this is absolute (each has burned us):
- **The primary working tree is shared state.** A subagent or a background review may be reading it on another task's branch. `git checkout` under them corrupts what they see.
- **`git checkout <branch>` fails when that branch is checked out in another worktree.** The failure is easy to miss inside a `||` fallback, and a following `git reset --hard` then lands on *the current* branch instead. (Hit for real in the `docs/` submodule, 2026-07-10.) When you only need a submodule's gitlink to point somewhere, use `git checkout --detach <sha>` — never `checkout <branch>`.
- **Branch switching in one tree invalidates `target/`.** Separate worktrees keep separate `target/` dirs, so two tasks stop thrashing each other's incremental cache. The price is one cold build per new worktree — accepted; do not `cargo clean` on top of it.

Constraints inside a worktree:
- **The dev DB and Redis (`5433`/`6380`) are shared across worktrees.** Another session may hold them. Before `docker compose down`, check (Test session cleanup override above). To reproduce a suspected flake in isolation, bring up a throwaway container on different ports (e.g. `5434`/`6381`) rather than touching the shared one.
- Every non-trivial branch keeps its concept doc at `branches/<branch-name>.md` (see "Branch concept doc" below).

**After the PR merges, remove that branch's worktree — but keep the branch.**

```bash
RUSTC_WRAPPER= git worktree remove ~/.config/superpowers/worktrees/backend/<slug>
RUSTC_WRAPPER= git worktree prune
```

The two are different things and only one is allowed:
- **Worktree**: delete it once the branch is merged. A stale worktree keeps a merged branch checked out, which makes `git checkout <branch>` fail elsewhere (the failure mode described above) and leaves a multi-GB `target/` behind.
- **Branch**: never delete, local or remote (`--delete-branch` is forbidden) unless the user explicitly asks.

If `git worktree remove` refuses because the tree is dirty, do not `--force` blindly: show the user what is uncommitted first.

Exceptions (narrow, and say so out loud when you take one):
- Read-only investigation that writes nothing tracked.
- Continuing work already in progress on its own task branch/worktree.
- Scratchpad files outside the repo.
- `next_session.md` — a tracked handoff note that is deliberately kept as an uncommitted working-tree edit in the primary tree. Update it in place; never commit it onto a task branch.

## Workflow

Skill catalogs (canonical reference):
- `skills/superpowers.md` — process/execution skills (TDD, planning, debugging, review, parallelism).
- `skills/gstack.md` — slash-command skills (`/office-hours`, `/plan-eng-review`, `/qa`, `/ship`, `/cso`, ...).
- `skills/gsd.md` — phase-based "Get Shit Done" skills (project/milestone/phase lifecycle).

Hard rules:
- Before touching a tracked file: new branch + dedicated worktree (`superpowers:using-git-worktrees`). See the absolute rule above.
- Start every bug fix with `superpowers:systematic-debugging` — root cause before any fix.
- Multi-file work: `superpowers:writing-plans` → `superpowers:executing-plans`.
- 2+ independent tasks: `superpowers:dispatching-parallel-agents`.
- Before code: when UI is involved, run `/plan-design-review` in addition to `/plan-eng-review`.
- **Spec/design docs always get Advisor verification.** Any spec or design doc (brainstorming output, `planning/*` design files, written plans) MUST be read and verified by the Advisor itself before transitioning to `writing-plans`/implementation — no separate review subagent. Fix trivial issues directly; confirm anything larger with the user. This is separate from the pre-PR verification rule above.

## Feature documentation

- `docs/` is a git submodule pointing to `git@github.com:Fandora-labs/docs.git`.
- To edit docs: branch inside `docs/`, commit, push, open a PR against `Fandora-labs/docs`. **Direct push to `docs` `main` is forbidden.**
- After the `docs` PR merges, bump the `docs` submodule pointer in the parent repo and include the gitlink change in the relevant backend PR/commit.
- A code-change PR that requires doc changes ships only the submodule pointer bump in the parent repo — never raw doc files.
- If `docs/` is empty after clone, run `git submodule update --init --recursive`.
- Every backend feature creates or updates `docs/backend/features/<feature-name>.md`:
  - Planning: requirements, design decisions, architecture.
  - Implementation: implementation detail, API spec, data model.
  - Code change: synchronize altered behavior and new constraints.
- The feature doc is the single source of truth for that feature.
- `docs/backend/features/` is owned by the `backend` domain (matches the monorepo branch policy below). Front/mobile feature docs go under their respective domain dirs (`docs/front/`, `docs/mobile/`).

## TODO

- `TODO.md` tracks all work items. Check off (`- [ ]` → `- [x]`) when starting; add new tasks as they emerge; keep current.

## Language

- Code / commit messages: English.
- **PR descriptions (titles + bodies): always Korean, written so anyone — not just the engineer who wrote the diff — can understand what changed and why.** Keep code identifiers, paths, commands, and commit hashes verbatim.
- Documentation under `docs/`: Korean. Code identifiers, API paths, DB columns, enum values, commands, and commit hashes stay verbatim. When editing an existing English `docs/` file, convert the touched section to Korean.
- User-facing conversation: Korean.
- This file (`CLAUDE.md`): English.

## Monorepo & branch policy

Branch-per-domain, with matching doc directory.

| Domain | Branch | Doc dir |
|--------|--------|---------|
| Common / docs / infra | `main` | `docs/app/` |
| Frontend (Next.js) | `front` | `docs/front/` |
| Mobile (Expo / React Native) | `mobile` | `docs/mobile/` |
| Backend (Rust/Axum) | `backend` | `docs/backend/` |

- Domain rules: `front` → `~/.claude/commands/frontend-rule.md`; `mobile` → `docs/mobile/spec.md`.
- Each branch merges into `main` via PR.
- **Always sync local after a PR merges.** The moment any PR is merged, reflect it locally: `git fetch origin`, then fast-forward the merged base branch to its remote (e.g. backend PR merged → update local `backend` to `origin/backend`; a `backend`→`main` sync PR merged → update local `main`). If the active working branch sits on that base, pull the new base in (rebase/merge). Never leave the local base stale after a merge.

## Changelog (`chang.md` / `complete.md`)

Each domain dir (`docs/app/`, `docs/front/`, `docs/mobile/`, `docs/backend/`) holds two files:

- **`chang.md`** — pending / in-progress changes.
- **`complete.md`** — completed change log (date + commit hash).

Rules:
- When types change or anything is modified, write the entry in the corresponding `chang.md` first (domain mapping = the branch table above).
- Once the change is implemented, remove it from `chang.md` and append to `complete.md` with date + commit hash.
- `complete.md` format: `## [YYYY-MM-DD] <short-hash> — <type>: <title>` + `[commit](URL)` + bullet list of changes.
- **Apply this lifecycle without exception** — every change follows `chang.md` → implementation → `complete.md`.

## Branch concept doc

Every non-trivial side branch (`feat/*`, `fix/*`, `x-*`, ad-hoc names — anything that isn't `main`/`front`/`mobile`/`backend`) keeps a one-page concept doc at `branches/<branch-name>.md` (repo root).

- **Purpose** — what this branch is for (1–3 lines). Written at branch creation.
- **Changes** — running list of what was added / fixed / removed as the branch progresses.
- **Outcome** — at PR / merge: final summary + key commit hashes + PR link.

The doc commits alongside code in the same branch — no docs submodule round-trip. Long-lived domain branches (`main`/`front`/`mobile`/`backend`) are already covered by `chang.md`/`complete.md` and don't need a separate concept doc. Single-commit trivial branches (typo, style, comment-only) may skip the doc.

---

# 12-rule behavioral contract (Karpathy + Chang baseline)

These rules apply to every task in this project unless explicitly overridden.
Bias: caution over speed on non-trivial work. Use judgment on trivial tasks.

## Rule 1 — Think Before Coding
State assumptions explicitly. If uncertain, ask rather than guess.
Present multiple interpretations when ambiguity exists.
Push back when a simpler approach exists.
Stop when confused. Name what's unclear.

## Rule 2 — Simplicity First
Minimum code that solves the problem. Nothing speculative.
No features beyond what was asked. No abstractions for single-use code.
Test: would a senior engineer say this is overcomplicated? If yes, simplify.

## Rule 3 — Surgical Changes
Touch only what you must. Clean up only your own mess.
Don't "improve" adjacent code, comments, or formatting.
Don't refactor what isn't broken. Match existing style.

## Rule 4 — Goal-Driven Execution
Define success criteria. Loop until verified.
Don't follow steps. Define success and iterate.
Strong success criteria let you loop independently.

## Rule 5 — Use the model only for judgment calls
Use me for: classification, drafting, summarization, extraction.
Do NOT use me for: routing, retries, deterministic transforms.
If code can answer, code answers.

## Rule 6 — Token budgets are not advisory
Per-task: 4,000 tokens. Per-session: 30,000 tokens.
If approaching budget, summarize and start fresh.
Surface the breach. Do not silently overrun.

## Rule 7 — Surface conflicts, don't average them
If two patterns contradict, pick one (more recent / more tested).
Explain why. Flag the other for cleanup.
Don't blend conflicting patterns.

## Rule 8 — Read before you write
Before adding code, read exports, immediate callers, shared utilities.
"Looks orthogonal" is dangerous. If unsure why code is structured a way, ask.

## Rule 9 — Tests verify intent, not just behavior
Tests must encode WHY behavior matters, not just WHAT it does.
A test that can't fail when business logic changes is wrong.

## Rule 10 — Checkpoint after every significant step
Summarize what was done, what's verified, what's left.
Don't continue from a state you can't describe back.
If you lose track, stop and restate.

## Rule 11 — Match the codebase's conventions, even if you disagree
Conformance > taste inside the codebase.
If you genuinely think a convention is harmful, surface it. Don't fork silently.

## Rule 12 — Fail loud
"Completed" is wrong if anything was skipped silently.
"Tests pass" is wrong if any were skipped.
Default to surfacing uncertainty, not hiding it.

## Rule 13 — Explain choices like the user is five
Whenever you present options, a recommendation, or a tradeoff, explain the reasoning so simply that a kindergartner could follow it: plain words, no jargon, name the concrete stakes (what breaks, what the user gains or loses, what it costs). Lead with the recommended option and the one-line "why". Applies to every choice you surface — AskUserQuestion options, prose A/B/C, or a single-line recommendation. Conversation language stays Korean (per Language rules); this rule is about *clarity*, not language.

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).
