# CLAUDE.md

Guidance for Claude Code in this repository.

## Push family context

Shared facts for all Push repos (repo map, git identity, `core/` pinning, cross-repo hardware facts):

@~/.claude/push-family.md

## Project

`automation` — LFO-style MIDI CC automation sequencer, a catalog hack for
[ableton-push-hack](https://github.com/federico-pepe/ableton-push-hack).
Draw curves in the web UI; the hack loops them and sends MIDI CC to Live.
Full description, build and deploy steps: [README.md](README.md).

## Rules

- `core` comes from GitHub, pinned in `src/go.mod`. No local `replace`.
- No `/dev/snd` access before `alsaseq.WaitForBootSettle()`.
- The catalog boot service runs `<binary> -config <hack.json>`; keep `-config`.
- Build: `make` (linux/amd64, `CGO_ENABLED=0`). Release: bump `hack.json`
  `version`, push a matching `vX.Y.Z` tag; `.github/workflows/release.yml`
  builds and writes `release.json`.
