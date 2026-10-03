#!/usr/bin/env bash
# verify.sh — the one command that backs every claim in FEATURES.md.
#
#   ./scripts/verify.sh          # fast gate: fmt-check, vet, build, test
#   ./scripts/verify.sh --race   # adds -race on the concurrency-heavy packages
#   ./scripts/verify.sh --full   # race everywhere (slow; CI / pre-release)
#   ./scripts/verify.sh --soak   # fast gate + the chaos/soak harness
#                                # (duration: MICRODB_SOAK=5m, default 60s)
#
# A non-zero exit means the tree is not shippable.
set -uo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-fast}"
FAIL=0
step() { printf '\n== %s\n' "$1"; }

step "gofmt (content check, CRLF-normalised)"
BAD=""
for f in $(find . -name '*.go' -not -path './.git/*'); do
  sed 's/\r$//' "$f" > /tmp/vf_in.go 2>/dev/null || continue
  gofmt /tmp/vf_in.go > /tmp/vf_out.go 2>/dev/null || continue
  cmp -s /tmp/vf_in.go /tmp/vf_out.go || BAD="$BAD $f"
done
if [ -n "$BAD" ]; then echo "not gofmt-clean:$BAD"; FAIL=1; else echo "clean"; fi

step "go vet ./..."
go vet ./... || FAIL=1

step "build (all binaries)"
go build ./... || FAIL=1

step "go test ./... -count=1"
go test ./... -count=1 -timeout 20m || FAIL=1

if [ "$MODE" = "race" ] || [ "$MODE" = "full" ]; then
  PKGS="./internal/store ./internal/cluster ./internal/api ./internal/changelog ./internal/index ./internal/bloom ./internal/ranges"
  [ "$MODE" = "full" ] && PKGS="./..."
  step "-race $PKGS"
  CC="${MICRODB_GCC:-C:\w\msys64\mingw64\bin\gcc.exe}" CGO_ENABLED=1 go test -race $PKGS -count=1 -timeout 25m || FAIL=1
fi

if [ "$MODE" = "soak" ]; then
  # The chaos/soak harness (build tag `soak`): a seeded random workload
  # over a cluster while nodes are killed and restarted, asserting that
  # every quorum-acked write survives and membership converges. It is
  # time-boxed, not test-count-boxed, so the knob is a duration.
  step "soak / chaos harness (MICRODB_SOAK=${MICRODB_SOAK:-60s})"
  go test ./integration/ -tags soak -run TestSoak -count=1 -timeout 30m -v || FAIL=1
fi

step "result"
if [ "$FAIL" = "0" ]; then echo "PASS — tree is shippable"; else echo "FAIL — do not ship"; fi
exit $FAIL
