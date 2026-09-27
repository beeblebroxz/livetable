#!/bin/bash
# Keeps Python tests from running against a stale livetable extension.
# The test runner only checks importability, so an old installed build passes silently.
# PreToolUse Bash: before pytest or run_all.sh, deny if any impl/src/**/*.rs or
# impl/Cargo.toml is newer than the installed extension. Comparing mtimes catches every
# way a source can change (Edit, sed, git pull/checkout/stash) without tracking state.
input=$(cat)
cmd=$(jq -r '.tool_input.command // empty' <<<"$input")
# Match pytest/run_all.sh only where a command starts (after ^ ; & | or '('), optionally
# behind VAR=value, sh/bash, python -m, or a path, so `grep pytest ...` does not trigger.
seg='(^|[;&|(])[[:space:]]*([A-Za-z_][A-Za-z0-9_]*=[^[:space:]]*[[:space:]]+)*'
path='([^[:space:];&|()]*/)?'
runner="(${path}((ba)?sh|python3?[[:space:]]+-m)[[:space:]]+)?"
grep -qE "${seg}${runner}${path}(pytest|run_all\.sh)([[:space:];&|)]|\$)" <<<"$cmd" || exit 0

root="${CLAUDE_PROJECT_DIR:-$(pwd)}"
py=python3
[[ "$cmd" == *.venv/* && -x "$root/.venv/bin/python3" ]] && py="$root/.venv/bin/python3"

# find_spec locates the package without importing it, so a broken build can't crash this.
so=$("$py" - 2>/dev/null <<'EOF'
import glob, importlib.util, os
spec = importlib.util.find_spec("livetable")
if spec and spec.origin and spec.origin.endswith(".so"):
    print(spec.origin)
elif spec:
    for d in spec.submodule_search_locations or []:
        found = glob.glob(os.path.join(d, "*.so"))
        if found:
            print(found[0])
            break
EOF
)
[ -n "$so" ] || exit 0  # not installed: the test runner's import check reports that

newer=$(find "$root/impl/src" "$root/impl/Cargo.toml" \( -name '*.rs' -o -name Cargo.toml \) -newer "$so" 2>/dev/null | head -1)
[ -n "$newer" ] || exit 0

reason="${newer#"$root"/} changed after the installed Python extension ($so) was built, so these tests would run against a stale build. Rebuild and install first: \`cd impl && ./install.sh\`. If \`import livetable\` then fails with \"mis-aligned LINKEDIT\", read the python-extension-load-failure.md project memory and use its exact SDKROOT build command (the full SDK path, not a bare name); touch impl/src/lib.rs first so cargo relinks."
jq -n --arg r "$reason" '{hookSpecificOutput: {hookEventName: "PreToolUse", permissionDecision: "deny", permissionDecisionReason: $r}}'
