#!/bin/bash
# Keeps Python tests from running against a stale livetable extension.
# The test runner only checks importability, so an old installed build passes silently.
#   PostToolUse Edit/Write on impl/src/**/*.rs or impl/Cargo.toml -> mark stale
#   PostToolUse Bash that installs the extension                   -> clear
#   PreToolUse  Bash that runs Python tests while stale            -> deny with rebuild steps
input=$(cat)
event=$(jq -r '.hook_event_name // empty' <<<"$input")
tool=$(jq -r '.tool_name // empty' <<<"$input")
root="${CLAUDE_PROJECT_DIR:-$(pwd)}"
marker="$root/.git/claude-pyext-stale"
shopt -s nocasematch  # sessions start from both ~/projects and ~/Projects

case "$event:$tool" in
  PostToolUse:Edit | PostToolUse:Write | PostToolUse:MultiEdit)
    file=$(jq -r '.tool_input.file_path // empty' <<<"$input")
    case "$file" in
      "$root"/impl/src/*.rs | "$root"/impl/Cargo.toml) touch "$marker" ;;
    esac
    ;;
  PostToolUse:Bash)
    cmd=$(jq -r '.tool_input.command // empty' <<<"$input")
    if grep -qE 'install\.sh|maturin develop|pip3? install[^;&|]*livetable' <<<"$cmd"; then
      rm -f "$marker"
    fi
    ;;
  PreToolUse:Bash)
    [ -f "$marker" ] || exit 0
    cmd=$(jq -r '.tool_input.command // empty' <<<"$input")
    if grep -qE '(^|[^[:alnum:]_-])pytest([^[:alnum:]_-]|$)|run_all\.sh' <<<"$cmd"; then
      reason='Rust sources under impl/src changed since the Python extension was last installed, so these Python tests would run against a stale build. Rebuild and install first: `cd impl && ./install.sh`. If `import livetable` then fails with "mis-aligned LINKEDIT", read the python-extension-load-failure.md project memory and use its exact SDKROOT build command (the full SDK path, not a bare name); touch impl/src/lib.rs first so cargo relinks. If you already rebuilt another way, delete .git/claude-pyext-stale and rerun.'
      jq -n --arg r "$reason" '{hookSpecificOutput: {hookEventName: "PreToolUse", permissionDecision: "deny", permissionDecisionReason: $r}}'
    fi
    ;;
esac
exit 0
