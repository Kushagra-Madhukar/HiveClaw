#!/usr/bin/env bash
set -euo pipefail

BASE_SHA="${BASE_SHA:-}"
HEAD_SHA="${HEAD_SHA:-}"

if [[ -z "$BASE_SHA" || -z "$HEAD_SHA" ]]; then
  if git rev-parse --verify HEAD~1 >/dev/null 2>&1; then
    BASE_SHA="$(git rev-parse HEAD~1)"
    HEAD_SHA="$(git rev-parse HEAD)"
  else
    echo "docs-gate: unable to determine diff range"
    exit 1
  fi
fi

mapfile -t changed_files < <(git diff --name-only "$BASE_SHA" "$HEAD_SHA")

if [[ "${#changed_files[@]}" -eq 0 ]]; then
  echo "docs-gate: no changed files"
  exit 0
fi

has_changed() {
  local path="$1"
  for file in "${changed_files[@]}"; do
    if [[ "$file" == "$path" ]]; then
      return 0
    fi
  done
  return 1
}

matches_any() {
  local file
  for file in "${changed_files[@]}"; do
    case "$file" in
      $1) return 0 ;;
    esac
  done
  return 1
}

docs_touched=0
for file in "${changed_files[@]}"; do
  if [[ "$file" == docs/* ]] || [[ "$file" == "REVIEWER_CHECKLIST.md" ]] || [[ "$file" == ".github/pull_request_template.md" ]]; then
    docs_touched=1
    break
  fi
done

architecture_trigger=0
mcp_trigger=0
security_trigger=0
web_trigger=0

for file in "${changed_files[@]}"; do
  case "$file" in
    aria-core/*|aria-intelligence/*|aria-x/src/*|aria-policy/*|aria-learning/*)
      architecture_trigger=1
      ;;
  esac
  case "$file" in
    aria-mcp/*|docs/MCP_BOUNDARY_RULES.md|docs/MCP_BUILD_VS_BUY_MATRIX.md)
      mcp_trigger=1
      ;;
    aria-x/src/*)
      if [[ "$file" == *mcp* ]]; then
        mcp_trigger=1
      fi
      ;;
  esac
  case "$file" in
    aria-policy/*|aria-vault/*|aria-safety/*|aria-x/src/browser.rs|aria-x/src/web.rs|aria-x/src/crawl.rs|aria-x/src/approvals.rs)
      security_trigger=1
      ;;
  esac
  case "$file" in
    aria-x/src/browser.rs|aria-x/src/web.rs|aria-x/src/crawl.rs|aria-x/src/tools.rs)
      web_trigger=1
      ;;
  esac
done

required_docs=()

if [[ "$architecture_trigger" -eq 1 ]]; then
  required_docs+=(
    "docs/RUST_SYSTEMS_REVIEW.md"
    "docs/RUST_SYSTEMS_REVIEW_TODO.md"
    "docs/ENGINEERING_PR_REVIEW_CHECKLIST.md"
  )
fi

if [[ "$mcp_trigger" -eq 1 ]]; then
  required_docs+=(
    "docs/MCP_BOUNDARY_RULES.md"
    "docs/MCP_BUILD_VS_BUY_MATRIX.md"
    "docs/ENGINEERING_PR_REVIEW_CHECKLIST.md"
  )
fi

if [[ "$security_trigger" -eq 1 ]]; then
  required_docs+=(
    "docs/APP_AUDIT_RECOMMENDATIONS.md"
    "docs/ENGINEERING_PR_REVIEW_CHECKLIST.md"
  )
fi

if [[ "$web_trigger" -eq 1 ]]; then
  required_docs+=(
    "docs/WEB_ACCESS_PLATFORM_PLAN.md"
    "docs/WEB_ACCESS_PLATFORM_TODO.md"
    "docs/APP_AUDIT_RECOMMENDATIONS.md"
    "docs/ENGINEERING_PR_REVIEW_CHECKLIST.md"
  )
fi

if [[ "${#required_docs[@]}" -eq 0 ]]; then
  echo "docs-gate: no guarded change classes detected"
  exit 0
fi

declare -A uniq_docs=()
for doc in "${required_docs[@]}"; do
  uniq_docs["$doc"]=1
done

matched_doc=0
for doc in "${!uniq_docs[@]}"; do
  if has_changed "$doc"; then
    matched_doc=1
    break
  fi
done

if [[ "$matched_doc" -eq 1 ]]; then
  echo "docs-gate: required documentation touched"
  exit 0
fi

echo "docs-gate: guarded code paths changed without corresponding documentation updates"
echo "Changed files:"
printf ' - %s\n' "${changed_files[@]}"
echo "At least one of these docs should be updated:"
for doc in "${!uniq_docs[@]}"; do
  printf ' - %s\n' "$doc"
done
exit 1
