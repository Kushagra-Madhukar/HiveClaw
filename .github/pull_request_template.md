## Summary

- What changed:
- Why it changed:
- What remains intentionally out of scope:

## Placement

- Why is this the right crate/module/layer for the change?
- If not reused from an existing subsystem, why not?

## Boundary Classification

- Classification:
  - [ ] `native_required`
  - [ ] `mcp_preferred`
  - [ ] `review_required`
- If this is a new integration, what MCP-first check was performed?

## Security / Trust Boundary Impact

- Does this touch any of the following?
  - [ ] approvals
  - [ ] credentials / vault
  - [ ] policy / capability enforcement
  - [ ] browser / crawl / web
  - [ ] shell / filesystem
  - [ ] external bridge / subprocess trust boundary
- If yes, what code-enforced safeguards were added or preserved?

## Resource Impact

- Runtime cost:
- Memory impact:
- Disk/artifact impact:
- Binary/dependency impact:
- Low-end-device impact:

## Reuse / Build-vs-Buy

- Existing subsystem reused:
- External crate adopted or evaluated:
- If custom code was added instead of using a crate or MCP server, why?

## Documentation

- Docs updated:
  - [ ] `docs/ENGINEERING_PR_REVIEW_CHECKLIST.md` reviewed
  - [ ] architecture/runtime docs updated where needed
  - [ ] MCP docs updated where needed
  - [ ] security/audit docs updated where needed
  - [ ] no doc update needed, and why is documented below
- Documentation note:

## Validation

- [ ] `cargo check --workspace`
- [ ] `cargo test --workspace --quiet`
- Additional verification:

## Reviewer Checklist

Reviewer: use [docs/ENGINEERING_PR_REVIEW_CHECKLIST.md](/Users/kushagramadhukar/coding/anima/docs/ENGINEERING_PR_REVIEW_CHECKLIST.md).

- [ ] Right layer / right module
- [ ] Reuse preferred over duplication
- [ ] Trust boundaries preserved in code
- [ ] Resource cost is bounded and acceptable
- [ ] Tests are sufficient
- [ ] Operator / inspect impact covered where needed
