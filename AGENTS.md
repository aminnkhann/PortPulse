# Project coding boundaries

PortPulse is a small learning project. Prefer code a beginner can read and explain over clever or highly general solutions.

## Keep code simple

- Build only what the approved product requirements call for. Do not add extra features “for later.”
- Prefer the smallest direct implementation that solves the task.
- Use the Python standard library first. Add a dependency only when the task needs it and the standard library is not a good fit.
- Keep functions short and focused. Use plain data structures and clear names.
- Avoid unnecessary classes, layers, wrappers, factories, plugins, configuration options, and generic frameworks.
- Follow the existing project structure and conventions. Do not reorganize files without a concrete need.
- Do not duplicate logic; share it only when a real caller needs the same behavior.

## Keep data handling clear

- Use deterministic inputs and outputs. Do not use randomness or the current clock in business rules unless the task explicitly calls for it.
- Keep timestamps in UTC and make timestamp meanings explicit.
- Validate external or fixture data at the point it enters the program. Give a clear reason when data is rejected; do not silently ignore errors.
- Keep invalid, duplicate, and conflicting events from changing trusted state.

## Make small, reviewable changes

- Read the caller and nearby code before changing behavior; update affected callers and documentation.
- Add focused tests for new behavior when a test setup exists. Do not create a test framework for a tiny change.
- Run only the checks needed for the change, and state what was and was not checked.
- Comment only when the reason for a choice is not clear from the code.
- Keep README, rules, schema, fixtures, and expected output consistent when changing the event contract.

## Repository maintenance

- Keep <https://github.com/dci-fbw-dataeng-python-25-a/amin-final-project> and <https://github.com/aminnkhann/portpulse> as mirrors of each other. Synchronize every tracked file, including source, tests, migrations, fixtures, runtime configuration, dependency files, README, `AGENTS.md`, all documentation and screenshots, and `.github/` workflows. Documentation-only and CI-only changes must also be mirrored; there are no repository-specific exceptions unless the user explicitly authorizes a particular difference. Keep public documentation accurate and do not rely on archived planning documents.
- For every change, use matching branch names, commit messages, and PR titles in both repositories, link the corresponding PRs, and keep both open until checks pass. Never merge one side while the other is unresolved. If either repository is inaccessible or its PR cannot be made ready, stop before merging either side and report the outstanding synchronization work.
- The repositories have separate Git histories, so commit hashes and PR numbers differ. Before merging, compare the complete tracked file trees of the paired branches, including documentation, images, `AGENTS.md`, and CI files. After merging, compare both default-branch trees again. Match file paths, contents, and modes; do not copy Git history, ignored local files, credentials, or generated runtime data.
