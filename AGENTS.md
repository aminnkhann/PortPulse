# Project coding boundaries

PortPulse is a small learning project. Prefer code a beginner can read and explain over clever or highly general solutions.

## Keep code simple

- Build only what the approved PRD requires. Do not add extra features “for later.”
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
- Keep README, rules, schema, fixture, and expected output consistent when changing the event contract.

## Synchronized GitHub repositories

Keep these two repositories in sync:

- Course repository: https://github.com/dci-fbw-dataeng-python-25-a/amin-final-project
- Personal repository: https://github.com/aminnkhann/portpulse

For implementation changes, apply the same source, tests, migrations, fixtures, runtime configuration, dependency files, and README changes to both repositories in the same work session. Use matching branch names, commit messages, and PR titles; link corresponding PRs and keep them open until both checks pass. Never merge one side while the other side is unresolved.

These repository-specific files do not need to be mirrored: `PRD.md`, `.github/workflows/` (CI), `progress.md`, and `docs/`. Keep them accurate in the repository being changed when useful. If an implementation or dependency change requires a CI adjustment for the checks to run, make the required adjustment in both repositories. After merging, compare implementation files rather than requiring documentation and CI file trees to match.

The repositories have separate Git histories, so their commit hashes and PR numbers will differ. If either repository is inaccessible or its PR cannot be made ready, stop before merging either side and report what remains.
