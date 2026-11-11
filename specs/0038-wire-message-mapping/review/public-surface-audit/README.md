# Public declaration compatibility audit — 2026-11-09

A local Roslyn syntax audit compares all 98 changed tracked production C# files
against repository HEAD `64ba97097199c708974195f55fbdca0ce3e8487c`. No existing
public/protected declaration in the covered set is removed or changed; 68
declarations are additive. All recorded current-source hashes still match.

The scanner covers visible enclosing types, constructors, methods, properties,
indexers, fields, events, operators and delegates, including signature modifiers
(except async/partial implementation modifiers), parameter defaults and generic
constraints. It retains the public constructors rather than treating new optional
parameters as a binary-compatible replacement. The earlier transport-only scan
covered 97 files and had the same zero-removal result.

This is source-declaration evidence, not a compiled API compatibility report.
Attributes, conditional compilation, implicit/record-generated members, enum values,
new overload ambiguity and new untracked types need the existing compilation and
behavioral evidence. It does not prove wire encoding or attribute-driven behavior.
The isolated input snapshots and command outputs are under
`/private/tmp/brighter-4170-public-surface-audit/`; the reproducible scanner, baseline,
current source hashes and compact results are retained here. No production source
is modified by this audit. Existing endpoint/default-mapping regressions remain
the behavioral compatibility evidence; full-target solution builds remain separate.
