# Acknowledgements

This project is a port of **[deepstreamIO/deepstream.io](https://github.com/deepstreamIO/deepstream.io)**.

## Licence of the original

**MIT**, © 2019 deepstreamHub GmbH. Read from the `LICENSE` file at the root of the
repository at commit `c8f257f`, not from a badge.

## What was copied

**No source was copied.** No file, function, class or fragment of `deepstream.io` appears
in this project. Everything here is written against a behavioural specification —
`deepstream-io-port/specs/SPEC-001-deepstream-io.md` in the harness repository — and the
Java in `src/main` shares no text with the TypeScript it was derived from.

Two things did cross over, and neither is source:

- **The behaviour itself.** The version-conflict rules, the two distinct rejections, the
  presence connection count, the path syntax and what a patch does to a record are all
  derived from `deepstream.io`, and reproduce it deliberately. That is what a port is,
  and it is not something to be coy about.
- **Scenario inputs.** `deepstream-io-port/bench/scenarios.json` in the harness
  repository holds writes that were fed through both systems to compare their answers.
  They were written for that comparison; none is taken from the original's own tests.

The probes and benchmark runners in the harness repository *import* `deepstream.io` and
run it unmodified. They live there, not here, and this project does not depend on it at
build time or at run time.

## What that means for this project's licence

MIT is a permissive licence and imposes no share-alike obligation, so nothing about the
original constrains what this project may be licensed as. Its attribution requirement
applies to copies and substantial portions of the software, and none is included here;
the attribution above is given because it is owed to the work this was derived from, not
because a copied file forces it.

## Also used

- **[Akka](https://akka.io)** — the SDK and runtime this port is built on
  (`akka-javasdk` 3.6.3, Business Source License 1.1).
