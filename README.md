# deepstream-io-akka

Keeps a shared piece of data in step across everyone editing it at once, decides whose
change wins when two arrive together, and tells every watcher what changed and who is here.

A port of [deepstreamIO/deepstream.io](https://github.com/deepstreamIO/deepstream.io) onto
**Akka**, built with **Akka Specify**.

---

## Where it came from

`deepstreamIO/deepstream.io` is a server for applications where several people work on the
same data at the same time. Each shared piece of data carries a number that goes up by one
every time it changes; a client sends the number it thinks it is producing, and the server
uses that number to decide whether the change is accepted or has been overtaken by someone
else's.

Only that decision, and the two things wrapped around it, were rebuilt here: what a change
does to the data, and who gets told about it — including who is currently connected. The
original's other traffic — one-off messages, remote calls, and clients that offer to
provide data on demand — was left alone, along with its permission rules, its clustering
and its own wire format.

The written specification this was built from lives in a separate repository,
`akka-specify-harness`, under `deepstream-io-port/`. It is private for now.

---

## deepstreamIO/deepstream.io → this port

📉 1,286 TypeScript lines → **753 Java lines**<br>
📁 7 files → **21 files**<br>
⚡ 0.30 → **2.80** milliseconds, one accepted change over HTTP<br>
🎯 18 situations compared → **17 of 18 agree, 1 differs by decision**<br>
🔁 0 changes replayed to a watcher that reconnects → **every change it missed, in order**<br>
🧪 0 rules broken on purpose to check a test notices → **10**

The two timings are each system as it is normally set up, and they are not doing the same
amount of work: the original's default keeps the data in memory only, and this port writes
every accepted change down before it answers. How each number was measured, and the ones
that did not make this list, are written up next to the specification in
`akka-specify-harness` under `deepstream-io-port/bench/REPORT.md`.

---

## What it took to build

⏱️ **1.1 hours** from the first command to the published repository, **1.1** of them active<br>
💬 **318** exchanges with the model<br>
✍️ **294,218** tokens written by the model, **66,663,761** counting everything sent and re-sent<br>
🙋 **0** questions to a human<br>
🧪 **29** tests

```bash
python toolkit/tokens.py --port deepstream-io
```

The record of every question, and where the time went, lives with the specification.

---

## What it does

- **A change carries the number it expects to produce, and only the next number is
  accepted.** Two people who both read version 4 and both send version 5 do not both
  succeed; one is told it was overtaken.
- **Being overtaken and asking for an impossible number are two different answers.** The
  first hands back the version that won and the data that came with it, so the loser can
  redo its change on top and send again without asking for anything else; the second hands
  back nothing, because there is nothing to redo it on top of.
- **A change that is refused leaves the data exactly as it was.** Nothing is half applied,
  including a batch of several edits sent together — if any one of them is not allowed,
  none of them happens.
- **Every accepted change is sent to everyone watching, in order, and nothing else is.** A
  refused change reaches nobody.
- **A watcher whose connection drops and comes back is sent everything it missed, in
  order, before anything new.** It says which change it last saw, and it is caught up from
  there.
- **Someone is here if they have at least one connection open.** Opening a second window
  announces nothing, closing one of two announces nothing, and only the last one closing
  says they have gone.

---

## Design decisions

**One owner per piece of data.** Everything about one shared item happens in one place, so
two changes to it can never be dealt with at the same moment and there is nothing to queue
or unpick. Deciding whose change wins becomes comparing two numbers, with no chance of both
being told yes.

**Written down before it is answered.** An accepted change is recorded on disk before the
sender is told it worked, rather than kept in memory and written later. If the machine
stops, nobody was told something happened that then did not.

**A numbered list of changes, not just the latest one.** Each change gets its own place in
a numbered list rather than overwriting a single "current" entry. A watcher that fell
behind can name where it got to and be handed the rest, instead of being given today's
answer and left to guess what it missed.

**A refused edit works on a copy.** An edit is worked out against a copy of the data and
only swapped in once it has finished, rather than being applied piece by piece to the real
thing. A batch that turns out to be impossible halfway through cannot leave half of itself
behind.

**A limit on how big one item may get.** A change that would push an item past half a
megabyte is refused and says so. Past a point, the system stops being able to copy an item
between places without telling anyone, and a refusal a sender can read is better than a
silence it cannot.

---

## Running it — the short path

You do not need Java, Maven, or the Akka CLI installed. Akka Specify installs them for you.

**1. Install Akka Specify** in Claude Code:

```
/plugin marketplace add akka/ai-marketplace
/plugin install akka@akka-ai-marketplace
```

Restart Claude Code when it asks.

**2. Give it this prompt:**

> Clone https://github.com/TylerJewell/deepstream-io-akka into a new directory and open it.
> Then run /akka:setup to install everything this project needs, and /akka:build to
> compile it, run the tests, and start it locally.

**3.** The service answers on **port 9013**. There is no page to open — it is a service
other programs talk to.

---

## Running it — the developer path

### Requirements

- Java 21 or newer
- Maven 3.9 or newer
- An Akka download token — run `akka code token` once

### Start the service

```bash
mvn compile
akka local run
```

### Try it

Create something, change it, and lose a race:

```bash
curl -X POST localhost:9013/record/chat-room \
  -H 'Content-Type: application/json' \
  -d '{"action":"update","version":-1,"upsert":true,"data":{"topic":"hello"}}'

curl -X POST localhost:9013/record/chat-room \
  -H 'Content-Type: application/json' \
  -d '{"action":"update","version":2,"data":{"topic":"second"}}'

curl -X POST localhost:9013/record/chat-room \
  -H 'Content-Type: application/json' \
  -d '{"action":"update","version":2,"data":{"topic":"too late"}}'
```

The third answers `409` with the version that won and the data that came with it.

Watch it change, and catch up after a break:

```bash
curl -N localhost:9013/record/chat-room/subscribe
curl -N localhost:9013/record/chat-room/subscribe?since=2
```

Say who is here:

```bash
curl -X POST localhost:9013/presence/alice/connect
curl localhost:9013/presence
curl -N localhost:9013/presence/subscribe
```

---

## What it answers

| Request | What it does |
|---|---|
| `POST /record/{name}` | Change the item. Answers `200` accepted, `409` overtaken, `400` impossible number or bad path, `404` no such item, `413` too big |
| `GET /record/{name}` | The item's current number and contents |
| `GET /record/{name}/version` | The item's current number, or `-1` if there is no such item |
| `DELETE /record/{name}` | Remove the item |
| `GET /record/{name}/subscribe` | A live feed of changes; `?since=` or the browser's own reconnect header says where to carry on from |
| `POST /presence/{user}/connect` | Open a connection for this person |
| `POST /presence/{user}/disconnect` | Close one |
| `GET /presence` | Everyone who is here |
| `POST /presence/query` | Yes or no for each of a list of names |
| `GET /presence/subscribe` | A live feed of people arriving and leaving |
| `GET /presence/{user}/subscribe` | The same feed, for one person |

---

## Configuration

| Variable | Default | Notes |
|---|---|---|
| none | | The port is set in `src/main/resources/application.conf`; nothing else is configurable |

---

## Where it differs from deepstreamIO/deepstream.io

Everything not listed here behaves the same way on purpose, including the parts that look
like mistakes.

- **What a watcher gets after its connection drops.** deepstream.io sends it nothing:
  nothing is replayed, reconnecting brings nothing back, and re-reading gives today's
  answer with no sign that anything was missed. This port gives every change a place in a
  numbered list and sends a returning watcher everything after the place it names, in
  order, before anything new — because an item whose whole point is that its changes are
  ordered cannot keep that promise across the one moment the original leaves undefined.
- **Whether a change that works says so.** deepstream.io says nothing when a change is
  accepted, on purpose and for speed, unless the sender asked for confirmation up front.
  This port answers every change with the number it became, because a request over this
  kind of connection has an answer whether or not anything is put in it, and a sender told
  its own number does not need to ask again.
- **Where an accepted change is kept.** deepstream.io writes to two places — a fast one
  and a lasting one — waits for neither unless confirmation was asked for, and skips the
  lasting one entirely for names matching a configured prefix. This port writes once, to a
  numbered record of changes, and does not answer until that has finished. There is no way
  here to trade permanence for speed on particular names, which the original allows.
- **A path with something other than a whole number in square brackets.** deepstream.io
  accepts `a.b[x]` and puts the value under a key that is not a number and not the one that
  was written. This port refuses the path, because a path the sender cannot have meant is
  better turned down than quietly redirected somewhere nobody named.
- **Whether you are here as far as you are concerned.** deepstream.io has two ways to ask
  who is around and they disagree: asking for everyone leaves you out of the answer, asking
  about a list of names does not. This port includes you in both, and a caller that wants
  "everyone but me" removes itself — one question should not have two meanings depending on
  how it is phrased.
- **An upper limit on how big one item may be.** deepstream.io has none; whatever is sent
  is stored. This port refuses a change that would take an item past half a megabyte and
  says by how much, because past a point the item can no longer be copied between places
  and nothing would say so.
- **How the original's messages travel.** deepstream.io speaks its own format over a
  long-lived two-way connection, with a second way in over ordinary web requests. This port
  offers only ordinary web requests, plus a one-way live feed for watching. A client
  written for the original will not talk to this without changes.
- **Everything about running on more than one machine.** `not checked`. The original
  shares who-is-here between machines through its own agreement mechanism, and routes
  changes between them. This port was only ever run as a single instance, so whether the
  two agree under that condition is unknown.
- **Clients that offer to provide data on demand.** `not checked`. The original lets a
  client register interest in names matching a pattern and be woken when somebody asks for
  one. Nothing here does that, and no attempt was made to find out what the original does
  when the two features meet.

---

## Licence

`deepstreamIO/deepstream.io` is MIT, © 2019 deepstreamHub GmbH. This port copies no source
from it and reimplements the behaviour from a written specification; see
[`ACKNOWLEDGEMENTS.md`](ACKNOWLEDGEMENTS.md).
