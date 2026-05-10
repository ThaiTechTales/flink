# Event Time

Most engineers coming from traditional app development, databases, APIs, or batch systems assume time is simple:

- events arrive immediately
- events arrive in order
- events arrive close to real-world time

In distributed streaming systems, that assumption breaks. Events can arrive late, out of order, or with timestamps that differ from processing time.

## Why Time Becomes Hard In Distributed Systems

In local and mostly synchronous systems, time feels straightforward.

![](./99-diagrams/01-concepts/application-01.png)

Streaming systems are different. Events can:

- originate globally
- travel through variable network paths
- queue in Kafka
- retry during failures
- replay after outages
- arrive out of order
- arrive minutes or hours late

Once events stop arriving in neat chronological order, time becomes a core architecture problem.

![](./99-diagrams/01-concepts/application-02.png)
With this example, events no longer arrive in neat chronological order. 

## Time Models In Flink

A common beginner mistake is assuming "time" means one thing. In reality, there are multiple time models in Flink, each with different semantics and use cases:

In Flink, there are multiple time models:

- processing time
- event time
- ingestion time (historically important, less common in modern production setups)

Each time model has different semantics, trade-offs, and use cases. Understanding these differences is critical for designing correct and efficient streaming applications.

## Processing Time

Processing time is the time when Flink processes an event. It is the simplest time model, as it does not require any coordination or tracking of event timestamps. Flink simply uses the system clock to determine the processing time for each event.

### Example

![](./99-diagrams/01-concepts/processing-01.png)

Suppose an event happened at `10:00:00`, but due to delays it reaches Flink at `10:07:00`.

With processing time, Flink treats that event as happening at `10:07:00`, because that is when Flink saw it.

### Why It Exists

Processing time is attractive because it is:

- simple
- fast
- low overhead
- operationally lightweight

It does not require watermark coordination, event-time tracking, or late-data handling. This makes it easier to implement and operate, especially for use cases where timing precision is not critical.

### Why It Can Be Dangerous

Processing time sacrifices correctness when arrival time differs from occurrence time. 

![](./99-diagrams/01-concepts/processing-02.png)

For example, in fraud detection:

- analytics become inaccurate
- windows become inconsistent
- dashboards drift
- fraud patterns may be missed

Processing time reflects when the system observed the event, not when the event actually occurred. This can lead to incorrect results if there are delays, out-of-order events, or if the timing of events is critical to the application logic.

### Where It Is Still Useful

| Use case | Why acceptable |
| --- | --- |
| Internal metrics | Minor timing inaccuracies are acceptable |
| Infrastructure monitoring | Near-real-time operational visibility matters most |
| Temporary dashboards | Simplicity is preferred |
| Lightweight alerting | Low latency is prioritized |
| Non-critical analytics | Operational simplicity is more important than precision |

## Event Time

![](./99-diagrams/01-concepts/event-time-01.png)

Event time is the most important model in modern streaming systems. It is the timestamp inside the event itself, representing when the event actually happened in the real world. Event time allows Flink to process events based on their true occurrence time, rather than when they were observed by the system.

Event time is the timestamp inside the event itself:

- not when Flink saw it
- not when Kafka stored it
- not when processing happened
- but when the event actually happened in the real world

### Why Event Time Matters

Event time preserves correctness, which is essential for:

- financial systems
- IoT analytics
- telemetry
- anomaly detection
- fraud detection
- clickstream analytics
- observability pipelines
- ML feature generation

Without event time, delayed events can corrupt results. For example, if an event arrives late but has an old timestamp, it can cause windows to produce incorrect aggregates or miss important patterns.

### Why Event Time Is Hard

Event time introduces a hard distributed-systems question: `How long should the system wait for delayed events?` This is the core problem of event time processing. It leads directly to the concept of watermarks, which we will cover next.

At scale, this directly affects latency, state size, memory usage, and output correctness. This is the core problem of event time processing and leads directly to the concept of watermarks, which we will cover next.

### Core Event-Time Problem

| Event | Actual event time | Arrival time |
| --- | --- | --- |
| A | 10:00:01 | 10:00:02 |
| B | 10:00:02 | 10:00:03 |
| C | 10:00:03 | 10:07:00 |

Event C happened earlier but arrived much later.

Now Flink must decide:

- Should windows stay open?
- When is a result final?
- How long should state be retained?
- When should late events be dropped?

These questions lead directly to watermarks.

## Watermarks

Watermarks are often misunderstood. They are not a magical guarantee of event-time progress. They are not a strict cutoff. They are not a perfect solution to late data. A watermark is Flink's estimate of event-time progress.

More precisely, it is a signal that Flink believes events earlier than a given timestamp are unlikely to arrive. 

**Important:** this is not a guarantee. It is an approximation.

### Mental Model

**Analogy #1:** Airport boarding

- the gate closes at a stated time
- passengers arriving later are likely excluded

Watermarks are similar. Flink eventually decides: `I believe events earlier than this timestamp are complete.`

**Analogy #2:** Newspaper printing
- the newspaper prints at 6am with news up to 5:30am
- news arriving after 5:30am is not included until the next edition
- the newspaper can still print late-breaking news online, but the 6am edition is final
- the newspaper can adjust the cutoff time for future editions based on how late news arrives
- the newspaper can choose to print a "late-breaking news" section for events that arrive after the cutoff, but this is separate from the main edition

### Why Watermarks Exist

![](./99-diagrams/01-concepts/watermarks-01.png)

Without watermarks, Flink would not know when to:

- close windows
- emit final aggregations
- clean up state
- advance event time

A streaming system could otherwise wait forever. Watermarks provide a practical mechanism to make progress in the face of uncertainty about event arrival times. They allow Flink to balance correctness with latency and resource usage by providing a way to estimate when it can safely emit results based on event time.

### Example Progression

If the current watermark is `10:05:00`, Flink assumes: `I have likely seen all events with timestamps before 10:05:00.`

This allows:

- windows before `10:05:00` to close
- state cleanup
- aggregation emission

### Watermarks Are Probabilistic

Watermarks are not guarantees. Late events can still arrive. 

Flink can adjust watermarks dynamically based on observed event patterns. If events arrive late, Flink can delay future watermarks to accommodate them. However, this is a trade-off between latency and correctness. If you set watermarks too aggressively, you risk dropping late events. If you set them too conservatively, you risk high latency and large state.

That leads to the next concept: **late data.**

## Late Events

Late events arrive after the watermark has already advanced past their event time.

| Event time | Arrival time | Watermark |
| --- | --- | --- |
| 10:00 | 10:07 | 10:05 |

This event is late because the watermark has already passed `10:05`.

### Why Late Events Are Operationally Hard

Late data forces a trade-off.

Option 1: Wait longer

- Pros: more accurate results
- Cons: more retained state, higher memory, delayed output, larger checkpoints

Option 2: Close windows sooner

- Pros: lower latency, smaller state, faster cleanup
- Cons: possible incorrect analytics, dropped late events

This trade-off is central to streaming architecture.

## Windowing

Windows turn infinite streams into finite chunks.

Without windows, a query like this never finishes meaningfully:

```sql
SELECT COUNT(*)
FROM events;
```

### Tumbling Windows

- fixed size
- non-overlapping
- each event belongs to exactly one window

### Sliding Windows

- overlapping windows
- one event can belong to multiple windows
- higher compute cost than tumbling windows

### Session Windows

Session windows group bursts of activity separated by inactivity gaps.

Common use cases:

- clickstream analytics
- gaming sessions
- mobile app behavior analysis

## Why Windows Need Watermarks

Windows need a mechanism to determine when a result can be finalized.

Watermarks provide that mechanism.

They coordinate when to close windows, emit output, and release state.

## Internal Watermark Propagation

In distributed pipelines, watermark behavior is not local to one operator. Watermarks must propagate through the graph, and overall progress is often constrained by slower partitions or inputs.

This is why uneven event arrival across partitions can delay window completion even when most data is on time.
