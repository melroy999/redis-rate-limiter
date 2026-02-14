# Architecture Documentation

This directory contains a collection of diagrams and reference documents that provide visual overviews of the system's architecture. All diagrams have been created using [Mermaid](https://mermaid.js.org/), such that they can be rendered natively on GitHub and produce meaningful diffs during code review.

## Diagrams

| Document | Description |
|----------|-------------|
| [Component Diagram](components.md) | High-level overview of the major runtime components and their interactions. Intended as the first point of reference when reading the codebase. |
| [Task Lifecycle Sequence](task-lifecycle.md) | Traces the flow of a single task from scheduling through execution to completion, divided into four phases. |
| [Class Hierarchy](class-hierarchy.md) | Inheritance and composition relationships between the classes that constitute the rate limiter, including the extension points for new backends. |
| [Task State Diagram](task-states.md) | All possible states a task can occupy, the transitions between them, and the crash recovery mechanisms. |
| [Redis Key Map](redis-keys.md) | Comprehensive reference of every Redis key used by the system, including data types, creators, readers, and TTL strategies. |
| [Sliding Window Algorithm](sliding-window.md) | Visual explanation of the sliding window counter algorithm, the weight formula, and the mathematical 2x burst bound guarantee. |
| [Drain Loop Flow](drain-flow.md) | Flowchart of the three-layer drain control loop, including wake coalescing, error recovery, and the five feedback entry points. |

## Related Documentation

- [Smart Jitter](../smart-jitter.md): adaptive thundering herd prevention strategy for retry delays.
