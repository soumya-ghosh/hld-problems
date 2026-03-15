# Known Unknowns vs. Unknown Unknowns in Observability

The distinction between "Known Unknowns" and "Unknown Unknowns" is the core philosophical difference between traditional **Monitoring** and modern **Observability**.

## 1. Known Unknowns (Monitoring)

**Definition:**
These are failure modes or system states that you are **aware could happen**, even if you don't know *when* they will happen or *what* the current value is.

Because you can anticipate these problems, you can write specific checks or dashboards for them.

*   **The Question:** "Is the system within the expected boundaries?"
*   **The Tool:** Static Dashboards and Threshold Alerts.
*   **The Mindset:** Defensive. You are checking for things you have seen fail before.

### Examples:
*   **Disk Space:** You know disks can fill up. You don't know *if* it is full right now. So, you set an alert: `Notify if Disk Usage > 90%`.
*   **CPU Load:** You know high traffic causes high CPU. You monitor the CPU metric.
*   **Error Rate:** You know HTTP 500s are bad. You have a dashboard showing the count of 500s.

**Limitation:** You can only monitor what you can predict. If a failure mode occurs that you didn't write a specific dashboard for, you are blind.

---

## 2. Unknown Unknowns (Observability)

**Definition:**
These are failure modes or system behaviors that you **never imagined could happen**. You cannot predict them, so you cannot pre-configure a dashboard or alert for them.

To solve these, you need a system that allows you to ask **new, arbitrary questions** about your data without shipping new code or parsing new logs.

*   **The Question:** "Why is *this specific subset* of requests failing?"
*   **The Tool:** High-cardinality exploration, Ad-hoc querying, Distributed Tracing.
*   **The Mindset:** Investigative. You are debugging a novel scenario.

### Examples:
*   **The "Emoji" Bug:** Your system crashes, but CPU and Disk are fine. You dig into the data and discover that latency spikes *only* happen when a user has a specific emoji in their username AND they are using the Android app v4.2 AND they are in the `us-west-1` region.
    *   *You could never have predicted to create a dashboard for "Latency by Emoji by App Version by Region".*
*   **The "noisy neighbor":** A specific customer starts sending a new type of malformed request that doesn't trigger a 500 error but causes a memory leak in one specific microservice instance.

---

## Summary Comparison

| Feature | Known Unknowns (Monitoring) | Unknown Unknowns (Observability) |
| :--- | :--- | :--- |
| **Focus** | System Health | System Behavior |
| **Primary Artifact** | Dashboards (Pre-computed) | Ad-hoc Queries (Exploratory) |
| **Data Structure** | Aggregated Metrics (Low Cardinality) | Granular Events (High Cardinality) |
| **Question Type** | "Is the DB slow?" | "Which users are experiencing slow DB queries and why?" |
| **Goal** | Detect the problem. | Explain the problem. |

> **"Monitoring tells you that you are down. Observability tells you why."**
