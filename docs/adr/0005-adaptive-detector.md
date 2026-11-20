# 0005. Per-cell adaptive baselines and a Bernoulli CUSUM instead of a trained model

- Status: Accepted
- Date: 2026-11-21

## Context

A NOC needs alarms that are explainable, typed (what is wrong) and fast, for thousands of cells
whose "normal" differs by cell and by hour of the day. Static thresholds are what many estates
still use, and they alarm on every busy cell at peak time.

## Options

| Option | For | Against |
|---|---|---|
| Static thresholds | Simple, explainable | Same threshold for every cell and hour; noisy at peak, blind at night |
| **Per-cell EWMA baselines + rules + CUSUM** | Explainable evidence per alert; a few floats of state per cell; learns each cell's normal online; typed alerts | Hand-chosen thresholds; needs a warm-up |
| Isolation forest / autoencoder per cell | Catches unknown patterns | Needs training data and a model lifecycle; scores are hard to explain to field teams; untyped |
| Seasonal forecasting (Prophet, ARIMA) | Models daily cycles explicitly | Heavy per cell; needs weeks of history |

## Decision

The detector (`streaming/detector.py`) combines:

1. **Hard rules**: availability below 50% means an outage. A cell that reports itself available while
   more than 50% of at least 20 RRC attempts fail is a sleeping cell. Both fire from the first window.
2. **EWMA baselines** (alpha 0.05, about 20 minutes of memory) of mean and variance per KPI per cell.
   Congestion = PRB at or above 95% **and** latency z of 4 or more. Interference = SINR z of -4 or
   less. Both need two consecutive windows.
3. A **Bernoulli CUSUM** on RRC setup failures (log-likelihood ratio of p1 = max(4 p0, p0 + 1.5%)
   against the cell's learned p0) for slow degradations.

Baselines freeze while a window looks anomalous or an alert is open. An alert closes after three
healthy windows. State is serialised as JSON in Spark's `applyInPandasWithState`.

## How it was tuned, and how it is judged

- Thresholds were tuned **offline** on the development seed (7) with `streaming/offline.py`, which
  runs the same detector code with pandas. A Gaussian z-score on the failure rate gave 88 false
  alerts there: failures are small counts (about 0.5 per minute), so a normal approximation is wrong.
  Switching to the count model cut that to 15 at the same recall. The CUSUM threshold sweep
  (8 to 16 nats) is in `docs/results/offline_dev_seed7.json`; 14 was chosen (1 false alert, 8.5-minute
  median delay for degradations).
- The live pipeline is scored on a **different seed (2026)** that was not used for tuning.

## Consequences

- Every alert carries its evidence (KPIs, z-scores, CUSUM) as JSON, so a field engineer can see why.
- Unknown failure patterns that do not move PRB, latency, SINR, availability or RRC success are not
  caught. That is the place for an ML model, alongside the rules rather than instead of them
  (see the MLOps lab).
- The synthetic network makes the problem easier than reality: incidents are clean and labelled,
  and there are no correlated multi-cell failures (a site power cut, a transport link) other than
  flash crowds.
