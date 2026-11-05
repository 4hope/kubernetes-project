# Key results

Regenerated from the fixed cohorts in `experiments/run-manifest.json`.

## Original comparison

10 requests/s, 80 s, 800 attempts/run, 3 repeats/condition.

| Condition | Failed / sent | Completion mean (range), s | Mean run p95, ms | Observed Ready range |
|---|---:|---:|---:|---:|
| baseline | 0 / 2400 | n/a | 31.99 | 2-2 |
| delete | 0 / 2400 | 12.14 (11.44-12.49) | 30.88 | 1-2 |
| rollout-a | 0 / 2400 | 25.46 (25.44-25.50) | 29.43 | 2-3 |
| rollout-b | 0 / 2400 | 24.80 (24.45-25.43) | 28.68 | 1-2 |

## Pressure comparison

250 requests/s, 60 s, 15,000 attempts/run, 3 repeats/strategy.

| Strategy | Failed / sent | Failure rate | Mean run p95, ms | Observed Ready range |
|---|---:|---:|---:|---:|
| rollout-a | 370 / 45000 | 0.82% | 1.90 | 2-3 |
| rollout-b | 2595 / 45000 | 5.77% | 21.23 | 0-2 |

### Failures before the update

Client scheduled time below 14 s. Update begins near 15 s.

| Run | Failed / attempted before 14 s |
|---|---:|
| rate-250_repeat-1_n-2_rollout-a | 0 / 3500 |
| rate-250_repeat-1_n-2_rollout-b | 0 / 3500 |
| rate-250_repeat-2_n-2_rollout-a | 135 / 3500 |
| rate-250_repeat-2_n-2_rollout-b | 15 / 3500 |
| rate-250_repeat-3_n-2_rollout-a | 0 / 3500 |
| rate-250_repeat-3_n-2_rollout-b | 0 / 3500 |

## Interpretation

- p95 values describe successful requests only; grouped p95 is the mean of run-level p95 values.
- Original wall-clock action_window_45s statistics are excluded because of clock shifts.
- Pressure failure totals cover entire runs, including pre-update errors. None were subtracted.
- Setup failure, pilots and follow-up demos remain separate from the original comparison.
- A had fewer recorded pressure failures, but the pre-update errors prevent attributing the entire difference to strategy alone.
- Original deletion mean is time until a replacement is Ready, not client downtime.
- B's original timing advantage is smaller than the roughly 1 s observation interval and does not establish a reliable speed advantage.
- Capacity checks use 20 s / 5,000 attempts per run. One-copy failure rates were 2.72%, 2.48%, 1.58%; two-copy checks had no failures.

## Separate records

Three later deletion demos and one loadgen preparation timeout are retained separately. Pilot runs remain under experiments/pressure/results, with their diagnostics in experiments/pressure/analysis. They are not discarded or pooled into the original comparison.
