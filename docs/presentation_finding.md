# Candidate finding for the DCI presentation

**Status: AWAITING VERIFICATION against the owner's local database.**

Use the synthetic ETA revision history to report how often the fictional estimates moved earlier or later. This describes changes to ETA estimates only; it does not measure vessel delay or prediction accuracy.

Run this read-only query after the synthetic dbt build:

```sql
SELECT
    CASE
        WHEN eta_change_minutes < 0 THEN 'ETA moved earlier'
        WHEN eta_change_minutes > 0 THEN 'ETA moved later'
        ELSE 'ETA unchanged'
    END AS revision_direction,
    count(*) AS revision_count,
    round(avg(eta_change_minutes)::numeric, 1) AS average_change_minutes
FROM analytics.mart_synthetic_eta_revision_history
WHERE previous_eta_utc IS NOT NULL
GROUP BY 1
ORDER BY 1;
```

Use the finding only after reproducing the query against the seed-42, 1,000-call synthetic dataset and recording the actual output. Label it as a synthetic ETA revision result.
