# bb_alert_scheduler

## Disk guard (shared across every pipeline)

This scheduler writes to a Supabase volume shared with the Business Central
sync and the marketplace/ads loaders. Before it writes, it asks the database
whether it is allowed. **If you get an email titled `[WARN]` or `[STOP]
Supabase disk`, start here.**

```sql
-- the GRN schedulers genuinely need more room, and the volume has space:
UPDATE etl_disk_policy SET budget_gb = 8 WHERE pipeline = 'grn';

-- you resized the Supabase volume (do this EVERY time you resize):
UPDATE etl_disk_policy SET budget_gb = 100 WHERE pipeline = '_disk';

-- someone else should get the emails:
UPDATE etl_alert_config SET recipients = ARRAY['birbal@thebakersdozen.in'];
```

All thirteen GRN schedulers share one `grn` budget, because they are one
workload from the volume's point of view. A `[STOP]` means this scheduler is
refusing to write until you do one of the above. Nothing is lost: it stops
before writing and the next run continues.

`etl_alerts.py` is **identical in every pipeline repo** - do not add per-repo
logic to it. Everything configurable lives in Postgres (`etl_disk_policy`,
`etl_alert_config`), so budgets, thresholds and recipients change with an
`UPDATE` and no deploy, for all pipelines at once.

Three behaviours worth knowing:

- **No new credentials were needed.** This repo has no Postgres driver and no
  DSN, so the guard reaches the policy through PostgREST RPC using the
  `SUPABASE_URL` + service-role key it already holds.
- **It fails OPEN.** If the guard cannot run - credentials missing, database
  unreachable - it logs an error and lets the scheduler continue. A guard that
  breaks a working pipeline is worse than one that cannot check. Grep for
  `Disk guard could not run`.
- **Budgets grow themselves** into genuinely unallocated volume space, so a
  pipeline that is legitimately growing is not blocked by a number somebody
  guessed months ago. It can never grow past the volume ceiling.

Full documentation:
https://github.com/keyur-tbd/bc-supabase-sync#disk-alerts-and-auto-budgeting---start-here-if-you-got-an-email
