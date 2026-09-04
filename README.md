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

## Birbal reads these tables (shared across every pipeline)

Since 2026-09-03 the Supabase project this writes to also backs **Birbal**
(`birbal-tbdai/birbal-mission-control`), the app the business asks questions in
plain language. Birbal never reads `public` directly: it reads one `select *`
view per table in a separate `warehouse` schema, plus a dictionary row per table
that tells it what the columns mean. Two consequences for this repo.

**A new table, or a new column, is invisible to Birbal until somebody exposes
it.** A view freezes its column list at CREATE time, and the exposure list is an
array inside a function - so nothing errors anywhere. The table simply does not
exist as far as the business is concerned, and an answer quietly leaves the new
column out. After applying the DDL this repo prints, run as `postgres`:

```sql
select app.sync_warehouse_views();   -- mirror new tables and columns
select app.sync_role_grants();       -- re-grant: the mirror drops grants
```

and add or update that table's row in `warehouse.warehouse_meta`. A column
nobody described there is a column Birbal will not use correctly.

**Never DROP or rename a table this pipeline owns.** A `warehouse` view depends
on it, so a plain `DROP` fails and `DROP ... CASCADE` deletes Birbal's view
without a word - that is how the BC sync went red on 2026-09-04. Add columns;
never replace tables. The writes themselves are safe by construction: every row
upserts on `row_hash`, so a reader never sees a half-loaded table.

Exposed today: all sixteen GRN/PRN tables the shared
`supabase_sink.py` knows about, including the one this repo loads (`GRN_SOURCE`).
Run `--print-schema` for the DDL when you promote a field.

Full contract, and the checks to run after a schema change:
https://github.com/keyur-tbd/bc-supabase-sync#who-reads-this-database-birbal
