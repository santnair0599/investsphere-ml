# The bug that taught me how Unity Catalog row-level security *really* works

*Draft LinkedIn post — InvestSphere build log, 2026-06-08*

---

I was building a governed lakehouse on Databricks (Free Edition, serverless) for my InvestSphere project — a capital-markets data platform with concentration-limit breach reporting. Bronze → Silver → Gold all green. Unity Catalog set up. Row-level security in place so a portfolio manager only sees their own portfolios.

Then I tried to create a simple SQL function to return the breaches for one portfolio:

```sql
CREATE OR REPLACE FUNCTION investsphere.ai.get_portfolio_breaches(target_portfolio STRING)
RETURNS TABLE (...)
RETURN SELECT ... FROM investsphere.gold.fact_limit_breach
WHERE portfolio_id = target_portfolio;
```

And got this:

> `[UNRESOLVED_COLUMN] A column with name m.user_email cannot be resolved. Did you mean m.pm_user, m.portfolio_id?`

I stared at it. My function doesn't mention `m`, or `user_email`, or anything close. Where was this coming from?

**The penny dropped: it wasn't my function. It was the row filter.**

Three things clicked into place about how Unity Catalog actually works:

**1. Row filters are evaluated LAZILY — at query time, not when you attach them.**
When I ran `ALTER TABLE … SET ROW FILTER …`, Databricks happily accepted it. The filter function had a bug — it referenced `m.user_email`, but my mapping table's column is `pm_user` — but UC doesn't run the filter at attach time. It runs it the moment *someone reads the table*. My very first read of `fact_limit_breach` was happening now, through this function. That's why a totally unrelated `CREATE FUNCTION` surfaced a column error from a different object entirely.

**2. The error points at the query, but the fault is in the policy.**
The stack trace highlighted my innocent `SELECT`. The actual broken code was the filter function sitting silently on the table. Lesson: when you see `UNRESOLVED_COLUMN` naming columns you never wrote, check for a row filter or column mask on the table.

**3. Policies are referenced by NAME — which is a gift.**
Because every secured table points at the filter *function* by name, I didn't have to touch a single table. One `CREATE OR REPLACE FUNCTION` with the correct column (`pm_user`, not `user_email`) fixed every table using it, all at once.

**The real root cause was boring: column-name drift.** The filter function and the mapping table had drifted apart on one column name. So I did what any platform engineer should: I made the names a *contract*. The mapping table, the function, and a header comment now all state the same two columns — `pm_user`, `portfolio_id` — and the function parameter is renamed `pid` so it can never be confused with the column. Names that can't drift can't break.

**Takeaways if you're doing UC governance:**
- Row filters / column masks validate at **read time**, not attach time. Test by actually querying the table.
- A confusing column error can come from a **policy on the table**, not your query. `DESCRIBE TABLE EXTENDED` shows attached filters/masks.
- Keep the policy function and its lookup table on **one documented column contract**.
- Fixing the **function** fixes every table that references it — design for that.

Governance isn't the glamorous part of data engineering, but it's where the platform earns trust. And sometimes a one-character column mismatch teaches you more about the engine than the docs do.

#DataEngineering #Databricks #UnityCatalog #Lakehouse #DataGovernance #Spark

---

*Part of my InvestSphere build — a governed investment-data platform on Databricks. More build-log posts in this folder.*
