# Databricks script  ---  generate synthetic data at scale, with Spark.
#
# IMPORTANT (business credibility): for a diversified investment holding company,
# the LARGE tables are DAILY PRICES and DAILY HOLDINGS/VALUATION snapshots.
# Transactions, cashflows, valuations are business-critical but LOW volume.
# We never label a transaction-heavy dataset as the normal business volume.
#
# Modes (set MODE via a job parameter):
#   dev        ~  50k-100k  rows   iterative pipeline development
#   demo       ~ 500k-800k  rows   GitHub / dashboard end-to-end demonstration
#   realistic  ~ 11.2M      rows   modelled diversified-investment historical scale
#   perftest   ~ 50-100M    rows   capital-markets STRESS TEST only (transaction-heavy;
#                                  valid for a brokerage/asset-manager, NOT the
#                                  normal holding-company volume)
#
# Writes Bronze Delta tables directly (fast). This OVERWRITES the small sample
# bronze tables -- re-run data/generate_data.py to restore the sample.

from pyspark.sql import SparkSession, functions as F

from job_params import get_param

spark = SparkSession.builder.getOrCreate()
BRONZE = get_param("catalog", "investsphere") + ".bronze"
START = "2024-06-01"

MODE = get_param("mode", "realistic")   # dev | demo | realistic | perftest

# knobs per mode: listed assets, calendar days, portfolios, holdings/portfolio/day,
# transactions/day, currencies, private assets, funds, valuation months
PRESETS = {
    "dev":       dict(assets=300,   days=180, pf=5,   hold=40,  txn=30,    ccy=8,  priv=20,  fund=10,  months=6),
    "demo":      dict(assets=800,   days=504, pf=10,  hold=40,  txn=100,   ccy=10, priv=50,  fund=30,  months=12),
    "realistic": dict(assets=10000, days=730, pf=50,  hold=100, txn=250,   ccy=20, priv=500, fund=300, months=24),
    "perftest":  dict(assets=10000, days=756, pf=200, hold=200, txn=80000, ccy=20, priv=500, fund=300, months=24),
}
c = PRESETS[MODE]

# day and month dimensions
days = (spark.range(c["days"]).withColumnRenamed("id", "day_idx")
        .withColumn("d", F.expr(f"cast(date_add('{START}', cast(day_idx as int)) as string)")))
months = (spark.range(c["months"]).withColumnRenamed("id", "m_idx")
          .withColumn("m", F.expr(f"cast(add_months('{START}', cast(m_idx as int)) as string)")))


def write(df, table, cluster_cols):
    (df.writeTo(BRONZE + "." + table).using("delta").clusterBy(*cluster_cols).createOrReplace())
    print(table, "->", df.count())


# ---- listed_market_prices: assets x days  (the largest table) ----
prices = (spark.range(c["assets"]).withColumnRenamed("id", "a").crossJoin(days)
    .withColumn("investment_asset_id", F.concat(F.lit("SYN_"), F.col("a")))
    .withColumn("price_date", F.col("d"))
    .withColumn("close_price", F.round(10 + F.pmod(F.col("a"), F.lit(490)) + F.col("day_idx") * 0.01, 4))
    .select("price_date", "investment_asset_id", "close_price"))
write(prices, "raw_listed_market_prices", ["investment_asset_id", "price_date"])

# ---- investment_holdings_snapshot: portfolio x holdings x days  (second largest) ----
holdings = (spark.range(c["pf"]).withColumnRenamed("id", "p")
    .crossJoin(spark.range(c["hold"]).withColumnRenamed("id", "h"))
    .crossJoin(days)
    .withColumn("portfolio_id", F.concat(F.lit("PF_"), F.col("p")))
    .withColumn("investment_asset_id", F.concat(F.lit("SYN_"), F.pmod(F.col("h") + F.col("p"), F.lit(c["assets"]))))
    .withColumn("as_of_date", F.col("d"))
    .withColumn("quantity", (F.pmod(F.col("h"), F.lit(5000)) + 1).cast("double"))
    .select("portfolio_id", "as_of_date", "investment_asset_id", "quantity"))
write(holdings, "raw_investment_holdings_snapshot", ["portfolio_id", "as_of_date"])

# ---- investment_transactions: txn/day x days  (LOW volume, business-critical) ----
txn_total = c["txn"] * c["days"]
txns = (spark.range(txn_total).withColumnRenamed("id", "i")
    .withColumn("day_idx", (F.col("i") / F.lit(c["txn"])).cast("int"))
    .withColumn("transaction_id", F.concat(F.lit("TX"), F.col("i")))
    .withColumn("transaction_date", F.expr(f"cast(date_add('{START}', day_idx) as string)"))
    .withColumn("portfolio_id", F.concat(F.lit("PF_"), F.pmod(F.col("i"), F.lit(c["pf"]))))
    .withColumn("investment_asset_id", F.concat(F.lit("SYN_"), F.pmod(F.col("i"), F.lit(c["assets"]))))
    .withColumn("transaction_type", F.when(F.pmod(F.col("i"), F.lit(2)) == 0, F.lit("BUY")).otherwise(F.lit("SELL")))
    .withColumn("quantity", (F.pmod(F.col("i"), F.lit(1000)) + 1).cast("double"))
    .withColumn("price", F.round(10 + F.pmod(F.col("i"), F.lit(490)), 4))
    .withColumn("counterparty_id", F.concat(F.lit("CP_"), F.pmod(F.col("i"), F.lit(5))))
    .select("transaction_id", "transaction_date", "portfolio_id", "investment_asset_id", "transaction_type", "quantity", "price", "counterparty_id"))
write(txns, "raw_investment_transactions", ["portfolio_id", "transaction_date"])

# ---- cashflows: one per portfolio per day (low volume) ----
cf = (spark.range(c["pf"]).withColumnRenamed("id", "p").crossJoin(days)
    .withColumn("cashflow_id", F.concat(F.lit("CF"), F.col("p"), F.lit("_"), F.col("day_idx")))
    .withColumn("portfolio_id", F.concat(F.lit("PF_"), F.col("p")))
    .withColumn("cashflow_date", F.col("d"))
    .withColumn("type", F.element_at(F.array(F.lit("DIVIDEND"), F.lit("FEE"), F.lit("DISTRIBUTION")),
                                     (F.pmod(F.col("day_idx"), F.lit(3)) + 1).cast("int")))
    .withColumn("amount", F.round((F.pmod(F.col("day_idx"), F.lit(100000)) + 1) * 1.0, 2))
    .withColumn("currency", F.lit("USD"))
    .select("cashflow_id", "portfolio_id", "cashflow_date", "type", "amount", "currency"))
write(cf, "raw_cashflows", ["portfolio_id", "cashflow_date"])

# ---- currency_rates: currencies x days (low volume) ----
ccy = (spark.range(c["ccy"]).withColumnRenamed("id", "k").crossJoin(days)
    .withColumn("rate_date", F.col("d"))
    .withColumn("currency", F.concat(F.lit("C"), F.col("k")))
    .withColumn("rate_to_base", F.round(0.2 + F.pmod(F.col("k"), F.lit(5)) * 0.2, 4))
    .select("rate_date", "currency", "rate_to_base"))
write(ccy, "raw_currency_rates", ["currency", "rate_date"])

# ---- private_valuation_snapshot: private assets x months (monthly cadence) ----
priv = (spark.range(c["priv"]).withColumnRenamed("id", "x").crossJoin(months)
    .withColumn("investment_asset_id", F.concat(F.lit("PE_"), F.col("x")))
    .withColumn("valuation_date", F.col("m"))
    .withColumn("fair_value", F.round((F.pmod(F.col("x"), F.lit(1000)) + 1) * 100000.0, 2))
    .withColumn("currency", F.lit("USD"))
    .select("investment_asset_id", "valuation_date", "fair_value", "currency"))
write(priv, "raw_private_valuation_snapshot", ["investment_asset_id", "valuation_date"])

# ---- fund_nav_snapshot: funds x months (monthly cadence) ----
fund = (spark.range(c["fund"]).withColumnRenamed("id", "x").crossJoin(months)
    .withColumn("investment_asset_id", F.concat(F.lit("FUND_"), F.col("x")))
    .withColumn("nav_date", F.col("m"))
    .withColumn("nav_per_unit", F.round(90 + F.pmod(F.col("x"), F.lit(40)) + F.col("m_idx") * 0.1, 4))
    .withColumn("currency", F.lit("USD"))
    .select("investment_asset_id", "nav_date", "nav_per_unit", "currency"))
write(fund, "raw_fund_nav_snapshot", ["investment_asset_id", "nav_date"])

print("MODE:", MODE,
      "| prices + holdings are the large tables; transactions/cashflows/valuations are small.")
