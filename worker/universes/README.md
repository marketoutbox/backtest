# top100mc

`top100mc.csv` is a fixed list of 1,000 NSE equities ranked by their **NSE six-month average total market capitalization** for January–June 2026 (₹ crore). Source: [AMFI's 30 June 2026 capitalization report](https://portal.amfiindia.com/spages/AverageMarketCapitalization30Jun2026.pdf), prepared from exchange data. The source also ranks BSE-only companies; those rows were excluded, and the remaining NSE rows were sorted by the NSE capitalization column. Instrument keys use `NSE_EQ|` plus the report's ISIN.

This is a historical snapshot, not today's live market-cap ranking. It is intentionally unchanged until the user asks for a new list. Corporate actions and symbol changes can affect whether Upstox still recognizes individual keys; the importer reports affected symbols separately.
