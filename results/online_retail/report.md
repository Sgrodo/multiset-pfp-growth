# FP-Growth and alpha analysis

## Configuration

- Dataset: `../dataset/online_retail.parquet`
- Minimum support: 10
- Heap size: 1,000
- PFP groups: 500
- Local Spark cores: 10
- Returns/negative quantities: retained
- Product descriptions: available

## Dataset summary

- Rows: 541,909
- Transactions: 25,900
- Distinct products: 4,070
- Distinct `(product, quantity)` items: 45,280

## Main results

- Frequent patterns: 224,306
- Canonical shapes: 216,278
- Proportional shapes (more than one alpha): 6,016 (2.78% of shapes)
- Patterns belonging to proportional shapes: 14,044 (6.26% of patterns)
- Distinct alpha levels before reduction: 224,306
- Alpha levels collapsed: 8,028
- Reduction of distinct alpha levels: 3.58%

## Interpretation

Alpha is present in 2.78% of canonical shapes and covers 6.26% of returned patterns. It is therefore a minority structure in this dataset, but it identifies 6,016 distinct proportional families.

## 10 most frequent quantitative bundles

Support is the number and percentage of transactions containing the exact products with the exact quantities shown.

| # | Transactions | Share | Bundle |
|---:|---:|---:|---|
| 1 | 246 | 0.95% | ROSES REGENCY TEACUP AND SAUCER [22699] x6, GREEN REGENCY TEACUP AND SAUCER [22697] x6 |
| 2 | 224 | 0.86% | GREEN REGENCY TEACUP AND SAUCER [22697] x6, PINK REGENCY TEACUP AND SAUCER [22698] x6 |
| 3 | 201 | 0.78% | JUMBO BAG RED RETROSPOT [85099B] x10, JUMBO BAG PINK POLKADOT [22386] x10 |
| 4 | 199 | 0.77% | LUNCH BAG RED RETROSPOT [20725] x10, LUNCH BAG PINK POLKADOT [22384] x10 |
| 5 | 199 | 0.77% | ROSES REGENCY TEACUP AND SAUCER [22699] x6, PINK REGENCY TEACUP AND SAUCER [22698] x6 |
| 6 | 194 | 0.75% | LUNCH BAG RED RETROSPOT [20725] x10, LUNCH BAG SUKI DESIGN [22383] x10 |
| 7 | 183 | 0.71% | JUMBO BAG VINTAGE DOILY [23203] x10, LUNCH BAG VINTAGE DOILY [23209] x10 |
| 8 | 182 | 0.70% | JUMBO BAG RED RETROSPOT [85099B] x10, JUMBO BAG VINTAGE DOILY [23203] x10 |
| 9 | 181 | 0.70% | LUNCH BAG RED RETROSPOT [20725] x10, LUNCH BAG  BLACK SKULL. [20727] x10 |
| 10 | 181 | 0.70% | LUNCH BAG SUKI DESIGN [22383] x10, LUNCH BAG CARS BLUE [20728] x10 |

## 10 richest proportional families

Each shape is a quantity ratio. Multiplying it by an alpha produces one of the observed quantitative bundles. `Supports by alpha` uses the form `alpha: transactions`.

| # | Shape/ratio | Alpha levels | Supports by alpha |
|---:|---|---|---|
| 1 | GARDENERS KNEELING PAD CUP OF TEA [23300] x1, GARDENERS KNEELING PAD KEEP CALM [23301] x1 | [1, 2, 3, 4, 5, 6, 12, 24, 48] | 1: 70; 2: 68; 3: 41; 4: 26; 5: 19; 6: 23; 12: 87; 24: 12; 48: 14 |
| 2 | WOODLAND CHARLOTTE BAG [20719] x1, RED RETROSPOT CHARLOTTE BAG [20724] x1 | [1, 2, 3, 6, 10, 20, 100] | 1: 40; 2: 20; 3: 10; 6: 10; 10: 117; 20: 16; 100: 10 |
| 3 | RED RETROSPOT CHARLOTTE BAG [20724] x1, CHARLOTTE BAG PINK POLKADOT [22356] x1 | [1, 2, 3, 5, 10, 20, 100] | 1: 38; 2: 25; 3: 15; 5: 10; 10: 133; 20: 17; 100: 10 |
| 4 | LUNCH BAG RED RETROSPOT [20725] x1, LUNCH BAG PINK POLKADOT [22384] x1 | [1, 2, 3, 4, 10, 20, 100] | 1: 72; 2: 54; 3: 25; 4: 11; 10: 199; 20: 31; 100: 11 |
| 5 | RED HANGING HEART T-LIGHT HOLDER [21733] x1, WHITE HANGING HEART T-LIGHT HOLDER [85123A] x1 | [1, 2, 3, 4, 6, 12, 32] | 1: 44; 2: 44; 3: 19; 4: 12; 6: 79; 12: 23; 32: 24 |
| 6 | PAPER CHAIN KIT 50'S CHRISTMAS [22086] x1, PAPER CHAIN KIT VINTAGE CHRISTMAS [22910] x1 | [1, 2, 3, 4, 6, 12, 40] | 1: 51; 2: 30; 3: 23; 4: 13; 6: 95; 12: 42; 40: 36 |
| 7 | WOODEN HEART CHRISTMAS SCANDINAVIAN [22577] x1, WOODEN STAR CHRISTMAS SCANDINAVIAN [22578] x1 | [1, 6, 10, 12, 20, 24, 48] | 1: 14; 6: 14; 10: 18; 12: 25; 20: 13; 24: 105; 48: 14 |
| 8 | SPACEBOY LUNCH BOX [22629] x1, DOLLY GIRL LUNCH BOX [22630] x1 | [1, 2, 3, 4, 12, 24, 64] | 1: 73; 2: 45; 3: 29; 4: 13; 12: 175; 24: 19; 64: 13 |
| 9 | GREEN REGENCY TEACUP AND SAUCER [22697] x1, PINK REGENCY TEACUP AND SAUCER [22698] x1 | [1, 2, 3, 4, 6, 12, 24] | 1: 84; 2: 52; 3: 14; 4: 15; 6: 224; 12: 29; 24: 34 |
| 10 | GREEN REGENCY TEACUP AND SAUCER [22697] x1, ROSES REGENCY TEACUP AND SAUCER [22699] x1 | [1, 2, 3, 4, 6, 12, 24] | 1: 88; 2: 62; 3: 20; 4: 15; 6: 246; 12: 37; 24: 50 |

## 10 strongest proportional families by support

These are the proportional families containing at least one of the most frequently observed scaled bundles.

| # | Peak transactions | Shape/ratio | Alpha levels | Supports by alpha |
|---:|---:|---|---|---|
| 1 | 246 | GREEN REGENCY TEACUP AND SAUCER [22697] x1, ROSES REGENCY TEACUP AND SAUCER [22699] x1 | [1, 2, 3, 4, 6, 12, 24] | 1: 88; 2: 62; 3: 20; 4: 15; 6: 246; 12: 37; 24: 50 |
| 2 | 224 | GREEN REGENCY TEACUP AND SAUCER [22697] x1, PINK REGENCY TEACUP AND SAUCER [22698] x1 | [1, 2, 3, 4, 6, 12, 24] | 1: 84; 2: 52; 3: 14; 4: 15; 6: 224; 12: 29; 24: 34 |
| 3 | 201 | JUMBO BAG PINK POLKADOT [22386] x1, JUMBO BAG RED RETROSPOT [85099B] x1 | [1, 2, 3, 10, 20, 100] | 1: 63; 2: 50; 3: 23; 10: 201; 20: 24; 100: 28 |
| 4 | 199 | LUNCH BAG RED RETROSPOT [20725] x1, LUNCH BAG PINK POLKADOT [22384] x1 | [1, 2, 3, 4, 10, 20, 100] | 1: 72; 2: 54; 3: 25; 4: 11; 10: 199; 20: 31; 100: 11 |
| 5 | 199 | PINK REGENCY TEACUP AND SAUCER [22698] x1, ROSES REGENCY TEACUP AND SAUCER [22699] x1 | [1, 2, 3, 4, 6, 12, 24] | 1: 77; 2: 46; 3: 15; 4: 16; 6: 199; 12: 27; 24: 36 |
| 6 | 194 | LUNCH BAG RED RETROSPOT [20725] x1, LUNCH BAG SUKI DESIGN [22383] x1 | [1, 2, 3, 4, 10, 20] | 1: 67; 2: 43; 3: 19; 4: 10; 10: 194; 20: 24 |
| 7 | 183 | JUMBO BAG VINTAGE DOILY [23203] x1, LUNCH BAG VINTAGE DOILY [23209] x1 | [1, 2, 10, 20, 100] | 1: 32; 2: 18; 10: 183; 20: 20; 100: 11 |
| 8 | 182 | JUMBO BAG VINTAGE DOILY [23203] x1, JUMBO BAG RED RETROSPOT [85099B] x1 | [1, 2, 3, 10, 20, 100] | 1: 34; 2: 27; 3: 14; 10: 182; 20: 34; 100: 22 |
| 9 | 181 | LUNCH BAG RED RETROSPOT [20725] x1, LUNCH BAG  BLACK SKULL. [20727] x1 | [1, 2, 3, 5, 10, 20] | 1: 56; 2: 43; 3: 25; 5: 10; 10: 181; 20: 28 |
| 10 | 181 | JUMBO BAG VINTAGE LEAF [23202] x1, JUMBO BAG VINTAGE DOILY [23203] x1 | [1, 2, 3, 10, 20, 100] | 1: 46; 2: 21; 3: 15; 10: 181; 20: 30; 100: 21 |

## Technical appendix

- Returned-pattern lengths (`length: count`): `{"2": 124838, "3": 83305, "4": 11398, "5": 3475, "6": 946, "7": 266, "8": 66, "9": 11, "10": 1}`
- Returned-pattern support buckets (`range: count`): `{"8-15": 177179, "16-31": 42737, "32-63": 3728, "64-127": 589, "128-255": 73}`
- Alpha counts among proportional shapes (`alphas: shapes`): `{"2": 4610, "3": 1019, "4": 226, "5": 116, "6": 34, "7": 10, "9": 1}`
- These distributions describe the top-K-limited patterns returned by the configured PFP run; they are not a complete enumeration of every possible frequent pattern.

## Visual summary

- `top_bundles.png`
- `top_alpha_families.png`

Full lists are available in `top_patterns.csv`, `top_alpha_groups.csv`, and `top_supported_alpha_groups.csv`.
