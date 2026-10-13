# FP-Growth and alpha analysis

## Configuration

- Dataset: `../dataset/online_retail.parquet`
- Minimum support: 10
- Heap size: 1,000
- PFP groups: 500
- Local Spark cores: 10
- Product descriptions: available

## Dataset summary

- Rows: 541,909
- Transactions: 25,900
- Distinct products: 4,070
- Distinct `(product, quantity)` items: 41,267

## Main results

- Frequent patterns: 214,024
- Canonical shapes: 206,625
- Proportional shapes (more than one alpha): 5,437 (2.63% of shapes)
- Patterns belonging to proportional shapes: 12,836 (6.00% of patterns)
- Distinct alpha levels before reduction: 214,024
- Alpha levels collapsed: 7,399
- Reduction of distinct alpha levels: 3.46%

## Interpretation

Alpha is present in 2.63% of canonical shapes and covers 6.00% of returned patterns. It is therefore a minority structure in this dataset, but it identifies 5,437 distinct proportional families.

## 10 most frequent quantitative bundles

Support is the number and percentage of transactions containing the exact products with the exact quantities shown.

| # | Transactions | Share | Bundle |
|---:|---:|---:|---|
| 1 | 247 | 0.95% | ROSES REGENCY TEACUP AND SAUCER [22699] x6, GREEN REGENCY TEACUP AND SAUCER [22697] x6 |
| 2 | 222 | 0.86% | GREEN REGENCY TEACUP AND SAUCER [22697] x6, PINK REGENCY TEACUP AND SAUCER [22698] x6 |
| 3 | 199 | 0.77% | ROSES REGENCY TEACUP AND SAUCER [22699] x6, PINK REGENCY TEACUP AND SAUCER [22698] x6 |
| 4 | 199 | 0.77% | JUMBO BAG RED RETROSPOT [85099B] x10, JUMBO BAG PINK POLKADOT [22386] x10 |
| 5 | 197 | 0.76% | LUNCH BAG RED RETROSPOT [20725] x10, LUNCH BAG PINK POLKADOT [22384] x10 |
| 6 | 194 | 0.75% | LUNCH BAG RED RETROSPOT [20725] x10, LUNCH BAG SUKI DESIGN [22383] x10 |
| 7 | 182 | 0.70% | JUMBO BAG VINTAGE DOILY [23203] x10, LUNCH BAG VINTAGE DOILY [23209] x10 |
| 8 | 182 | 0.70% | JUMBO BAG RED RETROSPOT [85099B] x10, JUMBO BAG VINTAGE DOILY [23203] x10 |
| 9 | 181 | 0.70% | LUNCH BAG RED RETROSPOT [20725] x10, LUNCH BAG  BLACK SKULL. [20727] x10 |
| 10 | 180 | 0.69% | JUMBO BAG VINTAGE DOILY [23203] x10, JUMBO BAG VINTAGE LEAF [23202] x10 |

## 10 richest proportional families

Each shape is a quantity ratio. Multiplying it by an alpha produces one of the observed quantitative bundles. `Supports by alpha` uses the form `alpha: transactions`.

| # | Shape/ratio | Alpha levels | Supports by alpha |
|---:|---|---|---|
| 1 | GARDENERS KNEELING PAD CUP OF TEA [23300] x1, GARDENERS KNEELING PAD KEEP CALM [23301] x1 | [1, 2, 3, 4, 5, 6, 12, 24, 48] | 1: 62; 2: 68; 3: 41; 4: 26; 5: 20; 6: 24; 12: 85; 24: 12; 48: 14 |
| 2 | SPACEBOY LUNCH BOX [22629] x1, DOLLY GIRL LUNCH BOX [22630] x1 | [1, 2, 3, 4, 6, 12, 24, 64] | 1: 65; 2: 49; 3: 29; 4: 10; 6: 10; 12: 175; 24: 19; 64: 13 |
| 3 | WOODLAND CHARLOTTE BAG [20719] x1, RED RETROSPOT CHARLOTTE BAG [20724] x1 | [1, 2, 3, 6, 10, 20, 100] | 1: 36; 2: 19; 3: 12; 6: 10; 10: 117; 20: 16; 100: 10 |
| 4 | RED RETROSPOT CHARLOTTE BAG [20724] x1, CHARLOTTE BAG PINK POLKADOT [22356] x1 | [1, 2, 3, 5, 10, 20, 100] | 1: 34; 2: 22; 3: 17; 5: 12; 10: 133; 20: 17; 100: 11 |
| 5 | LUNCH BAG RED RETROSPOT [20725] x1, LUNCH BAG SUKI DESIGN [22383] x1 | [1, 2, 3, 4, 10, 20, 100] | 1: 60; 2: 46; 3: 21; 4: 13; 10: 194; 20: 24; 100: 10 |
| 6 | LUNCH BAG RED RETROSPOT [20725] x1, LUNCH BAG PINK POLKADOT [22384] x1 | [1, 2, 3, 4, 10, 20, 100] | 1: 62; 2: 57; 3: 27; 4: 11; 10: 197; 20: 31; 100: 11 |
| 7 | RED HANGING HEART T-LIGHT HOLDER [21733] x1, WHITE HANGING HEART T-LIGHT HOLDER [85123A] x1 | [1, 2, 3, 4, 6, 12, 32] | 1: 40; 2: 41; 3: 17; 4: 12; 6: 80; 12: 23; 32: 24 |
| 8 | PAPER CHAIN KIT 50'S CHRISTMAS [22086] x1, PAPER CHAIN KIT VINTAGE CHRISTMAS [22910] x1 | [1, 2, 3, 4, 6, 12, 40] | 1: 43; 2: 29; 3: 23; 4: 12; 6: 96; 12: 41; 40: 36 |
| 9 | WOODEN HEART CHRISTMAS SCANDINAVIAN [22577] x1, WOODEN STAR CHRISTMAS SCANDINAVIAN [22578] x1 | [1, 6, 10, 12, 20, 24, 48] | 1: 12; 6: 13; 10: 16; 12: 24; 20: 14; 24: 105; 48: 14 |
| 10 | GREEN REGENCY TEACUP AND SAUCER [22697] x1, PINK REGENCY TEACUP AND SAUCER [22698] x1 | [1, 2, 3, 4, 6, 12, 24] | 1: 80; 2: 54; 3: 13; 4: 16; 6: 222; 12: 29; 24: 34 |

## 10 strongest proportional families by support

These are the proportional families containing at least one of the most frequently observed scaled bundles.

| # | Peak transactions | Shape/ratio | Alpha levels | Supports by alpha |
|---:|---:|---|---|---|
| 1 | 247 | GREEN REGENCY TEACUP AND SAUCER [22697] x1, ROSES REGENCY TEACUP AND SAUCER [22699] x1 | [1, 2, 3, 4, 6, 12, 24] | 1: 88; 2: 67; 3: 20; 4: 18; 6: 247; 12: 37; 24: 49 |
| 2 | 222 | GREEN REGENCY TEACUP AND SAUCER [22697] x1, PINK REGENCY TEACUP AND SAUCER [22698] x1 | [1, 2, 3, 4, 6, 12, 24] | 1: 80; 2: 54; 3: 13; 4: 16; 6: 222; 12: 29; 24: 34 |
| 3 | 199 | PINK REGENCY TEACUP AND SAUCER [22698] x1, ROSES REGENCY TEACUP AND SAUCER [22699] x1 | [1, 2, 3, 4, 6, 12, 24] | 1: 75; 2: 48; 3: 15; 4: 17; 6: 199; 12: 27; 24: 35 |
| 4 | 199 | JUMBO BAG PINK POLKADOT [22386] x1, JUMBO BAG RED RETROSPOT [85099B] x1 | [1, 2, 3, 10, 20, 100] | 1: 61; 2: 42; 3: 24; 10: 199; 20: 25; 100: 29 |
| 5 | 197 | LUNCH BAG RED RETROSPOT [20725] x1, LUNCH BAG PINK POLKADOT [22384] x1 | [1, 2, 3, 4, 10, 20, 100] | 1: 62; 2: 57; 3: 27; 4: 11; 10: 197; 20: 31; 100: 11 |
| 6 | 194 | LUNCH BAG RED RETROSPOT [20725] x1, LUNCH BAG SUKI DESIGN [22383] x1 | [1, 2, 3, 4, 10, 20, 100] | 1: 60; 2: 46; 3: 21; 4: 13; 10: 194; 20: 24; 100: 10 |
| 7 | 182 | JUMBO BAG VINTAGE DOILY [23203] x1, LUNCH BAG VINTAGE DOILY [23209] x1 | [1, 2, 3, 10, 20, 100] | 1: 29; 2: 19; 3: 11; 10: 182; 20: 20; 100: 11 |
| 8 | 182 | JUMBO BAG VINTAGE DOILY [23203] x1, JUMBO BAG RED RETROSPOT [85099B] x1 | [1, 2, 3, 10, 20, 100] | 1: 32; 2: 23; 3: 15; 10: 182; 20: 34; 100: 23 |
| 9 | 181 | LUNCH BAG RED RETROSPOT [20725] x1, LUNCH BAG  BLACK SKULL. [20727] x1 | [1, 2, 3, 5, 10, 20] | 1: 42; 2: 43; 3: 27; 5: 10; 10: 181; 20: 28 |
| 10 | 180 | JUMBO BAG VINTAGE LEAF [23202] x1, JUMBO BAG VINTAGE DOILY [23203] x1 | [1, 2, 3, 10, 20, 100] | 1: 43; 2: 20; 3: 15; 10: 180; 20: 30; 100: 22 |

## Technical appendix

- Returned-pattern lengths (`length: count`): `{"2": 116916, "3": 80958, "4": 11280, "5": 3513, "6": 963, "7": 299, "8": 81, "9": 13, "10": 1}`
- Returned-pattern support buckets (`range: count`): `{"8-15": 169548, "16-31": 40274, "32-63": 3554, "64-127": 575, "128-255": 73}`
- Alpha counts among proportional shapes (`alphas: shapes`): `{"2": 4099, "3": 964, "4": 198, "5": 121, "6": 39, "7": 14, "8": 1, "9": 1}`
- These distributions describe the top-K-limited patterns returned by the configured PFP run; they are not a complete enumeration of every possible frequent pattern.

## Visual summary

- `top_bundles.png`
- `top_alpha_families.png`

Full lists are available in `top_patterns.csv`, `top_alpha_groups.csv`, and `top_supported_alpha_groups.csv`.
