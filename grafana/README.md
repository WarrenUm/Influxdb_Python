# Grafana low-code dashboard (OSRS GE prices)

This directory contains provisioning for a Grafana instance that reads the OSRS
Grand Exchange price data directly from InfluxDB v2 — no application code required.

```
grafana/
├── provisioning/
│   ├── datasources/influxdb.yaml     # InfluxDB v2 Flux data source (token from env)
│   ├── dashboards/dashboards.yaml    # dashboard provider -> /var/lib/grafana/dashboards
│   └── alerting/deviation-alert.yaml # price-deviation alert rule (with guard)
├── dashboards/
│   └── ge-prices.json                # line + candlestick + gauge panels, $itemID variable
└── README.md
```

## Data assumptions

- InfluxDB v2 at `http://localhost:8086`
- Org `Ge-data-project`, bucket `GEItemPrices`
- Measurement `itemPrice`, tag `itemID`
- Fields `avgHighPrice`, `avgLowPrice`, `highPriceVolume`, `lowPriceVolume`

## Environment variables

The data source token is **not** stored in these files. Grafana expands
`${INFLUX_TOKEN}` from the environment when it loads the provisioning file, so you
must export a valid InfluxDB read token before starting Grafana:

```bash
export INFLUX_TOKEN="<your-influxdb-v2-read-token>"
```

Keep this token out of source control (the repo's `.env` is already git-ignored).

## Running Grafana with this provisioning

Using Docker, mount the three trees and pass the token through:

```bash
docker run -d --name grafana-ge \
  -p 3000:3000 \
  -e INFLUX_TOKEN="$INFLUX_TOKEN" \
  -v "$(pwd)/grafana/provisioning:/etc/grafana/provisioning" \
  -v "$(pwd)/grafana/dashboards:/var/lib/grafana/dashboards" \
  grafana/grafana:latest
```

On Linux, if Grafana runs on the host and InfluxDB is in a container (or vice
versa), adjust `url` in `provisioning/datasources/influxdb.yaml` accordingly
(for example `http://host.docker.internal:8086`).

Then open http://localhost:3000 and find the **OSRS GE Prices** dashboard under the
**OSRS GE** folder.

## Authentication

Grafana keeps its own built-in login (default `admin` / `admin`, which it prompts
you to change on first sign-in). This provisioning does **not** disable auth or
enable anonymous access, so the dashboard stays behind Grafana's login.

## Dashboard contents

- **`$itemID` variable** — populated from distinct `itemID` tag values via a Flux
  `schema.tagValues` query.
- **Line panel** — `avgHighPrice` and `avgLowPrice` for the selected item.
- **Candlestick panel** — high/low candles derived from the same fields.
- **Gauge panel** — latest combined volume (`highPriceVolume + lowPriceVolume`).

All panel queries filter with the parameterized `r.itemID == "${itemID}"`.

## Deviation alert

`provisioning/alerting/deviation-alert.yaml` fires when the latest `avgHighPrice`
deviates from its moving average by more than a configured `factor` (default
`0.25`). To avoid false positives, the Flux query suppresses the breach row when
the latest price, the moving average, or the factor is zero or undefined; combined
with `noDataState: OK`, the alert cannot fire on missing or degenerate data.
Adjust `factor` (and the moving-average window `n`) in that file to tune sensitivity.
