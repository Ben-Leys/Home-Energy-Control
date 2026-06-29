# Device Configuration

Use `hec/config.yaml.example` as the starting point. Secrets belong in `hec/.env`.

## HomeWizard P1 Meter

`p1_meter.host` points to the P1 meter IP or DNS name. The P1 reader only fetches smart meter data from
`http://<host>/api/v1/data`.

The HomeWizard battery group gateway is configured from the same host and `P1_METER` token. It owns the
`https://<host>/api/batteries` read and command API. Set `p1_meter.battery_verify_tls` to `false` for the local
self-signed certificate case.

## HomeWizard Batteries

Each entry under `batteries` is an individual battery measurement endpoint.

```yaml
batteries:
  - name: GARAGE
    host: 192.0.2.11
    verify_tls: false
```

The token is read from `BATTERY_<NAME>`, for example `BATTERY_GARAGE`.

## EVCC

Set `evcc.api_url` to the EVCC API base URL, usually `http://<host>:7070/api`. The controller validates manual current
updates through the API allowlist and rejects values outside the configured safe range.

## SMA Inverter

Set `inverter.host`, `port`, `modbus_unit_id`, and `standard_power_limit`. The `inverter.location` block is used for
daylight checks before sending inverter limit commands.

## ENTSO-E

Set `ENTSOE_API_KEY` in `hec/.env`. The `entsoe.domain` in the example is Belgium. The scheduler timezone is used to
build local day boundaries before converting to UTC for the ENTSO-E request.

## Elia

Set Elia dataset IDs under `elia`. Solar, wind, and grid load can use different forecast and historical dataset IDs.

## Shared HTTP Policy

The `http` section controls default timeout, retry count, backoff, and TLS verification policy for integrations that use
HTTP. Device-specific self-signed certificate settings can override TLS verification where needed.
