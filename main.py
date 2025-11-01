from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, time
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.monitor import MonitorManagementClient

# Optional: load .env if available
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


# =========================
# Data models
# =========================

@dataclass
class InventoryItem:
    subscription_id: str
    resource_group: str
    resource_id: str
    location: Optional[str]
    full_type: str             # e.g., "Microsoft.Sql/servers/databases"
    name: str
    tenant_id: Optional[str] = None


@dataclass
class ServiceSpec:
    resource_types: List[str]
    metrics: List[str]
    aggregations: List[str]
    metric_namespace: Optional[str] = None
    filter_odata: Optional[str] = None
    dimensions: Optional[List[str]] = None  # (not used with mgmt-plane; kept for parity)


# =========================
# Helpers
# =========================

def _parse_resource_id(resource_id: str) -> Dict[str, Optional[str]]:
    parts = [p for p in resource_id.strip("/").split("/") if p]
    out = {"subscription_id": None, "resource_group": None, "provider_ns": None,
           "resource_type": None, "resource_name": None, "full_type": None}
    try:
        out["subscription_id"] = parts[parts.index("subscriptions")+1]
    except Exception:
        pass
    try:
        out["resource_group"] = parts[parts.index("resourceGroups")+1]
    except Exception:
        pass
    if "providers" in parts:
        i = parts.index("providers")
        seg = parts[i+1:]
        if len(seg) >= 3:
            out["provider_ns"], out["resource_type"], out["resource_name"] = seg[0], seg[1], seg[2]
            out["full_type"] = "/".join([seg[0]] + seg[1:-1])  # e.g., Microsoft.Sql/servers/databases
    return out


def _to_utc_span(start_date: str, end_date: str, tz: str) -> Tuple[datetime, datetime]:
    z = ZoneInfo(tz)
    s_local = datetime.combine(datetime.strptime(start_date, "%Y-%m-%d").date(), time(0, 0), tzinfo=z)
    e_local = datetime.combine(datetime.strptime(end_date, "%Y-%m-%d").date(), time(23, 59, 59), tzinfo=z)
    return s_local.astimezone(ZoneInfo("UTC")), e_local.astimezone(ZoneInfo("UTC"))


def _iso_timespan(start_utc: datetime, end_utc: datetime) -> str:
    return f"{start_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}"


def _pt_interval(minutes: int) -> Optional[str]:
    return f"PT{int(minutes)}M" if minutes else None


def _agg_value_mgmt(dp: Any, agg: str):
    return getattr(dp, agg.lower(), None)


# =========================
# NEW: Services listing
# =========================

def list_services_in_resource_group(
    credential: ClientSecretCredential,
    subscription_id: str,
    resource_group: str
) -> List[Dict[str, Optional[str]]]:
    """
    List all resources (services) within a given RG.
    Returns dicts: name, type, id, location, resource_group, subscription_id
    """
    rm = ResourceManagementClient(credential, subscription_id)
    services: List[Dict[str, Optional[str]]] = []
    for res in rm.resources.list_by_resource_group(resource_group):
        services.append({
            "subscription_id": subscription_id,
            "resource_group": resource_group,
            "name": res.name,
            "type": res.type,
            "id": res.id,
            "location": getattr(res, "location", None),
        })
    return services


def list_all_services_for_tenant(
    credential: ClientSecretCredential,
    subscriptions: List[str],
    resource_groups: List[str] | str
) -> pd.DataFrame:
    """
    Enumerate all services for the tenant across subscriptions/RGs.
    Supports '*' to list all RGs in each subscription.
    """
    rows: List[Dict[str, Optional[str]]] = []
    for sub in subscriptions:
        rm = ResourceManagementClient(credential, sub)
        if resource_groups == "*" or (isinstance(resource_groups, list) and resource_groups == ["*"]):
            rgs = [rg.name for rg in rm.resource_groups.list()]
        else:
            rgs = list(resource_groups) if isinstance(resource_groups, list) else [resource_groups]
        for rg in rgs:
            rows.extend(list_services_in_resource_group(credential, sub, rg))
    return pd.DataFrame(rows)


# =========================
# Timeseries flatten (metrics)
# =========================

def _flatten_timeseries_mgmt(result, resource: InventoryItem, aggregations: List[str], service_key: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for metric in (result.value or []):
        mname = getattr(metric.name, "value", None) or getattr(metric.name, "localized_value", None)
        unit = getattr(metric, "unit", None)
        for ts in (metric.timeseries or []):
            meta = {}
            for mv in getattr(ts, "metadatavalues", []) or []:
                n = getattr(mv.name, "value", None) or getattr(mv.name, "localized_value", None)
                v = getattr(mv, "value", None)
                if n:
                    meta[f"dimension_{n}"] = v
            for dp in (ts.data or []):
                ts_ = getattr(dp, "time_stamp", None) or getattr(dp, "timestamp", None)
                for agg in aggregations:
                    val = _agg_value_mgmt(dp, agg)
                    if val is None:
                        continue
                    rows.append(
                        {
                            "tenant_id": resource.tenant_id,
                            "subscription_id": resource.subscription_id,
                            "resource_group": resource.resource_group,
                            "resource_type": resource.full_type,
                            "resource_name": resource.name,
                            "resource_location": resource.location,
                            "resource_id": resource.resource_id,
                            "service_key": service_key,
                            "metric_namespace": getattr(metric, "namespace", None),
                            "metric_name": mname,
                            "aggregation": agg,
                            "unit": unit,
                            "timestamp": ts_,
                            "value": val,
                            **meta,
                        }
                    )
    return rows


# =========================
# Resource discovery (for metrics flow)
# =========================

def discover_resources_by_rg(
    credential: ClientSecretCredential,
    tenant_id: str,
    subscriptions: List[str],
    resource_groups: List[str] | str,
    resource_types: Optional[List[str]] = None,
) -> List[InventoryItem]:
    items: List[InventoryItem] = []
    for sub in subscriptions:
        rm = ResourceManagementClient(credential, sub)
        if resource_groups == "*" or (isinstance(resource_groups, list) and resource_groups == ["*"]):
            rgs = [rg.name for rg in rm.resource_groups.list()]
        else:
            rgs = list(resource_groups) if isinstance(resource_groups, list) else [resource_groups]
        for rg in rgs:
            for res in rm.resources.list_by_resource_group(rg):
                rid = res.id
                meta = _parse_resource_id(rid)
                full_type = meta.get("full_type") or f"{meta.get('provider_ns')}/{meta.get('resource_type')}"
                full_type = res.type
                if resource_types and full_type not in set(resource_types):
                    continue
                items.append(
                    InventoryItem(
                        subscription_id=sub,
                        resource_group=rg,
                        resource_id=rid,
                        location=getattr(res, "location", None),
                        full_type=full_type,
                        name=getattr(res, "name", meta.get("resource_name") or ""),
                        tenant_id=tenant_id,
                    )
                )
    return items


# =========================
# Service registry → plan
# =========================

def build_query_plan(
    inventory: List[InventoryItem],
    service_registry: Dict[str, ServiceSpec],
) -> List[Dict[str, Any]]:
    plan: List[Dict[str, Any]] = []
    by_type: Dict[str, List[Tuple[str, ServiceSpec]]] = {}
    for key, spec in service_registry.items():
        for t in spec.resource_types:
            by_type.setdefault(t, []).append((key, spec))
    for item in inventory:
        for service_key, spec in by_type.get(item.full_type, []):
            plan.append(
                {
                    "service_key": service_key,
                    "resource": item,
                    "metrics": list(spec.metrics),
                    "aggregations": list(spec.aggregations) if spec.aggregations else ["Average"],
                    "metric_namespace": spec.metric_namespace,
                    "filter_odata": spec.filter_odata,
                }
            )
    return plan


# =========================
# Metrics execution (MonitorManagementClient)
# =========================

def list_inventory_with_metrics_mgmt(
    monitor_clients_by_sub: Dict[str, MonitorManagementClient],
    plan: List[Dict[str, Any]],
    *,
    start_date: str,
    end_date: str,
    interval_minutes: int,
    timezone: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not plan:
        raise ValueError("Empty query plan.")

    start_utc, end_utc = _to_utc_span(start_date, end_date, timezone)
    timespan = _iso_timespan(start_utc, end_utc)
    interval = _pt_interval(interval_minutes)

    all_rows: List[Dict[str, Any]] = []
    notes: List[Dict[str, Any]] = []

    defs_cache: Dict[Tuple[str, Optional[str]], set] = {}

    for item in plan:
        res: InventoryItem = item["resource"]
        service_key: str = item["service_key"]
        metrics: List[str] = item["metrics"]
        aggs: List[str] = item["aggregations"]
        ns: Optional[str] = item.get("metric_namespace")
        filter_odata: Optional[str] = item.get("filter_odata")

        monitor = monitor_clients_by_sub.get(res.subscription_id)
        if monitor is None:
            notes.append({"tenant_id": res.tenant_id, "subscription_id": res.subscription_id, "resource_id": res.resource_id,
                          "stage": "client", "error": "No MonitorManagementClient for subscription"})
            continue

        cache_key = (res.resource_id, ns)
        supported = defs_cache.get(cache_key)
        if supported is None:
            try:
                defs = monitor.metric_definitions.list(res.resource_id, metricnamespace=ns)
                supported = {d.name.value for d in defs}
                defs_cache[cache_key] = supported
            except Exception as e:
                notes.append({"tenant_id": res.tenant_id, "subscription_id": res.subscription_id, "resource_id": res.resource_id,
                              "full_type": res.full_type, "service_key": service_key, "stage": "metric_definitions", "error": str(e)})
                continue

        wanted = [m for m in metrics if m in supported]
        missing = [m for m in metrics if m not in supported]
        if not wanted:
            notes.append({"tenant_id": res.tenant_id, "subscription_id": res.subscription_id, "resource_id": res.resource_id,
                          "full_type": res.full_type, "service_key": service_key, "stage": "validation",
                          "warning": f"No requested metrics supported. Missing: {missing}"})
            continue
        if missing:
            notes.append({"tenant_id": res.tenant_id, "subscription_id": res.subscription_id, "resource_id": res.resource_id,
                          "full_type": res.full_type, "service_key": service_key, "stage": "validation",
                          "warning": f"Unsupported metrics skipped: {missing}"})

        try:
            resp = monitor.metrics.list(
                resource_uri=res.resource_id,
                timespan=timespan,
                interval=interval,
                metricnames=",".join(wanted),
                aggregation=",".join(aggs),
                metricnamespace=ns,
                filter=filter_odata,
                auto_adjust_timegrain=True,
                validate_dimensions=False,
            )
        except Exception as e:
            notes.append({"tenant_id": res.tenant_id, "subscription_id": res.subscription_id, "resource_id": res.resource_id,
                          "full_type": res.full_type, "service_key": service_key, "stage": "metrics.list", "error": str(e)})
            continue

        rows = _flatten_timeseries_mgmt(resp, res, aggs, service_key)
        if not rows:
            notes.append({"tenant_id": res.tenant_id, "subscription_id": res.subscription_id, "resource_id": res.resource_id,
                          "full_type": res.full_type, "service_key": service_key, "stage": "flatten", "warning": "No data points returned"})
        all_rows.extend(rows)

    df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(
        columns=[
            "tenant_id","subscription_id","resource_group","resource_type","resource_name","resource_location","resource_id",
            "service_key","metric_namespace","metric_name","aggregation","unit","timestamp","value"
        ]
    )
    notes_df = pd.DataFrame(notes)
    return df, notes_df


# =========================
# Registry I/O
# =========================

def _load_service_registry(path: str) -> Dict[str, ServiceSpec]:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    reg: Dict[str, ServiceSpec] = {}
    for key, val in raw.items():
        reg[key] = ServiceSpec(
            resource_types=val.get("resource_types", []),
            metrics=val.get("metrics", []),
            aggregations=val.get("aggregations", ["Average"]),
            metric_namespace=val.get("metric_namespace"),
            filter_odata=val.get("filter_odata"),
            dimensions=val.get("dimensions"),
        )
    return reg


# =========================
# Settings from .env (multi-tenant)
# =========================

def _coerce_list(value: str) -> List[str]:
    return [x.strip() for x in value.split(",") if x.strip()]

def _load_env_settings() -> Dict[str, Any]:
    if load_dotenv:
        load_dotenv(override=False)

    start_date = os.getenv("START_DATE", "").strip()
    end_date = os.getenv("END_DATE", "").strip()
    if not start_date or not end_date:
        raise ValueError("Env must define START_DATE and END_DATE (YYYY-MM-DD).")
    interval_minutes = int(os.getenv("INTERVAL_MINUTES", "5"))
    timezone = os.getenv("TIMEZONE", "America/El_Salvador").strip()
    output = os.getenv("OUTPUT_PATH", "").strip()
    per_tenant_output = os.getenv("PER_TENANT_OUTPUT", "true").lower() == "true"

    list_services_only = os.getenv("LIST_SERVICES_ONLY", "false").lower() == "true"  # NEW toggle

    tenant_count = int(os.getenv("TENANT_COUNT", "1"))
    tenants: List[Dict[str, Any]] = []
    for i in range(1, tenant_count + 1):
        px = f"TENANT_{i}_"
        tenant_id = os.getenv(px + "TENANT_ID", "").strip()
        client_id = os.getenv(px + "CLIENT_ID", "").strip()
        client_secret = os.getenv(px + "CLIENT_SECRET", "").strip()
        subs = _coerce_list(os.getenv(px + "SUBSCRIPTIONS", ""))
        rgs_raw = os.getenv(px + "RESOURCE_GROUPS", "*").strip()
        rgs = "*" if rgs_raw == "*" else _coerce_list(rgs_raw)
        rtypes_env = os.getenv(px + "RESOURCE_TYPES", "").strip()
        rtypes = _coerce_list(rtypes_env) if rtypes_env else None
        registry_path = os.getenv(px + "REGISTRY_PATH", "").strip()

        missing = [k for k, v in {
            "TENANT_ID": tenant_id, "CLIENT_ID": client_id,
            "CLIENT_SECRET": client_secret, "REGISTRY_PATH": registry_path
        }.items() if not v]
        if missing:
            raise ValueError(f"Missing env for {px}[{', '.join(missing)}]")
        if not subs:
            raise ValueError(f"{px}SUBSCRIPTIONS must have at least one subscription id")

        tenants.append({
            "tenant_id": tenant_id,
            "client_id": client_id,
            "client_secret": client_secret,
            "subscriptions": subs,
            "resource_groups": rgs,
            "resource_types": rtypes,
            "registry_path": registry_path,
        })

    return {
        "tenants": tenants,
        "start_date": start_date,
        "end_date": end_date,
        "interval_minutes": interval_minutes,
        "timezone": timezone,
        "output": output,
        "per_tenant_output": per_tenant_output,
        "list_services_only": list_services_only,   # NEW
    }


# =========================
# Per-tenant runs
# =========================

def _credential_for_tenant(tenant_id: str, client_id: str, client_secret: str) -> ClientSecretCredential:
    return ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

def _run_for_tenant_services_listing(
    tenant_cfg: Dict[str, Any]
) -> pd.DataFrame:
    """
    NEW: Only list services (no metrics) for this tenant and return a DataFrame.
    """
    cred = _credential_for_tenant(tenant_cfg["tenant_id"], tenant_cfg["client_id"], tenant_cfg["client_secret"])
    df = list_all_services_for_tenant(
        credential=cred,
        subscriptions=tenant_cfg["subscriptions"],
        resource_groups=tenant_cfg["resource_groups"],
    )
    return df


def _run_for_tenant_mgmt(
    tenant_cfg: Dict[str, Any],
    start_date: str,
    end_date: str,
    interval_minutes: int,
    timezone: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    cred = _credential_for_tenant(tenant_cfg["tenant_id"], tenant_cfg["client_id"], tenant_cfg["client_secret"])
    registry = _load_service_registry(tenant_cfg["registry_path"])

    inventory = discover_resources_by_rg(
        credential=cred,
        tenant_id=tenant_cfg["tenant_id"],
        subscriptions=tenant_cfg["subscriptions"],
        resource_groups=tenant_cfg["resource_groups"],
        resource_types=tenant_cfg.get("resource_types"),
    )
    if not inventory:
        return pd.DataFrame(), pd.DataFrame([{"tenant_id": tenant_cfg["tenant_id"], "stage": "discover", "warning": "No resources discovered"}])

    plan = build_query_plan(inventory, registry)
    if not plan:
        return pd.DataFrame(), pd.DataFrame([{"tenant_id": tenant_cfg["tenant_id"], "stage": "plan", "warning": "No resources match registry"}])

    monitor_clients_by_sub: Dict[str, MonitorManagementClient] = {sub: MonitorManagementClient(cred, sub) for sub in tenant_cfg["subscriptions"]}

    return list_inventory_with_metrics_mgmt(
        monitor_clients_by_sub=monitor_clients_by_sub,
        plan=plan,
        start_date=start_date,
        end_date=end_date,
        interval_minutes=interval_minutes,
        timezone=timezone,
    )


# =========================
# Main
# =========================

def main():
    parser = argparse.ArgumentParser(description="Azure metrics collector via MonitorManagementClient (mgmt-plane), multi-tenant from .env. Also supports services listing only.")
    # No CLI flags needed; behavior toggled by env LIST_SERVICES_ONLY=true
    args = parser.parse_args()

    settings = _load_env_settings()

    if settings["list_services_only"]:
        # --- LIST SERVICES MODE ---
        for tenant_cfg in settings["tenants"]:
            df_services = _run_for_tenant_services_listing(tenant_cfg)
            out = f"./services_{tenant_cfg['tenant_id']}.csv"
            df_services.to_csv(out, index=False)
            print(f"[tenant {tenant_cfg['tenant_id']}] listed {len(df_services)} services → {out}")
        print("Done listing services.")
        return

    # --- METRICS MODE ---
    all_metrics: List[pd.DataFrame] = []
    all_notes: List[pd.DataFrame] = []

    for tenant_cfg in settings["tenants"]:
        df_metrics, df_notes = _run_for_tenant_mgmt(
            tenant_cfg=tenant_cfg,
            start_date=settings["start_date"],
            end_date=settings["end_date"],
            interval_minutes=settings["interval_minutes"],
            timezone=settings["timezone"],
        )
        if not df_metrics.empty:
            all_metrics.append(df_metrics)
        if not df_notes.empty:
            all_notes.append(df_notes)

        if settings.get("per_tenant_output", True):
            base_out = f"./metrics_{tenant_cfg['tenant_id']}.csv"
            base, ext = os.path.splitext(base_out)
            (df_metrics if not df_metrics.empty else pd.DataFrame()).to_csv(base_out, index=False)
            (df_notes if not df_notes.empty else pd.DataFrame()).to_csv(f"{base}__notes{ext or '.csv'}", index=False)
            print(f"[tenant {tenant_cfg['tenant_id']}] wrote {base_out} and {base}__notes{ext or '.csv'}")

    if settings.get("output") and not settings.get("per_tenant_output", True):
        combined_metrics = pd.concat(all_metrics, ignore_index=True) if all_metrics else pd.DataFrame()
        combined_notes = pd.concat(all_notes, ignore_index=True) if all_notes else pd.DataFrame()
        base, ext = os.path.splitext(settings["output"])
        combined_metrics.to_csv(settings["output"], index=False)
        combined_notes.to_csv(f"{base}__notes{ext or '.csv'}", index=False)
        print(f"[combined] wrote {settings['output']} and {base}__notes{ext or '.csv'}")

    total_rows = sum(len(df) for df in all_metrics)
    print(f"Done. Tenants processed: {len(settings['tenants'])}. Rows collected: {total_rows:,}.")


if __name__ == "__main__":
    main()
