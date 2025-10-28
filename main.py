# pip install azure-identity azure-mgmt-resource azure-mgmt-monitor python-dotenv pandas

import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

import pandas as pd
from azure.identity import ClientSecretCredential
from azure.mgmt.resource.subscriptions import SubscriptionClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.monitor import MonitorManagementClient

load_dotenv(override=True)

class MultiTenantAuth:
    """Minimal multi-tenant connection manager (.env + env vars)."""
    def __init__(self, mode, tenants):
        self._mode = mode
        self._creds = {}
        for tc in tenants:
            if mode == "secret":
                secret = os.getenv(tc["env_secret_var"])
                if not secret:
                    raise EnvironmentError(f"Missing env var {tc['env_secret_var']}")
                cred = ClientSecretCredential(
                    tenant_id=tc["tenant_id"],
                    client_id=tc["client_id"],
                    client_secret=secret,
                )
            else:
                raise ValueError("Unsupported mode")
            self._creds[tc["tenant_id"]] = cred

    def credential(self, tenant_id):
        return self._creds.get(tenant_id)

    def tenants(self):
        return list(self._creds.keys())

def fetch_metrics_for_period(
    monitor_client: MonitorManagementClient,
    resource_id: str,
    metric_names: str,
    start_date: str,        # format "YYYY-MM-DD"
    end_date: str,          # format "YYYY-MM-DD"
    interval_minutes: int
) -> dict:
    """Fetch metrics for given date-range (00:00 start_date to 23:59 end_date) with given interval in minutes."""
    start_dt = datetime.fromisoformat(start_date).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    end_dt   = datetime.fromisoformat(end_date).replace(hour=23, minute=59, second=59, microsecond=0, tzinfo=timezone.utc)
    start_str = start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_str   = end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    timespan  = f"{start_str}/{end_str}"
    interval_iso = f"PT{interval_minutes}M"

    response = monitor_client.metrics.list(
        resource_id,
        timespan=timespan,
        interval=interval_iso,
        metricnames=metric_names,
        aggregation="Average"
    )

    out = {}
    for metric in response.value:
        name = metric.name.value.lower()
        pts  = []
        for ts in metric.timeseries:
            for dp in ts.data:
                if hasattr(dp, "average") and dp.average is not None:
                    pts.append({"time": dp.time_stamp.isoformat(), "value": dp.average})
        out[name] = pts
    return out

def list_inventory_with_db_metrics(tenant_id, credential, start_date, end_date, interval_minutes, output_rows):
    sub_client = SubscriptionClient(credential)
    for sub in sub_client.subscriptions.list():
        sub_id   = sub.subscription_id
        sub_name = getattr(sub, "display_name", sub_id)
        print(f"\nTenant: {tenant_id} | Subscription: {sub_name} ({sub_id})")

        rg_client      = ResourceManagementClient(credential, sub_id)
        monitor_client = MonitorManagementClient(credential, sub_id)

        for rg in rg_client.resource_groups.list():
            for res in rg_client.resources.list_by_resource_group(rg.name):
                rtype = (res.type or "").lower()
                loc   = getattr(res, "location", "")
                print(f"    - {res.type} / {res.name} ({loc})")

                # Determine metric names by resource type
                if rtype == "microsoft.sql/servers/databases":
                    metric_names = "dtu_consumption_percent,cpu_percent"
                elif rtype in ("microsoft.sql/managedinstances", "microsoft.sql/managedinstances/databases"):
                    metric_names = "sql_instance_cpu_percent,sql_instance_memory_percent"
                elif rtype == "microsoft.compute/virtualmachines":
                    metric_names = "percentage_cpu,available_memory_bytes"
                else:
                    continue

                data = fetch_metrics_for_period(
                    monitor_client,
                    res.id,
                    metric_names,
                    start_date,
                    end_date,
                    interval_minutes
                )

                for metric_name, pts in data.items():
                    if pts:
                        for pt in pts:
                            output_rows.append({
                                "tenant_id":       tenant_id,
                                "subscription_id": sub_id,
                                "subscription_name": sub_name,
                                "resource_group":   rg.name,
                                "resource_id":      res.id,
                                "resource_type":    res.type,
                                "location":         loc,
                                "metric_name":      metric_name,
                                "timestamp":        pt["time"],
                                "value":            pt["value"]
                            })

def main():
    tenants = [
        {
            "tenant_id":       os.getenv("TENANT1_ID"),
            "client_id":       os.getenv("TENANT1_CLIENT_ID"),
            "env_secret_var":  "TENANT1_SP_SECRET"
        }
    ]
    auth = MultiTenantAuth(mode="secret", tenants=tenants)
    print("Connected tenants:")
    for tenant_id in auth.tenants():
        cred = auth.credential(tenant_id)
        print(f" - {tenant_id}: credential loaded -> {type(cred).__name__}")
    all_rows = []
    for tenant_id in auth.tenants():
        cred = auth.credential(tenant_id)
        try:
            list_inventory_with_db_metrics(
                tenant_id=tenant_id,
                credential=cred,
                start_date="2025-10-01",
                end_date="2025-10-28",
                interval_minutes=5,
                output_rows=all_rows
            )
        except Exception as e:
            print(f"[WARN] Tenant {tenant_id}: {e}")

    df = pd.DataFrame(all_rows)
    csv_file = f"metrics_export_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(csv_file, index=False, encoding='utf-8')
    print(f"Saved metrics to {csv_file}")

if __name__ == "__main__":
    main()
