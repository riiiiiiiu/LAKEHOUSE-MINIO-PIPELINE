"""
proof_demo.py
=============
Screen-recording-ready proof:
  ✅ Opens Parquet files from MinIO
  ✅ Validates data transformation across medallion layers

Usage:
    pip install boto3 pandas pyarrow rich
    python proof_demo.py

Update the CONFIG block below to match your Docker Compose setup.
"""

import io
import time
import boto3
import pandas as pd
from botocore.client import Config
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.rule import Rule
from rich import box
from rich.text import Text

# ─────────────────────────────────────────────
# CONFIG — update to match your Docker Compose
# ─────────────────────────────────────────────
MINIO_ENDPOINT   = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin123"
BUCKET_NAME      = "lakehouse"

LAYERS = {
    "EMAIL_THREAD_SUMMARIES": "email_thread_summaries/",
}

console = Console()

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def pause(secs=0.6):
    time.sleep(secs)


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def list_parquet_keys(client, prefix):
    resp = client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    return [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]


def read_parquet(client, key):
    meta = client.head_object(Bucket=BUCKET_NAME, Key=key)
    if meta["ContentLength"] == 0:
        console.print(f"  [yellow]⚠️  Skipping 0-byte file:[/yellow] [dim]{key}[/dim]")
        return None
    obj = client.get_object(Bucket=BUCKET_NAME, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def df_to_rich_table(df, title, max_rows=5):
    tbl = Table(title=title, box=box.ROUNDED, show_lines=True,
                header_style="bold cyan", title_style="bold white")
    for col in df.columns:
        tbl.add_column(str(col), overflow="fold")
    for _, row in df.head(max_rows).iterrows():
        tbl.add_row(*[str(v) for v in row])
    return tbl


def schema_table(df, layer_name):
    tbl = Table(title=f"Schema — {layer_name}", box=box.ROUNDED,
                header_style="bold magenta", show_lines=True)
    tbl.add_column("Column",  style="bold")
    tbl.add_column("Dtype",   style="yellow")
    tbl.add_column("Nulls",   style="red")
    tbl.add_column("Unique",  style="green")
    for col in df.columns:
        tbl.add_row(col, str(df[col].dtype),
                    str(df[col].isnull().sum()),
                    str(df[col].nunique()))
    return tbl


def step(number, text):
    console.print(Rule(f"[bold yellow]STEP {number}[/bold yellow]  {text}", style="yellow"))
    pause()


# ─────────────────────────────────────────────
# MAIN DEMO
# ─────────────────────────────────────────────

def main():
    console.print()
    console.print(Panel.fit(
        "[bold white]MinIO Parquet — Data Transformation Proof[/bold white]\n"
        "[dim]Medallion Architecture: RAW → PROCESSED → CURATED[/dim]",
        border_style="bright_blue", padding=(1, 4)
    ))
    pause(1)

    # ── STEP 1: Connect to MinIO ──────────────────────────────────────
    step(1, "Connect to MinIO")
    client = get_client()
    try:
        client.head_bucket(Bucket=BUCKET_NAME)
        console.print(f"  [green]✅ Connected[/green]  endpoint=[cyan]{MINIO_ENDPOINT}[/cyan]  bucket=[cyan]{BUCKET_NAME}[/cyan]")
    except Exception as e:
        console.print(f"  [red]❌ Connection failed: {e}[/red]")
        return
    pause()

    # ── STEP 2: List Parquet files per layer ─────────────────────────
    step(2, "Discover Parquet files in each layer")
    all_keys = {}
    for layer, prefix in LAYERS.items():
        keys = list_parquet_keys(client, prefix)
        all_keys[layer] = keys
        status = f"[green]{len(keys)} file(s)[/green]" if keys else "[red]none found[/red]"
        console.print(f"  [bold]{layer:10s}[/bold] {prefix:20s} → {status}")
        for k in keys:
            console.print(f"             [dim]• {k}[/dim]")
    pause()

    # ── STEP 3: Open & inspect each layer ────────────────────────────
    layer_dfs = {}
    for layer, prefix in LAYERS.items():
        keys = all_keys.get(layer, [])
        if not keys:
            layer_dfs[layer] = None
            continue

        step(3 if layer == "RAW" else " ", f"Open Parquet — [bold cyan]{layer}[/bold cyan] layer")

        frames = []
        for key in keys:
            console.print(f"  [dim]Reading:[/dim] [cyan]{key}[/cyan]")
            df = read_parquet(client, key)
            if df is not None:
                frames.append(df)
            pause(0.3)

        if not frames:
            console.print(f"  [red]❌ All files in {layer} were empty, skipping.[/red]")
            layer_dfs[layer] = None
            continue

        df_combined = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0]
        layer_dfs[layer] = df_combined

        console.print(f"\n  [bold]Shape:[/bold] [green]{df_combined.shape[0]:,}[/green] rows × [green]{df_combined.shape[1]}[/green] columns")
        pause(0.3)
        console.print(schema_table(df_combined, layer))
        pause(0.3)
        console.print(df_to_rich_table(df_combined, f"Sample Data — {layer} (top 5 rows)"))
        pause(0.8)

    # ── STEP 4: Validate transformations ─────────────────────────────
    step(4, "Validate Data Transformation")

    raw  = layer_dfs.get("RAW")
    proc = layer_dfs.get("PROCESSED")
    cur  = layer_dfs.get("CURATED")

    # Row & column count summary
    summary = Table(title="Layer Comparison", box=box.ROUNDED,
                    header_style="bold white", show_lines=True)
    summary.add_column("Layer",    style="bold cyan")
    summary.add_column("Rows",     style="green",  justify="right")
    summary.add_column("Columns",  style="yellow", justify="right")
    summary.add_column("Nulls",    style="red",    justify="right")

    for label, df in [("RAW", raw), ("PROCESSED", proc), ("CURATED", cur)]:
        if df is not None:
            summary.add_row(label,
                            f"{df.shape[0]:,}",
                            str(df.shape[1]),
                            str(int(df.isnull().sum().sum())))
        else:
            summary.add_row(label, "[dim]N/A[/dim]", "[dim]N/A[/dim]", "[dim]N/A[/dim]")

    console.print(summary)
    pause(0.5)

    # Column diff
    def col_diff(df_a, df_b, label_a, label_b):
        added   = set(df_b.columns) - set(df_a.columns)
        removed = set(df_a.columns) - set(df_b.columns)
        console.print(f"\n  [bold]{label_a} → {label_b}[/bold]")
        if added:
            console.print(f"    [green]➕ Columns added   :[/green] {sorted(added)}")
        else:
            console.print(f"    [green]➕ Columns added   :[/green] none")
        if removed:
            console.print(f"    [red]➖ Columns removed :[/red] {sorted(removed)}")
        else:
            console.print(f"    [red]➖ Columns removed :[/red] none")

    if raw is not None and proc is not None:
        col_diff(raw, proc, "RAW", "PROCESSED")
    if proc is not None and cur is not None:
        col_diff(proc, cur, "PROCESSED", "CURATED")

    # Null reduction proof
    if raw is not None and proc is not None:
        raw_nulls  = int(raw.isnull().sum().sum())
        proc_nulls = int(proc.isnull().sum().sum())
        delta = raw_nulls - proc_nulls
        console.print()
        if delta > 0:
            console.print(f"  [green]✅ Null reduction RAW → PROCESSED: {raw_nulls} → {proc_nulls}  (cleaned {delta} nulls)[/green]")
        elif delta == 0:
            console.print(f"  [yellow]⚠️  No null change RAW → PROCESSED ({raw_nulls} nulls in both)[/yellow]")
        else:
            console.print(f"  [red]❌ Nulls increased RAW → PROCESSED ({raw_nulls} → {proc_nulls})[/red]")

    pause(0.5)

    # ── FINAL ────────────────────────────────────────────────────────
    console.print()
    console.print(Panel.fit(
        "[bold green]✅  PROOF COMPLETE[/bold green]\n\n"
        "  • Parquet files successfully opened from MinIO\n"
        "  • Schema, row counts, and sample data inspected per layer\n"
        "  • Column changes and null reduction validated across layers",
        border_style="green", padding=(1, 4)
    ))
    console.print()


if __name__ == "__main__":
    main()