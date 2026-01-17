#!/usr/bin/env python3
"""
LiveTable Reactive Propagation Demo

Visualizes how changes propagate from a base table to multiple views:
                    ┌─> FilterView (high-value only)
    Table (sales) ──┼─> SortedView (ranked by amount)
                    └─> AggregateView (totals by region)

This demonstrates LiveTable's incremental update capabilities where
views are automatically registered and can be updated via tick().

Usage:
    python3 demo_reactive_propagation.py         # Step-by-step mode
    python3 demo_reactive_propagation.py --tick  # Use table.tick() to sync all
    python3 demo_reactive_propagation.py --slow  # Slower animation
"""

import livetable
import time
import sys


# ANSI escape codes for terminal formatting
class Term:
    CLEAR = "\033[2J"
    HOME = "\033[H"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    CYAN = "\033[36m"
    RED = "\033[31m"
    RESET = "\033[0m"


def clear_screen():
    """Clear terminal and move cursor to top-left."""
    print(Term.CLEAR + Term.HOME, end="", flush=True)


def format_money(amount):
    """Format a number as currency."""
    return f"${amount:,.0f}"


def render_table_box(headers, rows, highlight_idx=None, width=32):
    """Render a table as an ASCII box with optional row highlight."""
    lines = []
    lines.append(f"  {'':2}{'':2}".join(f"{h:<9}" for h in headers))
    lines.append("─" * (width - 4))

    for i, row in enumerate(rows):
        row_str = "  ".join(f"{str(v):<9}" for v in row)
        if i == highlight_idx:
            row_str = f"{Term.GREEN}{row_str}{Term.RESET}"
        lines.append(row_str)

    return lines


def get_table_rows(table_or_view, columns):
    """Extract rows from table/view as list of tuples."""
    rows = []
    for i in range(len(table_or_view)):
        row = table_or_view[i]
        rows.append(tuple(row.get(c) for c in columns))
    return rows


def render_state(sales, filtered, sorted_view, aggregates,
                 new_row=None, sync_step=None, highlight_new=False):
    """Render full dashboard state to terminal."""
    clear_screen()

    print("=" * 78)
    print(f"  {Term.BOLD}LIVETABLE REACTIVE PROPAGATION DEMO{Term.RESET}")
    print(f"  {Term.DIM}One table, three views - all updated incrementally{Term.RESET}")
    print("=" * 78)
    print()

    # Get data from all views
    sales_rows = get_table_rows(sales, ["region", "product", "amount"])
    filter_rows = get_table_rows(filtered, ["region", "product", "amount"])
    sorted_rows = get_table_rows(sorted_view, ["region", "product", "amount"])
    agg_rows = get_table_rows(aggregates, ["region", "total", "count"])

    # Format amounts as currency
    sales_fmt = [(r, p, format_money(a)) for r, p, a in sales_rows]
    filter_fmt = [(r, p, format_money(a)) for r, p, a in filter_rows]
    sorted_fmt = [(r, p, format_money(a)) for r, p, a in sorted_rows]
    agg_fmt = [(r, format_money(t), int(c)) for r, t, c in agg_rows]

    # [1] Base table
    print(f"  {Term.CYAN}[1] BASE TABLE{Term.RESET} (sales) - {len(sales)} rows")
    print(f"  {Term.DIM}All incoming sales transactions{Term.RESET}")
    print()
    highlight_idx = len(sales) - 1 if highlight_new else None
    for line in render_table_box(["region", "product", "amount"], sales_fmt, highlight_idx):
        print(f"      {line}")
    print()

    # Show new row being added and sync progress
    if new_row:
        amount_str = format_money(new_row['amount'])
        passes_filter = new_row['amount'] > 500
        filter_note = "" if passes_filter else f" {Term.DIM}(filtered out){Term.RESET}"
        print(f"  {Term.YELLOW}>>> NEW:{Term.RESET} {new_row['region']} {new_row['product']} {amount_str}{filter_note}")
        print()

        steps = [
            ("FilterView", f"amount > $500"),
            ("SortedView", "by amount desc"),
            ("AggregateView", "sum by region"),
        ]
        for i, (name, desc) in enumerate(steps):
            if sync_step is not None and i < sync_step:
                print(f"      {Term.GREEN}[OK]{Term.RESET} {name} synced ({desc})")
            elif sync_step is not None and i == sync_step:
                print(f"      {Term.YELLOW}[..]{Term.RESET} Syncing {name}... ({desc})")
            else:
                print(f"      {Term.DIM}[ ] {name} ({desc}){Term.RESET}")
        print()

    # [2] FilterView
    print(f"  {Term.CYAN}[2] FILTER VIEW{Term.RESET} (amount > $500) - {len(filtered)} rows")
    print(f"  {Term.DIM}Only high-value sales pass through{Term.RESET}")
    print()
    for line in render_table_box(["region", "product", "amount"], filter_fmt):
        print(f"      {line}")
    print()

    # [3] SortedView
    print(f"  {Term.CYAN}[3] SORTED VIEW{Term.RESET} (by amount descending) - {len(sorted_view)} rows")
    print(f"  {Term.DIM}Filtered sales ranked by value{Term.RESET}")
    print()
    for line in render_table_box(["region", "product", "amount"], sorted_fmt):
        print(f"      {line}")
    print()

    # [4] AggregateView
    print(f"  {Term.CYAN}[4] AGGREGATE VIEW{Term.RESET} (grouped by region) - {len(aggregates)} groups")
    print(f"  {Term.DIM}Regional totals updated incrementally{Term.RESET}")
    print()
    for line in render_table_box(["region", "total", "count"], agg_fmt):
        print(f"      {line}")
    print()

    print("=" * 78)


def main():
    # Check for command-line options
    slow = "--slow" in sys.argv
    use_tick = "--tick" in sys.argv
    delay = 1.0 if slow else 0.4

    # Create schema for sales data
    schema = livetable.Schema([
        ("region", livetable.ColumnType.STRING, False),
        ("product", livetable.ColumnType.STRING, False),
        ("amount", livetable.ColumnType.FLOAT64, False),
    ])

    # Create base table
    sales = livetable.Table("sales", schema)

    # Create multiple views from the same base table
    # Views created with filter/sort/group_by are auto-registered for tick()
    filtered = sales.filter(lambda r: r["amount"] > 500)
    sorted_view = sales.sort("amount", descending=True)
    aggregates = sales.group_by("region", agg=[
        ("total", "amount", "sum"),
        ("count", "amount", "count"),
    ])

    # Load initial data
    initial_sales = [
        {"region": "West", "product": "Widget", "amount": 250.0},
        {"region": "East", "product": "Gadget", "amount": 1200.0},
        {"region": "West", "product": "Premium", "amount": 800.0},
        {"region": "North", "product": "Basic", "amount": 150.0},
    ]

    for row in initial_sales:
        sales.append_row(row)

    # Initial sync - tick() updates all registered views at once
    sales.tick()

    # Show initial state
    render_state(sales, filtered, sorted_view, aggregates)
    print()
    mode_str = "tick() mode" if use_tick else "step-by-step mode"
    print(f"  {Term.DIM}Initial state loaded ({mode_str}). Press Enter to start streaming...{Term.RESET}")
    if use_tick:
        print(f"  {Term.DIM}Using table.tick() to update all {sales.registered_view_count()} views at once{Term.RESET}")
    input()

    # New sales to stream - demonstrates various scenarios
    new_sales = [
        # Passes filter, inserts in middle of sorted order
        {"region": "North", "product": "Premium", "amount": 950.0},
        # Below threshold - won't appear in filter/sorted views
        {"region": "East", "product": "Widget", "amount": 300.0},
        # New region! Creates new group in aggregates, becomes #1 in sorted
        {"region": "South", "product": "Deluxe", "amount": 1500.0},
        # New top seller - becomes #1 in sorted view
        {"region": "West", "product": "Ultra", "amount": 2000.0},
    ]

    for row in new_sales:
        # Step 1: Add row to base table
        sales.append_row(row)
        render_state(sales, filtered, sorted_view, aggregates,
                     new_row=row, sync_step=0, highlight_new=True)
        time.sleep(delay)

        if use_tick:
            # tick() mode: Update all views at once
            sales.tick()
            render_state(sales, filtered, sorted_view, aggregates,
                         new_row=row, sync_step=3, highlight_new=True)
            time.sleep(delay * 1.5)
        else:
            # Step-by-step mode: Show each view updating individually
            # Step 2: Sync FilterView
            filtered.refresh()
            render_state(sales, filtered, sorted_view, aggregates,
                         new_row=row, sync_step=1, highlight_new=True)
            time.sleep(delay)

            # Step 3: Sync SortedView
            sorted_view.refresh()
            render_state(sales, filtered, sorted_view, aggregates,
                         new_row=row, sync_step=2, highlight_new=True)
            time.sleep(delay)

            # Step 4: Sync AggregateView
            aggregates.refresh()
            render_state(sales, filtered, sorted_view, aggregates,
                         new_row=row, sync_step=3, highlight_new=True)
            time.sleep(delay * 1.5)  # Pause a bit longer between rows

    # Final state
    render_state(sales, filtered, sorted_view, aggregates)
    print()
    print(f"  {Term.GREEN}Demo complete!{Term.RESET}")
    print()
    print(f"  {Term.BOLD}What you just saw:{Term.RESET}")
    print(f"    - {len(new_sales)} new sales streamed into base table")
    if use_tick:
        print(f"    - table.tick() updated all {sales.registered_view_count()} views at once")
    else:
        print(f"    - Each .refresh() processed changes individually")
    print(f"    - FilterView excluded sales below $500 threshold")
    print(f"    - SortedView maintained descending order automatically")
    print(f"    - AggregateView updated regional totals incrementally")
    print(f"    - New 'South' region appeared when first sale arrived")
    print()
    if not use_tick:
        print(f"  {Term.DIM}Tip: Run with --tick flag to see table.tick() mode{Term.RESET}")
        print()


if __name__ == "__main__":
    main()
