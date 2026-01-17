#!/usr/bin/env python3
"""
LiveTable Streaming Publisher

Generates periodic sales data and publishes to the WebSocket server.
Run alongside the frontend to see real-time cascading view updates.

Usage:
    python3 streaming_publisher.py              # Default: 1 sale every 2 seconds
    python3 streaming_publisher.py --fast       # 1 sale every 0.5 seconds
    python3 streaming_publisher.py --slow       # 1 sale every 5 seconds
"""

import asyncio
import json
import random
import sys
import websockets

# WebSocket server URL
WS_URL = "ws://127.0.0.1:8080/ws"

# Sample data for generating sales
REGIONS = ["West", "East", "North", "South", "Central"]
PRODUCTS = ["Widget", "Gadget", "Premium", "Basic", "Deluxe", "Ultra", "Pro", "Lite"]


def generate_sale():
    """Generate a random sale row."""
    return {
        "region": random.choice(REGIONS),
        "product": random.choice(PRODUCTS),
        "amount": random.randint(100, 2500),
    }


async def publish_sales(interval: float):
    """Connect to WebSocket and publish sales at regular intervals."""
    print(f"Connecting to {WS_URL}...")

    try:
        async with websockets.connect(WS_URL) as ws:
            print(f"Connected! Publishing sales every {interval}s")
            print("Press Ctrl+C to stop\n")

            # Subscribe to the demo table
            subscribe_msg = json.dumps({
                "type": "Subscribe",
                "table_name": "demo"
            })
            await ws.send(subscribe_msg)

            sale_count = 0
            while True:
                # Generate a new sale
                sale = generate_sale()
                sale_count += 1

                # Create InsertRow message
                msg = json.dumps({
                    "type": "InsertRow",
                    "table_name": "demo",
                    "row": sale
                })

                await ws.send(msg)

                # Format for display
                passes_filter = sale["amount"] > 500
                filter_status = "PASS" if passes_filter else "skip"
                print(f"[{sale_count:3}] {sale['region']:8} {sale['product']:8} ${sale['amount']:,}  [{filter_status}]")

                await asyncio.sleep(interval)

    except ConnectionRefusedError:
        print("Error: Could not connect to WebSocket server")
        print("Make sure the server is running: cd impl && cargo run --bin livetable-server --features server")
        sys.exit(1)
    except websockets.exceptions.ConnectionClosed:
        print(f"\n\nConnection closed after publishing {sale_count} sales")
    except KeyboardInterrupt:
        print(f"\n\nStopped after publishing {sale_count} sales")


def main():
    # Parse command line options
    if "--fast" in sys.argv:
        interval = 0.5
    elif "--slow" in sys.argv:
        interval = 5.0
    else:
        interval = 2.0

    print("=" * 50)
    print("  LIVETABLE STREAMING PUBLISHER")
    print("=" * 50)
    print()

    asyncio.run(publish_sales(interval))


if __name__ == "__main__":
    main()
