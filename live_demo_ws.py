#!/usr/bin/env python3
"""
Live Table Demo - WebSocket Client

This script connects to the LiveTable WebSocket server and continuously updates the table.
All changes are broadcast to connected browser clients in real-time!

Usage:
    python live_demo_ws.py

Then open http://localhost:5173 in multiple browser tabs to see synchronized updates!
"""

import asyncio
import websockets
import json
import os
import random
from datetime import datetime

# WebSocket server URL
WS_URL = os.environ.get("LIVETABLE_WS_URL", "ws://localhost:8080/ws")

# Sample data for generating sales entries
REGIONS = ["West", "East", "North", "South", "Central"]
PRODUCTS = ["Widget", "Gadget", "Premium", "Basic", "Deluxe", "Ultra", "Pro", "Lite"]

def create_random_row():
    """Generate a random sale record matching the server demo table."""
    return {
        "region": random.choice(REGIONS),
        "product": random.choice(PRODUCTS),
        "amount": random.randint(100, 2500),
    }

async def live_demo():
    uri = WS_URL

    print("🚀 Starting Live Table Demo")
    print("=" * 60)
    print("Open http://localhost:5173 in your browser to watch updates!")
    print("Open multiple tabs to see real-time synchronization")
    print("=" * 60)
    print()

    async with websockets.connect(uri) as websocket:
        # Subscribe to the demo table
        await websocket.send(json.dumps({
            "type": "Subscribe",
            "table_name": "demo"
        }))

        # Query initial state
        await websocket.send(json.dumps({
            "type": "Query",
            "table_name": "demo"
        }))

        # Receive subscription confirmation and initial data
        response = await websocket.recv()
        msg = json.loads(response)
        print(f"📡 {msg['type']}: {msg}")

        response = await websocket.recv()
        msg = json.loads(response)
        print(f"📊 Initial table data: {len(msg.get('rows', []))} rows")
        print()

        iteration = 0
        row_ids = [row["row_id"] for row in msg.get("rows", [])]

        try:
            while True:
                iteration += 1
                timestamp = datetime.now().strftime("%H:%M:%S")

                # Decide what operation to perform
                operation = random.choices(
                    ["insert", "update", "delete"],
                    weights=[45, 35, 20],  # Favor inserts
                    k=1
                )[0]

                if operation == "insert":
                    new_row = create_random_row()
                    message = {
                        "type": "InsertRow",
                        "table_name": "demo",
                        "row": new_row
                    }
                    await websocket.send(json.dumps(message))
                    print(f"[{timestamp}] #{iteration:3d} ➕ INSERT: {new_row['region']}, {new_row['product']}, ${new_row['amount']:,}")

                elif operation == "update" and row_ids:
                    row_id = random.choice(row_ids)
                    column = random.choice(["region", "product", "amount"])

                    if column == "region":
                        new_value = random.choice(REGIONS)
                    elif column == "product":
                        new_value = random.choice(PRODUCTS)
                    else:
                        new_value = random.randint(100, 2500)

                    message = {
                        "type": "UpdateCell",
                        "table_name": "demo",
                        "row_id": row_id,
                        "column": column,
                        "value": new_value
                    }
                    await websocket.send(json.dumps(message))
                    print(f"[{timestamp}] #{iteration:3d} ✏️  UPDATE: Row ID {row_id}, {column} = {new_value}")

                elif operation == "delete" and len(row_ids) > 1:  # Keep at least 1 row
                    row_id = random.choice(row_ids)
                    message = {
                        "type": "DeleteRow",
                        "table_name": "demo",
                        "row_id": row_id
                    }
                    await websocket.send(json.dumps(message))
                    print(f"[{timestamp}] #{iteration:3d} ❌ DELETE: Row ID {row_id}")

                # Read and apply this connection's broadcast response.
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                    msg = json.loads(response)
                    if msg["type"] == "RowInserted":
                        row_ids.append(msg["row_id"])
                    elif msg["type"] == "RowDeleted":
                        row_ids = [existing for existing in row_ids if existing != msg["row_id"]]
                    elif msg["type"] == "TableData":
                        row_ids = [row["row_id"] for row in msg.get("rows", [])]
                    elif msg["type"] == "Error":
                        print(f"Server error: {msg['message']}")
                except asyncio.TimeoutError:
                    pass

                # Sleep between operations (2 ops/second)
                await asyncio.sleep(0.5)

        except KeyboardInterrupt:
            print()
            print("=" * 60)
            print("👋 Demo stopped by user")
            print(f"Final estimated row count: {len(row_ids)}")
            print("=" * 60)

if __name__ == "__main__":
    try:
        asyncio.run(live_demo())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
