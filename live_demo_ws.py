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
import random
from datetime import datetime

# Sample data for generating random entries
NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack"]
DEPARTMENTS = ["Engineering", "Sales", "Marketing", "HR", "Finance", "Operations", "Support"]
CITIES = ["New York", "San Francisco", "London", "Tokyo", "Berlin", "Sydney", "Toronto"]

def create_random_row():
    """Generate a random person record."""
    return {
        "name": random.choice(NAMES),
        "age": random.randint(22, 65),
        "department": random.choice(DEPARTMENTS),
        "salary": random.randint(50000, 150000),
        "city": random.choice(CITIES),
    }

async def live_demo():
    uri = "ws://localhost:8080/ws"

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
        row_count = len(msg.get('rows', []))

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
                    row_count += 1
                    print(f"[{timestamp}] #{iteration:3d} ➕ INSERT: {new_row['name']}, {new_row['age']}, {new_row['department']} (Total: {row_count})")

                elif operation == "update" and row_count > 0:
                    row_idx = random.randint(0, row_count - 1)
                    column = random.choice(["age", "salary", "department", "city"])

                    if column == "age":
                        new_value = random.randint(22, 65)
                    elif column == "salary":
                        new_value = random.randint(50000, 150000)
                    elif column == "department":
                        new_value = random.choice(DEPARTMENTS)
                    else:  # city
                        new_value = random.choice(CITIES)

                    message = {
                        "type": "UpdateCell",
                        "table_name": "demo",
                        "row_index": row_idx,
                        "column": column,
                        "value": new_value
                    }
                    await websocket.send(json.dumps(message))
                    print(f"[{timestamp}] #{iteration:3d} ✏️  UPDATE: Row {row_idx}, {column} = {new_value}")

                elif operation == "delete" and row_count > 1:  # Keep at least 1 row
                    row_idx = random.randint(0, row_count - 1)
                    message = {
                        "type": "DeleteRow",
                        "table_name": "demo",
                        "row_index": row_idx
                    }
                    await websocket.send(json.dumps(message))
                    row_count -= 1
                    print(f"[{timestamp}] #{iteration:3d} ❌ DELETE: Row {row_idx} (Total: {row_count})")

                # Read server response (non-blocking)
                try:
                    response = await asyncio.wait_for(websocket.recv(), timeout=0.1)
                    # Just consume the response, don't print it
                except asyncio.TimeoutError:
                    pass

                # Sleep between operations (2 ops/second)
                await asyncio.sleep(0.5)

        except KeyboardInterrupt:
            print()
            print("=" * 60)
            print("👋 Demo stopped by user")
            print(f"Final estimated row count: {row_count}")
            print("=" * 60)

if __name__ == "__main__":
    try:
        asyncio.run(live_demo())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
