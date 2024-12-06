# SQLiteio
A socetio api style library but not made with websockets or long polling, it's build on a common SQLite database file.

Use Example:
```python
import asyncio
from sqliteio.core import SQLiteSocket

async def start_client():
  # create a SQLiteSocket
  client = SQLiteSocket(db_path="example.db", peer_id="Test Client")

  # register an event
  @client.on("connection")
  async def handle_connection(data):
    print(f"Connected to server: {data['peer']} with UUID {data['uuid']}")

  # register an custom event
  @client.on("message")
  async def handle_incomming_message(ctx,data):
    print(f"Message received: {ctx=}, {data}")

  # Start the client
  await client.start()

  # Emit a message (custom event) to the server
  await client.emit("message", {"text": f"Hello from {clientName}"})



  # Simulate client running (can be replaced with actual logic)
  await asyncio.sleep(20)

  # Gracefully stop the client after the simulation
  await client.stop()

if __name__ == "__main__":
  asyncio.run(start_client())
```
---
**WARNING**

This package is currently under construction.

---
