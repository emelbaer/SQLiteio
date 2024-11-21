import asyncio
from sqliteio.core import SQLiteSocket

async def start_client():
    client = SQLiteSocket(db_path="example.db", peer_id="client1")

    @client.on("on_connection")
    async def handle_connection(data):
        print(f"Connected to server: {data['peer']} with UUID {data['uuid']}")

    @client.on("on_disconnect")
    async def handle_disconnect(data):
        print(f"Disconnected from server: {data['reason']}")
        # Attempt to reconnect (or handle disconnection)
        await client.reconnect()

    # Start the client
    await client.start()

    # Simulate client running (can be replaced with actual logic)
    await asyncio.sleep(20)

    # Gracefully stop the client after the simulation
    await client.stop()

if __name__ == "__main__":
    asyncio.run(start_client())
