import asyncio
from sqliteio.core import SQLiteSocket

async def start_client():
    clients=[]
    for clientName in ("client1","client2","client3"):
        client = SQLiteSocket(db_path="example.db", peer_id=clientName)
        clients.append(client)

        @client.on("on_connection")
        async def handle_connection(data):
            print(f"Connected to server: {data['peer']} with UUID {data['uuid']}")

        @client.on("on_disconnect")
        async def handle_disconnect(data):
            print(f"Disconnected from server: {data['reason']}")
            # Attempt to reconnect (or handle disconnection)
            await client.reconnect()

        @client.on("on_reconnect")
        async def handle_reconnect(data):
            print(f"Reconnected to server with UUID {data['uuid']}")

        @client.on("message")
        async def handle_incomming_message(ctx,data):
            print(f"Message received: {ctx=}, {data}")

        # Start the client
        await client.start()

        # Emit a message to the server
        await client.emit("message", {"text": f"Hello from {clientName}"})

    # Simulate client running (can be replaced with actual logic)
    await asyncio.sleep(20)

    # Gracefully stop the client after the simulation
    for client in clients:
        await client.stop()

if __name__ == "__main__":
    asyncio.run(start_client())
