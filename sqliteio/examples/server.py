import asyncio
from sqliteio.core import SQLiteSocket
import signal
import sys

async def main():
    server = SQLiteSocket("example.db", peer_id="server1", is_server=True)

    async def handle_connection(data):
        print(f"Client connected: {data}")

    async def handle_disconnection(data):
        print(f"Client disconnected: {data}")

    async def handle_message(data):
        print(f"Received message: {data}")

    server.on("onconnection", handle_connection)
    server.on("ondisconnection", handle_disconnection)
    server.on("message", handle_message)

    await server.start()
    print("Server is running. Press CTRL+C to stop.")

    # Keep the server running indefinitely
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        print("Shutting down server...")
        await server.stop()

def shutdown(loop, task):
    """Gracefully shutdown the event loop."""
    print("Received termination signal. Shutting down...")
    task.cancel()
    loop.stop()

if __name__ == "__main__":
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # Create the server task
    server_task = loop.create_task(main())

    # Add signal handler for graceful shutdown
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown, loop, server_task)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        # Ensure all tasks are completed before closing the loop
        loop.run_until_complete(asyncio.gather(*asyncio.all_tasks(loop), return_exceptions=True))
        loop.close()
        print("Event loop closed.")
