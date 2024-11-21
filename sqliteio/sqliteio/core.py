from fnmatch import fnmatch
import inspect
import uuid
import asyncio
import time
from sqlalchemy import create_engine, Column, String, Integer, Table, MetaData, JSON
from sqlalchemy.orm import sessionmaker, scoped_session
from sqliteio.logger import setup_logger

# Set up the logger
logger = setup_logger(name="SQLiteIO", log_file="logs/sqliteio.log")


class SQLiteSocket:
    def __init__(self, db_path, peer_id, is_server=False, poll_interval=0.5, heartbeat_timeout=10):
        self.db_path = db_path
        self.peer_id = peer_id
        self.is_server = is_server
        self.poll_interval = poll_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.update_heartbeat = self.heartbeat_timeout // 2
        self.callbacks = {}
        self.rooms = {}
        self.running = False
        self.uuid = str(uuid.uuid4()).replace('-','')  # Unique identifier for each connection
        self.reconnect_interval = 5  # Reconnect interval for clients
        self.server_peer_id = None  # Identify the server for clients

        # SQLite DB setup
        self.engine = create_engine(f"sqlite:///{db_path}")
        self.metadata = MetaData()

        # Define the messages and sessions tables
        self.messages = Table(
            "messages",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("sender", String),
            Column("receiver", String, nullable=True),
            Column("room", String, nullable=True),
            Column("event", String),
            Column("data", JSON),
            Column("timestamp", Integer),
        )
        self.sessions = Table(
            "sessions",
            self.metadata,
            Column("peer", String, primary_key=True),
            Column("uuid", String),  # Unique ID per connection
            Column("last_heartbeat", Integer),
            Column("is_server", Integer),  # 1 for server, 0 for client
        )
        self.metadata.create_all(self.engine)
        self.Session = scoped_session(sessionmaker(bind=self.engine))

    async def _check_duplicate_connections(self):
        """Check for duplicate connections and handle conflicts."""
        with self.Session() as session:
            existing = session.execute(
                self.sessions.select().where(self.sessions.c.peer == self.peer_id)
            ).fetchone()

            if existing:
                last_heartbeat = existing[2]  # 'last_heartbeat' column
                if int(time.time()) - last_heartbeat < self.heartbeat_timeout:
                    if self.is_server:
                        logger.error(f"Server {self.peer_id} is already running. Duplicate connection detected.")
                        raise RuntimeError(f"Server {self.peer_id} is already running.")
                    else:
                        logger.warning(f"Client {self.peer_id} is reconnecting...")
            else:
                # Insert a new session if it doesn't exist
                session.execute(
                    self.sessions.insert().values(
                        peer=self.peer_id,
                        uuid=self.uuid,
                        last_heartbeat=int(time.time()),
                        is_server=int(self.is_server),
                    )
                )
            session.commit()

    def on(self, event, callback=None):
        """
        Register an event listener. Can be used as a method or a decorator.

        Usage:
            # Using as a decorator
            @socket.on("event_name")
            async def handler(data):
                ...

            # Using directly
            socket.on("event_name", handler)
        """
        if callback is None:
            # If no callback is provided, return a decorator
            def decorator(fn):
                self.callbacks[event] = fn
                return fn
            return decorator
        else:
            # If callback is provided, register it directly
            self.callbacks[event] = callback

    def emit(self, event, data, to=None, room=None, exclude=None, pattern=None):
        """Unified method to emit an event, handling both sync and async contexts."""
        try:
            loop = asyncio.get_running_loop()
            # If an event loop is running, assume it's called from an async context
            # run it in background
            wrapper = asyncio.create_task
        except RuntimeError:
            # No event loop is running, so run the async method synchronously
            wrapper = asyncio.run
        return wrapper(self._emit_async(event, data, to, room, exclude, pattern))

    async def _emit_async(self, event, data, to=None, room=None, exclude=None, pattern=None):
        """Emit an event to a specific client, all clients, or all except specified ones."""
        if isinstance(exclude, list):
            exclude.append(self.peer_id)
        elif exclude is None:
            exclude = self.peer_id
        with self.Session() as session:
            if room:
                # Broadcast to a room
                peers = session.execute(
                    self.sessions.select().where(self.sessions.c.peer.in_(self.rooms.get(room, [])))
                ).fetchall()
                for peer in peers:
                    if not self._should_exclude(peer[0], exclude, pattern):
                        await self._send_event(peer[0], event, data)
            elif to:
                client = session.execute(
                    self.sessions.select().where(self.sessions.c.peer == to)
                ).fetchone()
                if client:
                    await self._send_event(client[0], event, data)
                else:
                    logger.warning(f"Client {to} not found for emitting event.")
            else:
                # Broadcast to all peers except those in the exclude list or matching the pattern
                peers = session.execute(self.sessions.select()).fetchall()
                for peer in peers:
                    if not self._should_exclude(peer[0], exclude, pattern):
                        await self._send_event(peer[0], event, data)

    def _should_exclude(self, peer_id, exclude, pattern):
        """Determine if a peer should be excluded based on exclude list or pattern."""
        if exclude:
            if isinstance(exclude, list) and peer_id in exclude:
                return True
            if isinstance(exclude, str) and peer_id == exclude:
                return True
        if pattern and fnmatch(peer_id, pattern):
            return True
        return False

    async def _send_event(self, peer_id, event, data):
        """Send an event to a specific peer."""
        with self.Session() as session:
            session.execute(
                self.messages.insert().values(
                    sender=self.peer_id,
                    receiver=peer_id,
                    event=event,
                    data=data,
                    timestamp=int(time.time()),
                )
            )
            session.commit()
        logger.info(f"Sent event '{event}' with data: {data} to {peer_id}")

    async def join_room(self, room):
        """Join a room."""
        if room not in self.rooms:
            self.rooms[room] = set()
        self.rooms[room].add(self.peer_id)

    async def leave_room(self, room):
        """Leave a room."""
        if room in self.rooms:
            self.rooms[room].discard(self.peer_id)
            if not self.rooms[room]:
                del self.rooms[room]

    async def _process_messages(self):
        """Process incoming messages."""
        try:
            while self.running:
                with self.Session() as session:
                    messages = session.execute(
                        self.messages.select().where(
                            (self.messages.c.receiver == self.peer_id) |
                            (self.messages.c.receiver == "*") |
                            (self.messages.c.room.in_(self.rooms.keys()))
                        )
                    ).fetchall()

                    for msg in messages:
                        event = msg[4]  # 'event' column
                        data = msg[5]  # 'data' column
                        if event in self.callbacks:
                            callback = self.callbacks[event]
                            # Determine the number of arguments the callback expects
                            num_args = len(inspect.signature(callback).parameters)
                            if num_args == 2:
                                await callback(msg, data)
                            elif num_args == 1:
                                await callback(data)
                            else:
                                await callback()
                        elif "on_unknown_event" in self.callbacks:
                            await self.callbacks["on_unknown_event"]({"event": event, "data": data})
                        
                        # Delete the message after processing
                        session.execute(self.messages.delete().where(self.messages.c.id == msg[0]))
                    session.commit()
                await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:
            logger.info(f"_process_messages task cancelled for {self.peer_id}")
            raise
        
    async def _update_heartbeat(self):
        """Update heartbeat to keep the connection alive."""
        try:
            while self.running:
                with self.Session() as session:
                    session.execute(
                        self.sessions.update()
                        .where(self.sessions.c.peer == self.peer_id)
                        .values(last_heartbeat=int(time.time()))
                    )
                    session.commit()
                await asyncio.sleep(self.update_heartbeat)
        except asyncio.CancelledError:
            logger.info(f"_update_heartbeat task cancelled for {self.peer_id}")
            raise

    async def _clean_orphaned_sessions(self):
        """Clean up stale sessions."""
        try:
            while self.running:
                with self.Session() as session:
                    cutoff_time = int(time.time()) - self.heartbeat_timeout
                    session.execute(self.sessions.delete().where(self.sessions.c.last_heartbeat < cutoff_time))
                    session.commit()
                await asyncio.sleep(self.heartbeat_timeout)
        except asyncio.CancelledError:
            logger.info(f"_clean_orphaned_sessions task cancelled for {self.peer_id}")
            raise

    async def start(self):
        """Start the socket."""
        if self.running:
            logger.warning(f"{'Server' if self.is_server else 'Client'} {self.peer_id} is already running.")
            return
        await self._check_duplicate_connections()
        if self.is_server:
            logger.info(f"Server {self.peer_id} started successfully.")
        else:
            logger.info(f"Client {self.peer_id} started successfully.")
        self.running = True
        self.tasks = []
        self.tasks.append(asyncio.create_task(self._update_heartbeat()))
        self.tasks.append(asyncio.create_task(self._process_messages()))
        if self.is_server:
            self.tasks.append(asyncio.create_task(self._clean_orphaned_sessions()))

    def stop(self):
        """Stops the server or client, can be called sync or async"""
        try:
            loop = asyncio.get_running_loop()
            # If an event loop is running, assume it's called from an async context
            return self.stop_async()
        except RuntimeError:
            # No event loop is running, so run the async method synchronously
            return self.stop_sync()

    def stop_sync(self):
        """Synchronously stop the server or client."""
        if not self.running:
            logger.warning(f"{'Server' if self.is_server else 'Client'} {self.peer_id} is not running.")
            return
        self.running = False

        # Remove the server or client from the sessions table
        with self.Session() as session:
            session.execute(
                self.sessions.delete().where(self.sessions.c.peer == self.peer_id)
            )
            session.commit()

        if "ondisconnection" in self.callbacks:
            asyncio.run(self.callbacks["ondisconnection"]({"peer": self.peer_id, "uuid": self.uuid}))
        
        # Cancel running tasks
        for task in self.tasks:
            task.cancel()
            try:
                asyncio.run(task)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(e)
                raise e
        self.tasks.clear()

        logger.info(f"{'Server' if self.is_server else 'Client'} {self.peer_id} stopped synchronously.")

    async def stop_async(self):
        """Stop the server or client."""
        if not self.running:
            logger.warning(f"{'Server' if self.is_server else 'Client'} {self.peer_id} is not running.")
            return
        self.running = False
        # Cancel running tasks
        for task in self.tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self.tasks.clear()
        
        # Remove the server or client from the sessions table
        with self.Session() as session:
            session.execute(
                self.sessions.delete().where(self.sessions.c.peer == self.peer_id)
            )
            session.commit()
        
        if "ondisconnection" in self.callbacks:
            await self.callbacks["ondisconnection"]({"peer": self.peer_id, "uuid": self.uuid})
        logger.info(f"{'Server' if self.is_server else 'Client'} {self.peer_id} stopped.")

    async def reconnect(self):
        """Attempt to reconnect the client after a disconnection."""
        # Stop any running tasks
        if self.running:
            await self.stop()

        # Wait for the reconnect interval
        await asyncio.sleep(self.reconnect_interval)
        # Try to start again
        try:
            await self.start()
            logger.info(f"Client {self.peer_id} reconnected successfully.")
            if "on_reconnect" in self.callbacks:
                await self.callbacks["on_reconnect"]({"peer": self.peer_id, "uuid": self.uuid})
        except Exception as e:
            logger.error(f"Reconnection attempt failed for client {self.peer_id}: {e}")
            # Optionally, you can retry reconnecting or handle the failure as needed.