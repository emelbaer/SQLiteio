import uuid
import asyncio
import time
from sqlalchemy import create_engine, Column, String, Integer, Table, MetaData
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
        self.uuid = str(uuid.uuid4())  # Unique identifier for each connection
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
            Column("data", String),
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

    async def emit(self, event, data, to=None, room=None):
        """Emit an event to a specific client or all clients."""
        with self.Session() as session:
            if room:
                # Broadcast to a room
                peers = session.execute(
                    self.sessions.select().where(self.sessions.c.peer.in_(self.rooms.get(room, [])))
                ).fetchall()
                for peer in peers:
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
                logger.warning("Emit called with neither 'to' nor 'room' specified.")

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
                        await self.callbacks[event](data)
                    elif "on_unknown_event" in self.callbacks:
                        await self.callbacks["on_unknown_event"]({"event": event, "data": data})
                    session.execute(self.messages.delete().where(self.messages.c.id == msg[0]))
                session.commit()
            await asyncio.sleep(self.poll_interval)

    async def _update_heartbeat(self):
        """Update heartbeat to keep the connection alive."""
        while self.running:
            with self.Session() as session:
                session.execute(
                    self.sessions.update()
                    .where(self.sessions.c.peer == self.peer_id)
                    .values(last_heartbeat=int(time.time()))
                )
                session.commit()
            await asyncio.sleep(self.update_heartbeat)

    async def _clean_orphaned_sessions(self):
        """Clean up stale sessions."""
        while self.running:
            with self.Session() as session:
                cutoff_time = int(time.time()) - self.heartbeat_timeout
                session.execute(self.sessions.delete().where(self.sessions.c.last_heartbeat < cutoff_time))
                session.commit()
            await asyncio.sleep(self.heartbeat_timeout)

    async def start(self):
        """Start the socket."""
        await self._check_duplicate_connections()
        if self.is_server:
            logger.info(f"Server {self.peer_id} started successfully.")
        else:
            logger.info(f"Client {self.peer_id} started successfully.")
        self.running = True
        asyncio.create_task(self._update_heartbeat())
        asyncio.create_task(self._process_messages())
        asyncio.create_task(self._clean_orphaned_sessions())

    async def stop(self):
        """Stop the server or client."""
        if "ondisconnection" in self.callbacks:
            await self.callbacks["ondisconnection"]({"peer": self.peer_id, "uuid": self.uuid})
        self.running = False
        logger.info(f"{'Server' if self.is_server else 'Client'} {self.peer_id} stopped.")
