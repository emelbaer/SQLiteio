from fnmatch import fnmatch
import inspect
import uuid
import asyncio
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session, joinedload
from sqlalchemy.orm.exc import DetachedInstanceError
from sqliteio.model import BaseModel, Message, Session, Peer  # Import the ORM classes
from sqliteio.logger import setup_logger
from datetime import datetime, timedelta

# Set up the logger
logger = setup_logger(name="SQLiteIO", log_file="logs/sqliteio.log")


class SQLiteSocket:
    def __init__(self, db_path, peer_id, is_server=False, poll_interval=1, heartbeat_timeout=30):
        self.db_path = db_path
        self.peer_id = peer_id
        self.is_server = is_server
        self.role = 'client' if not is_server else 'server'
        self.poll_interval = poll_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.update_heartbeat = self.heartbeat_timeout // 2
        self.callbacks = {}
        self.rooms = {}
        self.running = False
        # Unique identifier for each connection
        self.sid = str(uuid.uuid4()).replace('-', '')
        self.reconnect_interval = 5  # Reconnect interval for clients
        self.server_peer_id = None  # Identify the server for clients

        # SQLite DB setup
        try:
            self.engine = create_engine(f"sqlite:///{db_path}")
            BaseModel.metadata.create_all(self.engine) # create Tables
            self.Session = scoped_session(sessionmaker(bind=self.engine))
        except Exception as e:
            logger.critical( f"Failed to connect to SQLite database: {e}")
            raise
        # SQLite instances
        self.peerEntry = None
        self.sessionEntry = None

    async def _check_duplicate_connections(self):
        """Check for duplicate connections and handle conflicts."""
        with self.Session() as session:
            # Delete dead peers with the same name
            session.query(Session).filter(
                Session.peer_id == self.peer_id,
                Session.last_heartbeat == None
            ).delete()

            existingPeer = session.query(Peer).filter(
                Peer.peer_name == self.peer_id
            ).first()
            if not existingPeer:
                # Insert a new peer
                new_peer = Peer(
                    peer_name=self.peer_id,
                    role=self.role,
                    last_heartbeat=datetime.now(),
                )
                session.add(new_peer)
                self.peerEntry = new_peer
            else:
                existingPeer.last_heartbeat = datetime.now()
                self.peerEntry = existingPeer
            
            existingSession = session.query(Session).filter(
                Session.sid == self.sid
            ).first()  # Check if the session already exists
            if existingSession:
                last_heartbeat = existingSession.last_heartbeat
                if datetime.now() - last_heartbeat < timedelta(seconds=self.heartbeat_timeout):
                    if self.is_server:
                        logger.error(
                            f"Server {self.peer_id} is already running. Duplicate connection detected.")
                        raise RuntimeError(
                            f"Server {self.peer_id} is already running.")
                    else:
                        logger.warning(
                            f"Client {self.peer_id} is reconnecting...")
                self.sessionEntry = existingSession
            else:
                # Insert a new session if it doesn't exist
                new_session = Session(
                    peer_id=self.peer_id,
                    sid=self.sid,
                    last_heartbeat=datetime.now(),
                )
                session.add(new_session)
                self.sessionEntry = new_session
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

    def emit(self, event, data, to=None, room=None, exclude=None, exclude_pattern=None, sendToOffline=False):
        """Unified method to emit an event, handling both sync and async contexts."""
        try:
            loop = asyncio.get_running_loop()
            # If an event loop is running, assume it's called from an async context
            # run it in background
            wrapper = asyncio.create_task
        except RuntimeError:
            # No event loop is running, so run the async method synchronously
            wrapper = asyncio.run
        return wrapper(self._emit_async(event, data, to, room, exclude, exclude_pattern, sendToOffline))

    async def _emit_async(self, event, data, to=None, room=None, exclude=None, exclude_pattern=None, sendToOffline=False):
        """Emit an event to a specific client, all clients, or all except specified ones."""
        if isinstance(exclude, list):
            exclude.append(self.peer_id)
        elif exclude is None:
            exclude = self.peer_id

        with self.Session() as session:
            if room:
                # Broadcast to a room
                if sendToOffline:
                    peers = session.query(Session.peer).filter(Session.peer.in_(self.rooms.get(room, []))).all()
                else:
                    peers = session.query(Session.peer).filter((Session.peer.in_(self.rooms.get(room, []))) & (Session.last_heartbeat != None)).all()
                for peer in peers:
                    if not self._should_exclude(peer.peer, exclude, exclude_pattern):
                        await self._send_event(peer.peer, event, data)
            elif to:
                # Send to a specific client
                client = session.query(Session.peer).filter(Session.peer_id == to).first()
                if client:
                    await self._send_event(client.peer, event, data)
                else:
                    logger.warning(f"Client {to} not found for emitting event.")
            else:
                # Broadcast to all peers except those in the exclude list or matching the pattern
                if sendToOffline:
                    peers = session.query(Session.peer).all()
                else:
                    peers = session.query(Session.peer).filter( Session.last_heartbeat != None ).all()
                for peer in peers:
                    if not self._should_exclude(peer.peer, exclude, exclude_pattern):
                        await self._send_event(peer.peer, event, data)

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
            new_message = Message(
                sender=self.peer_id,
                receiver=peer_id,
                event=event,
                data=data,
                timestamp=datetime.now(),
            )
            session.add(new_message)
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
                try:
                    with self.Session() as session:
                        messages = session.query(Message).filter(
                            (Message.receiver == self.peer_id) |
                            (Message.receiver == "*") |
                            (Message.room.in_(self.rooms.keys()))
                        ).all()

                        for msg in messages:
                            # Reattach the message to the session
                            msg = session.merge(msg)
                            event = msg.event
                            data = msg.data
                            # Delete the message after processing
                            session.delete(msg)
                            if event in self.callbacks:
                                callback = self.callbacks[event]
                                num_args = len(inspect.signature(callback).parameters)
                                if num_args == 2:
                                    await callback(msg, data)
                                elif num_args == 1:
                                    await callback(data)
                                else:
                                    await callback()
                            elif "unknown_event" in self.callbacks:
                                await self.callbacks["unknown_event"]({"event": event, "data": data})

                        session.commit()
                except DetachedInstanceError:
                    pass
                await asyncio.sleep(self.poll_interval)
        except asyncio.CancelledError:
            logger.info(f"_process_messages task cancelled for {self.peer_id}")
            raise
        except Exception as e:
            logger.error(f"Error processing messages for {self.peer_id}: {e}")
            raise


    async def _update_heartbeat(self):
        """Update heartbeat to keep the connection alive."""
        try:
            while self.running:
                with self.Session() as session:
                    session.merge(self.sessionEntry)
                    self.sessionEntry.last_heartbeat = datetime.now() # Update the last heartbeat of the session
                    session.merge(self.peerEntry)
                    self.peerEntry.last_heartbeat = datetime.now()  # Update the last heartbeat of the peer
                    session.commit()
                await asyncio.sleep(self.update_heartbeat)
        except asyncio.CancelledError:
            logger.info(f"_update_heartbeat task cancelled for {self.peer_id}")

    async def _clean_orphaned_sessions(self):
        """Clean up stale sessions."""
        try:
            while self.running:
                with self.Session() as session:
                    cutoff_time = datetime.now() - timedelta(seconds=self.heartbeat_timeout)
                    session.query(Session).filter(Session.last_heartbeat < cutoff_time).delete()
                    session.commit()
                await asyncio.sleep(self.heartbeat_timeout)
        except asyncio.CancelledError:
            logger.info(f"_clean_orphaned_sessions task cancelled for {self.peer_id}")
            raise

    async def start(self):
        """Start the socket."""
        if self.running:
            logger.warning(f"{'Server' if self.is_server else 'Client'} {
                           self.peer_id} is already running.")
            return
        await self._check_duplicate_connections()
        self.running = True
        self.tasks = []
        self.tasks.append(asyncio.create_task(self._update_heartbeat()))
        self.tasks.append(asyncio.create_task(self._process_messages()))
        if self.is_server:
            self.tasks.append(asyncio.create_task(
                self._clean_orphaned_sessions()))
        # print on logger about the successfully start
        if self.is_server:
            logger.info(f"Server {self.peer_id} started successfully.")
        else:
            logger.info(f"Client {self.peer_id} started successfully.")

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
            logger.warning(f"{'Server' if self.is_server else 'Client'} {
                           self.peer_id} is not running.")
            return
        self.running = False

        asyncio.run(self.stopSession()) #set last_heartbeat to null

        if "ondisconnection" in self.callbacks:
            asyncio.run(self.callbacks["ondisconnection"](
                {"peer": self.peer_id, "uuid": self.sid}))

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

        logger.info(f"{'Server' if self.is_server else 'Client'} {
                    self.peer_id} stopped synchronously.")

    async def stop_async(self):
        """Stop the server or client."""
        if not self.running:
            logger.warning(f"{'Server' if self.is_server else 'Client'} {
                           self.peer_id} is not running.")
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

        await self.stopSession() #set last_heartbeat to null

        if "ondisconnection" in self.callbacks:
            await self.callbacks["ondisconnection"]({"peer": self.peer_id, "uuid": self.sid})
        logger.info(f"{'Server' if self.is_server else 'Client'} {
                    self.peer_id} stopped.")

    async def stopSession(self):
        """Set the heartbeat of this session to null."""
        with self.Session() as session:
            session.query(Session).filter(Session.sid == self.sid).update(
                {"last_heartbeat": None}
            )
            session.commit()


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
                await self.callbacks["on_reconnect"]({"peer": self.peer_id, "uuid": self.sid})
        except Exception as e:
            logger.error(f"Reconnection attempt failed for client {
                         self.peer_id}: {e}")
            # Optionally, you can retry reconnecting or handle the failure as needed.
