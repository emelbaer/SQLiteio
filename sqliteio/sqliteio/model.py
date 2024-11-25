from sqlalchemy import Column, ForeignKey, String, Integer, JSON, DateTime
from sqlalchemy.orm import relationship, declarative_base
# Define the base class for ORM models
BaseModel = declarative_base()

# Define the Messages ORM class
class Message(BaseModel):
    __tablename__ = 'messages'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    sender = Column(String(65))
    receiver = Column(String(65), nullable=True)
    room = Column(String(65), nullable=True)
    event = Column(String(128))
    data = Column(JSON)
    timestamp = Column(DateTime)


class Peer(BaseModel):
    __tablename__ = 'peers'
    peer_name = Column(String, primary_key=True) # Unique name per peer
    role = Column(String)  # server or client
    last_heartbeat = Column(DateTime)  # Last heartbeat timestamp
    sessions = relationship("Session", back_populates="peer")  # Define the relationship with Sessions

# Define the Sessions ORM class
class Session(BaseModel):
    __tablename__ = 'sessions'
    
    peer_id = Column(String, ForeignKey('peers.peer_name'))
    sid = Column(String, primary_key=True)  # Unique ID per connection
    last_heartbeat = Column(DateTime)

    peer = relationship("Peer", back_populates="sessions")
