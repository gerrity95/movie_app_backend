from uuid import uuid4
import datetime
import pickle
from enum import Enum, auto


class State(Enum):
    
    undefined = auto()
    ok = auto()
    in_progress = auto()
    fail = auto()
    skipped = auto()
    
    def __str__(self):
        return self.name
    
    def deconstruct(self) -> str:
        return self.name


class RecommendationsEvent:

    @classmethod
    def event_type(cls) -> str:
        return cls.__name__
    
    @staticmethod
    def routing_key() -> str:
        return 'recommendations_queue'

    def __init__(self, user_id: str, is_new: bool):
        self.user_id = user_id
        self.is_new = is_new
        self.uuid = str(uuid4())
        self.parent_uuid = self.uuid
        self.timestamp = datetime.datetime.now()
        self.reccomendations = []
        self.result_routing_key = self.uuid
        self.test_attribute = "Hello Mark"
        self.state = State.undefined
        self.existing_reccs_id = None
        
    def serialize(self):
        """Convert a RecommendationsEvent to bytes so we can pass it through RMQ"""
        return pickle.dumps(self)
    
    def deserialize(self):
        """Convert a bytes version of RecommendationsEvent to an Object"""
        return pickle.loads(self)
