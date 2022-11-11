from uuid import uuid4
import datetime
import pickle
from enum import Enum, auto
from env_config import Config


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
        return Config().ROUTING_KEY

    def __init__(self, user_id: str):
        self.user_id = user_id
        self.uuid = str(uuid4())
        self.parent_uuid = self.uuid
        self.timestamp = datetime.datetime.now()
        self.reccomendations = []
        self.duration = 0
        self.result_routing_key = self.uuid
        self.test_attribute = "Hello Mark"
        self.state = State.undefined
        self.existing_reccs_id = None
        
    def deconstruct(self):
        """Deconstruct the class object into a JSON readable format"""
        a_dict = {
            "user_id": self.user_id,
            "uuid": self.uuid,
            "parent_uuid": self.parent_uuid,
            "reccomendations": self.reccomendations,
            "duration": self.duration,
            "result_routing_key": self.result_routing_key,
            "state": self.state.name,
            "existing_reccs_id": self.existing_reccs_id
        }
        
        # Remove empty attributes
        return {k: v for k, v in a_dict.items()
                if v != "" and v is not None and v != {}}
        
    def serialize(self):
        """Convert a RecommendationsEvent to bytes so we can pass it through RMQ"""
        return pickle.dumps(self)
    
    def deserialize(self):
        """Convert a bytes version of RecommendationsEvent to an Object"""
        return pickle.loads(self)
