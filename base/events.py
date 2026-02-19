from uuid import uuid4
import datetime
import pickle
from enum import Enum, auto
from env_config import Config

def define_state(state: str):
    # Construct State enum
    if isinstance(state, State):
        return state
    for state_enum in State:
        if state_enum.name.lower() == state:
            return state_enum
    return State.undefined

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

    @staticmethod
    def reply_key() -> str:
        return "reply_queue"

    def __init__(self):
        self.user_id = ""
        self.uuid = str(uuid4())
        self.parent_uuid = self.uuid
        self.timestamp = datetime.datetime.now()
        self.reccomendations = []
        self.duration = 0
        self.result_routing_key = self.uuid
        self.state = State.undefined
        self.existing_reccs_id = None
        
    def deconstruct(self) -> dict:
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
    
    @staticmethod
    def reconstruct(a_dict: dict):

        event = RecommendationsEvent()
        event.user_id = a_dict.get('user_id', event.user_id)
        event.uuid = a_dict.get('uuid', event.uuid)
        event.parent_uuid = a_dict.get('parent_uuid', event.parent_uuid)
        event.timestamp = a_dict.get('timestamp', event.timestamp)
        event.reccomendations = a_dict.get('reccomendations', event.reccomendations)
        event.duration = a_dict.get('duration', event.duration)
        event.result_routing_key = a_dict.get('result_routing_key', event.result_routing_key)
        event.existing_reccs_id = a_dict.get('existing_reccs_id', event.existing_reccs_id)
        event.state = define_state(a_dict.get('state', event.state))
        

        return event


        
    # def serialize(self):
    #     """Convert a RecommendationsEvent to bytes so we can pass it through RMQ"""
    #     return pickle.dumps(self)
    
    # def deserialize(self):
    #     """Convert a bytes version of RecommendationsEvent to an Object"""
    #     return pickle.loads(self)
