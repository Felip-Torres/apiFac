from pydantic import BaseModel
from datetime import date, datetime
from typing import List, Optional

class Group(BaseModel):
    ID: int
    NAME: str
    DESCRIPTION: str
    SIZE: int
    CREATION_DATE: date
    
class UsuarisClase(BaseModel):
    ID: int
    USERNAME: str
    PASSWORD: str
    BIO: str
    
class UserGroup(BaseModel):
    ID_GROUP: int
    ID_USER: int
    JOIN_DATE: datetime
    ADMIN: int  #revisar el tinyint de la DB es un bool
    
class Message(BaseModel):
    ID: int
    DATE: datetime
    STATUS: str
    BODY: str
    SENDER_ID: int
    RECEIVER_ID: int
    GROUP_ID: int 
    
class LastMessageUsers(BaseModel):
    ID_USER: int
    
class LoginRequest(BaseModel):
    USERNAME: str
    PASSWORD: str
    
class CreateGroupRequest(BaseModel):
    NAME: str
    DESCRIPTION: str
    USERS: List[int]
    ADMIN: int