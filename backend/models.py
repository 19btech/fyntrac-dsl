"""Pydantic models / schemas used across the application."""

from pydantic import BaseModel, Field, ConfigDict
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import uuid


class EventDefinition(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_name: str
    fields: List[Dict[str, str]]
    eventType: str = 'activity'
    eventTable: str = 'standard'
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class DSLFunction(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    function_name: str
    parameters: str
    description: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class EventData(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_name: str
    data_rows: List[Dict[str, Any]]
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class DSLTemplate(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    dsl_code: str
    python_code: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class DSLTemplateArtifact(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    template_id: str
    template_name: str
    version: int = 1
    python_code: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    read_only: bool = True


class TransactionOutput(BaseModel):
    model_config = ConfigDict(extra="ignore")
    postingdate: str
    effectivedate: str
    instrumentid: str
    subinstrumentid: str = '1'
    transactiontype: str
    amount: float


class TransactionReport(BaseModel):
    model_config = ConfigDict(extra="ignore")
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    template_name: str
    event_name: str
    transactions: List[Dict[str, Any]]
    executed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ChatMessage(BaseModel):
    message: str
    session_id: Optional[str] = None
    context: Optional[Dict[str, Any]] = None
    model: Optional[str] = None
    history: Optional[List[Dict[str, str]]] = None


class ChatResponse(BaseModel):
    response: str
    session_id: str
    structured: Optional[Dict[str, Any]] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None


class AIProviderTestRequest(BaseModel):
    provider: str
    api_key: str


class AIProviderSaveRequest(BaseModel):
    provider: str
    api_key: str
    selected_model: str
    available_models: List[Dict[str, str]]


class DSLValidationRequest(BaseModel):
    dsl_code: str


class SaveTemplateRequest(BaseModel):
    name: str
    dsl_code: str
    event_name: str
    replace: bool = False


class DSLRunRequest(BaseModel):
    dsl_code: str
    posting_date: Optional[str] = None
    effective_date: Optional[str] = None


class TemplateExecuteRequest(BaseModel):
    template_id: str
    event_name: str
    posting_date: Optional[str] = None
    effective_date: Optional[str] = None
