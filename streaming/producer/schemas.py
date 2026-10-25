"""
Data Schemas for Omnichannel Messaging Events
Modeled after enterprise conversational platforms (e.g. Blip, WhatsApp Business API).
"""

from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
import uuid


class ChannelType(str, Enum):
    WHATSAPP = "whatsapp"
    INSTAGRAM = "instagram"
    WEBCHAT = "webchat"
    MESSENGER = "messenger"


class MessageDirection(str, Enum):
    INBOUND = "inbound"    # Customer -> Bot / Human Agent
    OUTBOUND = "outbound"  # Bot / Human Agent -> Customer


class MessageStatus(str, Enum):
    SENT = "sent"
    DELIVERED = "delivered"
    READ = "read"
    FAILED = "failed"


class LLMMetadata(BaseModel):
    """Telemetry captured when a generative AI agent or intent classifier is invoked."""
    model_name: str = Field(..., description="LLM model identifier (e.g. gpt-4o, gemini-1.5-flash)")
    prompt_tokens: int = Field(default=0, ge=0)
    completion_tokens: int = Field(default=0, ge=0)
    latency_ms: float = Field(default=0.0, ge=0.0)
    intent_detected: Optional[str] = None
    confidence_score: Optional[float] = None


class OmnichannelMessageEvent(BaseModel):
    """Core omnichannel event payload ingested into the real-time Lakehouse."""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    conversation_id: str = Field(..., description="Unified conversation session thread ID")
    channel: ChannelType
    direction: MessageDirection
    status: MessageStatus
    message_type: str = Field(default="text", description="text, media, location, interactive")
    content_length: int = Field(default=0, ge=0)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    user_id_hash: str = Field(..., description="Pseudonymized SHA-256 user identifier (LGPD/GDPR)")
    bot_id: str = Field(..., description="Chatbot or routing flow identifier")
    is_handoff_to_human: bool = Field(default=False)
    error_code: Optional[str] = None
    llm_telemetry: Optional[LLMMetadata] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
