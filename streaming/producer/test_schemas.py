"""
Unit Tests for Omnichannel Event Schemas & Generation Logic
"""

import sys
from pathlib import Path
from datetime import datetime
import pytest

# Ensure producer package can be imported regardless of execution context
sys.path.insert(0, str(Path(__file__).parent))

from schemas import (
    OmnichannelMessageEvent,
    ChannelType,
    MessageDirection,
    MessageStatus,
    LLMMetadata,
)
from event_generator import create_synthetic_event


def test_omnichannel_event_creation():
    """Validates that a message event initializes with required attributes."""
    event = OmnichannelMessageEvent(
        conversation_id="conv_test_123",
        channel=ChannelType.WHATSAPP,
        direction=MessageDirection.INBOUND,
        status=MessageStatus.DELIVERED,
        user_id_hash="user_abc12345",
        bot_id="bot_support_01",
    )

    assert event.conversation_id == "conv_test_123"
    assert event.channel in [ChannelType.WHATSAPP, ChannelType.WHATSAPP.value]
    assert event.direction in [MessageDirection.INBOUND, MessageDirection.INBOUND.value]
    assert event.status in [MessageStatus.DELIVERED, MessageStatus.DELIVERED.value]
    assert event.is_handoff_to_human is False
    assert isinstance(event.timestamp, datetime)
    assert len(event.event_id) > 0


def test_synthetic_event_generator():
    """Tests the stochastic event generator produces valid schema events."""
    event = create_synthetic_event()

    assert event.conversation_id.startswith("conv_")
    assert event.channel in [c.value for c in ChannelType] or event.channel in list(ChannelType)
    assert event.direction in [d.value for d in MessageDirection] or event.direction in list(MessageDirection)
    assert event.status in [s.value for s in MessageStatus] or event.status in list(MessageStatus)
    assert len(event.user_id_hash) == 16


def test_llm_telemetry_schema():
    """Validates LLM telemetry attributes and constraints."""
    llm = LLMMetadata(
        model_name="gemini-1.5-flash",
        prompt_tokens=250,
        completion_tokens=80,
        latency_ms=450.2,
        intent_detected="track_order",
        confidence_score=0.96,
    )

    assert llm.model_name == "gemini-1.5-flash"
    assert llm.prompt_tokens == 250
    assert llm.completion_tokens == 80
    assert llm.latency_ms > 0
    assert 0 <= llm.confidence_score <= 1.0
