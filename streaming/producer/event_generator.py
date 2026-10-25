"""
High-throughput Synthetic Omnichannel Event Generator
Simulates real-world messaging traffic (Blip, WhatsApp Cloud API) and streams
records into Kafka / Redpanda or Google Cloud Pub/Sub.
"""

import json
import logging
import os
import random
import time
import hashlib
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

from schemas import (
    OmnichannelMessageEvent,
    ChannelType,
    MessageDirection,
    MessageStatus,
    LLMMetadata
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("event_generator")

load_dotenv()

BOT_IDS = ["bot_customer_support", "bot_sales_leads", "bot_billing_faq", "bot_onboarding"]
INTENTS = ["check_balance", "track_order", "talk_to_agent", "cancel_subscription", "product_query"]
MODELS = ["gemini-1.5-flash", "gpt-4o-mini", "claude-3-haiku"]


def generate_pseudo_user_id() -> str:
    """Generates an anonymized hashed user identifier conforming to privacy governance."""
    raw_id = f"+55119{random.randint(10000000, 99999999)}"
    return hashlib.sha256(raw_id.encode()).hexdigest()[:16]


def create_synthetic_event() -> OmnichannelMessageEvent:
    """Creates a realistic omnichannel message event."""
    channel = random.choices(
        [ChannelType.WHATSAPP, ChannelType.INSTAGRAM, ChannelType.WEBCHAT, ChannelType.MESSENGER],
        weights=[0.75, 0.12, 0.10, 0.03]  # WhatsApp dominant (typical in Brazil/LATAM)
    )[0]

    direction = random.choices(
        [MessageDirection.INBOUND, MessageDirection.OUTBOUND],
        weights=[0.45, 0.55]
    )[0]

    status = random.choices(
        [MessageStatus.DELIVERED, MessageStatus.READ, MessageStatus.SENT, MessageStatus.FAILED],
        weights=[0.60, 0.30, 0.08, 0.02]
    )[0]

    bot_id = random.choice(BOT_IDS)
    user_id = generate_pseudo_user_id()
    conversation_id = f"conv_{user_id}_{bot_id}_{datetime.utcnow().strftime('%Y%m%d')}"

    is_human_handoff = random.random() < 0.08  # 8% escalation rate to human support

    # Simulate LLM agent invocation for outbound responses
    llm_info: Optional[LLMMetadata] = None
    if direction == MessageDirection.OUTBOUND and random.random() < 0.40:
        llm_info = LLMMetadata(
            model_name=random.choice(MODELS),
            prompt_tokens=random.randint(120, 450),
            completion_tokens=random.randint(30, 180),
            latency_ms=round(random.uniform(280.0, 1250.0), 2),
            intent_detected=random.choice(INTENTS),
            confidence_score=round(random.uniform(0.78, 0.99), 4)
        )

    return OmnichannelMessageEvent(
        conversation_id=conversation_id,
        channel=channel,
        direction=direction,
        status=status,
        message_type=random.choice(["text", "interactive", "media"]),
        content_length=random.randint(10, 350),
        timestamp=datetime.utcnow(),
        user_id_hash=user_id,
        bot_id=bot_id,
        is_handoff_to_human=is_human_handoff,
        error_code="ERR_RATE_LIMIT" if status == MessageStatus.FAILED else None,
        llm_telemetry=llm_info
    )


def run_producer(
    target: str = "kafka",
    events_per_second: int = 10,
    max_events: Optional[int] = 100
):
    """
    Publishes synthetic events to Kafka or standard log output.

    Args:
        target: Destination broker ('kafka', 'pubsub', or 'stdout').
        events_per_second: Streaming throughput rate.
        max_events: Total events to emit (None for continuous streaming).
    """
    logger.info("Initializing synthetic stream generator (Target: %s, Rate: %d eps)...", target, events_per_second)

    producer = None
    topic = os.getenv("KAFKA_TOPIC_MESSAGING_EVENTS", "blip.omnichannel.events.v1")

    if target == "kafka":
        try:
            from kafka import KafkaProducer
            bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3
            )
            logger.info("Connected to Kafka broker at %s", bootstrap_servers)
        except Exception as e:
            logger.warning("Kafka broker unreachable (%s). Emitting events in stdout fallback mode.", e)
            target = "stdout"

    count = 0
    sleep_interval = 1.0 / max(events_per_second, 1)

    try:
        while max_events is None or count < max_events:
            event = create_synthetic_event()
            event_dict = json.loads(event.json())

            if producer and target == "kafka":
                producer.send(topic, key=event.conversation_id.encode("utf-8"), value=event_dict)
            else:
                logger.info("EMITTED [Chan: %s | Dir: %s | Status: %s | Conv: %s]",
                            event.channel.value, event.direction.value, event.status.value, event.conversation_id[:16])

            count += 1
            if count % 50 == 0:
                logger.info("Published %d omnichannel events...", count)

            time.sleep(sleep_interval)

        logger.info("Stream generation completed. Total events published: %d", count)
    except KeyboardInterrupt:
        logger.info("Generator interrupted by user. Shutting down gracefully...")
    finally:
        if producer:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    run_producer(target="stdout", events_per_second=5, max_events=20)
