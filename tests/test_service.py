"""The production gateway must publish LLM health events back to the bus."""
from kairos_text.config import TextSettings
from kairos_text.service import TextScoutsService


def test_gateway_health_hook_is_wired():
    svc = TextScoutsService(TextSettings(bus_backend="memory"))
    assert svc.extractor.gateway._on_health is not None
