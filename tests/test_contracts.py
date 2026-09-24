from uuid import uuid4

import pytest
from pydantic import ValidationError
from notification_service.api.schemas import parse_event
from notification_service.api.schemas import CampaignInput
from notification_service.clients.auth import Recipient
from notification_service.rendering import render_email
from notification_service.templates import initial_templates


def test_event_rejects_unknown_payload_and_naive_date():
    event = dict(event_id=str(uuid4()), event_type="user.registered",
                 occurred_at="2026-09-22T10:00:00Z", payload={"user_id": str(uuid4())})
    assert parse_event(event).event_type == "user.registered"
    with pytest.raises(ValidationError):
        parse_event({**event, "occurred_at": "2026-09-22T10:00:00"})
    with pytest.raises(ValidationError):
        parse_event({**event, "payload": {**event["payload"], "email": "a@example.org"}})


def test_movie_title_rejects_header_newlines():
    movie = {
        "event_id": str(uuid4()), "event_type": "movie.published",
        "occurred_at": "2026-09-22T10:00:00Z",
        "payload": {"movie_id": str(uuid4()), "title": "Premiere\nBcc: attacker@example.org",
                    "url": "https://cinema.example.org/movie"},
    }
    with pytest.raises(ValidationError):
        parse_event(movie)


def test_campaign_deduplicates_and_rejects_recipient_override():
    user = str(uuid4())
    body = dict(request_id=str(uuid4()), template_id="announcement",
                audience={"kind": "users", "user_ids": [user, user]},
                data={"headline": "Hi", "message": "Movie"})
    assert len(CampaignInput.model_validate(body).audience.user_ids) == 1
    with pytest.raises(ValidationError):
        CampaignInput.model_validate({**body, "data": {**body["data"], "recipient": {}}})


def test_rendering_escapes_names_and_handles_missing_name():
    template = initial_templates()["welcome"]
    named = Recipient(id=uuid4(), email="a@example.org", first_name="<Anna>")
    result = render_email(template, named, {})
    assert "&lt;Anna&gt;" in result.html
    assert "<Anna>" in result.text
    anonymous = Recipient(id=uuid4(), email="a@example.org")
    assert "Здравствуйте!" in render_email(template, anonymous, {}).text
    assert "None" not in render_email(template, anonymous, {}).text


def test_subject_cannot_inject_headers():
    with pytest.raises(ValueError):
        render_email(initial_templates()["announcement"], Recipient(id=uuid4(), email="a@example.org"),
                     {"headline": "Hi\r\nBcc: other@example.org", "message": "Hi"})
