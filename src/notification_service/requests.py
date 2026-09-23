import hashlib
import json
from uuid import uuid4

from fastapi import HTTPException
from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError

from notification_service.api.schemas import CampaignInput
from notification_service.db.models import Request, Template, Delivery, now
from notification_service.outbox import enqueue


class RequestService:
    def __init__(self, sessions):
        self.sessions = sessions

    async def accept(self, principal, kind, body):
        data = body.model_dump(mode="json")
        canonical = body.model_dump(mode="json", exclude_defaults=True)
        digest = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        external_id = body.request_id if isinstance(body, CampaignInput) else body.event_id
        async with self.sessions() as session:
            query = select(Request).where(Request.service_id == principal,
                        Request.endpoint_kind == kind, Request.external_id == external_id)
            old = await session.scalar(query)
            if old:
                return self.receipt(old, digest)
            if isinstance(body, CampaignInput):
                template_id, audience, context = body.template_id, data["audience"], data["data"]
            elif body.event_type == "user.registered":
                template_id, audience, context = "welcome", {"kind": "users", "user_ids": [str(body.payload.user_id)]}, {}
            else:
                template_id, audience, context = "new_movie", {"kind": "all"}, data["payload"]
            template = await session.scalar(select(Template).where(Template.template_id == template_id)
                                           .order_by(Template.version.desc()).limit(1))
            if template is None:
                raise HTTPException(503, "Templates not initialized")
            accepted = now()
            lane = body.priority if isinstance(body, CampaignInput) else ("urgent" if body.event_type == "user.registered" else "bulk")
            category = "marketing" if isinstance(body, CampaignInput) else ("account" if body.event_type == "user.registered" else "catalog")
            row = Request(id=uuid4(), service_id=principal, endpoint_kind=kind, external_id=external_id,
                          channel=body.channel, lane=lane, category=category,
                          input_hash=digest, event_type=getattr(body, "event_type", None),
                          template_version_id=template.id, audience=audience, data=context,
                          accepted_at=accepted, audience_cutoff=accepted, status="accepted")
            session.add(row)
            enqueue(session, "prepare", row.id, 0, lane=lane)
            try:
                await session.commit()
            except IntegrityError:
                await session.rollback()
                old = await session.scalar(query)
                if old is None:
                    raise
                return self.receipt(old, digest)
            return self.receipt(row, digest)

    @staticmethod
    def receipt(row, digest):
        if row.input_hash != digest:
            raise HTTPException(409, "Idempotency key already used with different content")
        return {"id": str(row.id), "status": "accepted"}

    async def get_status(self, principal, request_id):
        async with self.sessions() as session:
            row = await session.get(Request, request_id)
            if not row:
                raise HTTPException(404, "Request not found")
            if row.service_id != principal and principal != "operator":
                raise HTTPException(403, "Forbidden")
            counts = dict((await session.execute(select(Delivery.status, func.count())
                          .where(Delivery.request_id == row.id).group_by(Delivery.status))).all())
            return {"id": str(row.id), "preparation_status": row.status,
                    "last_error_code": row.last_error_code,
                    "deliveries": {key: counts.get(key, 0) for key in
                                  ("pending", "processing", "retry_wait", "sent", "failed", "skipped")}}
