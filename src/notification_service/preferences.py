from uuid import UUID

from pydantic import Field
from sqlalchemy.exc import IntegrityError

from notification_service.api.schemas import StrictModel
from notification_service.db.models import Preference


class Channels(StrictModel):
    email: bool = True


class Categories(StrictModel):
    account: bool = True
    catalog: bool = True
    marketing: bool = True


class PreferenceInput(StrictModel):
    enabled: bool = True
    channels: Channels = Field(default_factory=Channels)
    categories: Categories = Field(default_factory=Categories)


class PreferencesService:
    def __init__(self, sessions):
        self.sessions = sessions

    async def get(self, user_id: UUID):
        async with self.sessions() as session:
            row = await session.get(Preference, user_id)
            return PreferenceInput.model_validate(row.settings) if row else PreferenceInput()

    async def set(self, user_id: UUID, value: PreferenceInput):
        # Retry concurrent first creation, then serialize updates with a row lock.
        from sqlalchemy import select
        for attempt in range(2):
            async with self.sessions() as session:
                row = await session.scalar(select(Preference).where(Preference.user_id == user_id).with_for_update())
                if row is None:
                    session.add(Preference(user_id=user_id, settings=value.model_dump()))
                else:
                    row.settings = value.model_dump()
                try:
                    await session.commit()
                    return value
                except IntegrityError:
                    await session.rollback()
                    if attempt:
                        raise

    async def allows(self, user_id, channel, category):
        value = await self.get(user_id)
        return value.enabled and getattr(value.channels, channel, False) and getattr(value.categories, category, False)
