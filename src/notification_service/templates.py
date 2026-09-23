from sqlalchemy import select
from notification_service.db.models import Template


def initial_templates():
    definitions = {
        "welcome": ("Добро пожаловать в кинотеатр", "{{ greeting }}\nРады видеть вас в нашем кинотеатре!",
                    "<p>{{ greeting }}</p><p>Рады видеть вас в нашем кинотеатре!</p>"),
        "new_movie": ("Новый фильм: {{ data.title }}", "{{ greeting }}\n{{ data.title }}\n{{ data.url }}",
                      '<p>{{ greeting }}</p><p><a href="{{ data.url }}">{{ data.title }}</a></p>'),
        "announcement": ("{{ data.headline }}", "{{ data.message }}", "<p>{{ data.message }}</p>"),
        "personal_announcement": ("{{ data.headline }}", "{{ greeting }}\n{{ data.message }}",
                                  "<p>{{ greeting }}</p><p>{{ data.message }}</p>"),
    }
    return {key: Template(template_id=key, version=1, subject=subject, text_body=text, html_body=html)
            for key, (subject, text, html) in definitions.items()}


async def seed_templates(session):
    for key, template in initial_templates().items():
        existing = await session.scalar(select(Template.id).where(Template.template_id == key, Template.version == 1))
        if not existing:
            session.add(template)
    await session.commit()

