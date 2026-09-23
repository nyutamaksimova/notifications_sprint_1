from dataclasses import dataclass

from jinja2 import Environment, StrictUndefined


@dataclass(frozen=True)
class RenderedEmail:
    subject: str
    html: str
    text: str


def render_email(template, recipient, data):
    if "recipient" in data:
        raise ValueError("recipient is reserved")
    plain = Environment(undefined=StrictUndefined, autoescape=False)
    html = Environment(undefined=StrictUndefined, autoescape=True)
    name = " ".join(x for x in (recipient.first_name, recipient.last_name) if x).strip()
    values = {"recipient": recipient, "greeting": f"Здравствуйте, {name}!" if name else "Здравствуйте!", "data": data}
    subject = plain.from_string(template.subject).render(**values)
    if "\r" in subject or "\n" in subject:
        raise ValueError("invalid subject")
    return RenderedEmail(subject, html.from_string(template.html_body).render(**values),
                         plain.from_string(template.text_body).render(**values))

