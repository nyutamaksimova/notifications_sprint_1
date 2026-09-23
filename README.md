# Email-уведомления кинотеатра

FastAPI принимает события и кампании; PostgreSQL хранит заявки и outbox;
RabbitMQ передаёт задания prepare/send-воркерам. Send-воркер получает адрес и имя
через внутренний API auth-service и отправляет multipart email через SMTP.


### Регистрация

`POST /api/v1/events`, токен auth:

```json
{
  "event_id": "ed1b31c8-783d-44e6-92eb-f943f375b2aa",
  "event_type": "user.registered",
  "occurred_at": "2026-09-22T10:00:00Z",
  "payload": {"user_id": "65990712-54c2-4c69-a209-8b748a68c466"}
}
```

### Новый фильм

Тот же endpoint, токен catalog:

```json
{
  "event_id": "408ed500-e05f-4a97-ac49-a608d021d27f",
  "event_type": "movie.published",
  "occurred_at": "2026-09-22T10:00:00Z",
  "payload": {
    "movie_id": "63442038-d222-4109-941c-2a3099899c50",
    "title": "Новый фильм",
    "url": "https://cinema.example.org/movies/63442038-d222-4109-941c-2a3099899c50"
  }
}
```
### Одинаковая и персонализированная рассылки

`POST /api/v1/campaigns`, токен operator:

```json
{
  "request_id": "4e446bce-544b-442d-adb2-383b7085459b",
  "template_id": "personal_announcement",
  "audience": {"kind": "all"},
  "data": {"headline": "Премьера недели", "message": "В каталоге появился новый фильм."}
}
```
