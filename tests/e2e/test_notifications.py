import asyncio
import os
from uuid import uuid4

import httpx
import pytest

pytestmark = pytest.mark.e2e


async def wait_for(callback, timeout=45):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        result = await callback()
        if result:
            return result
        await asyncio.sleep(0.5)
    raise AssertionError("Notification did not arrive within timeout")


@pytest.mark.parametrize("template", ["announcement", "personal_announcement"])
async def test_registration_and_campaign(template):
    api = os.getenv("E2E_NOTIFICATION_URL")
    if not api:
        pytest.skip("Full Compose stack required")
    auth, mailpit = os.environ["E2E_AUTH_URL"], os.environ["E2E_MAILPIT_URL"]
    email = f"{uuid4().hex}@example.org"
    async with httpx.AsyncClient(timeout=10) as http:
        response = await http.post(auth + "/auth/register", json={
            "username": email, "password": "test-pass-123", "first_name": "Анна", "last_name": "Иванова"})
        assert response.status_code == 201, response.text
        user_id = response.json()["id"]

        async def messages():
            result = await http.get(mailpit + "/api/v1/search", params={"query": f"to:{email}"})
            result.raise_for_status()
            return result.json()["messages"]

        welcome = await wait_for(messages)
        detail = (await http.get(mailpit + "/api/v1/message/" + welcome[0]["ID"])).json()
        assert "Анна" in detail["Text"]
        key = str(uuid4())
        payload = {"request_id": key, "template_id": template, "audience": {"kind": "users", "user_ids": [user_id]},
                   "data": {"headline": "Премьера", "message": "Новый фильм"}}
        headers = {"Authorization": "Bearer " + "o" * 32}
        first = await http.post(api + "/api/v1/campaigns", json=payload, headers=headers)
        second = await http.post(api + "/api/v1/campaigns", json=payload, headers=headers)
        assert first.status_code == second.status_code == 202
        assert first.json()["id"] == second.json()["id"]

        async def sent():
            result = await http.get(api + "/api/v1/requests/" + first.json()["id"], headers=headers)
            return result.json()["deliveries"]["sent"] == 1

        await wait_for(sent)
        assert len(await messages()) == 2


async def test_real_user_preferences_and_urgent_during_bulk():
    api = os.getenv('E2E_NOTIFICATION_URL')
    if not api:
        pytest.skip('Full Compose stack required')
    auth, mailpit = os.environ['E2E_AUTH_URL'], os.environ['E2E_MAILPIT_URL']
    email = f'{uuid4().hex}@example.org'
    operator = {'Authorization': 'Bearer ' + 'o' * 32}
    async with httpx.AsyncClient(timeout=15) as http:
        registered = await http.post(auth + '/auth/register', json={'username': email, 'password': 'test-pass-123'})
        assert registered.status_code == 201
        user_id = registered.json()['id']
        login = await http.post(auth + '/auth/login', data={'username': email, 'password': 'test-pass-123'})
        assert login.status_code == 200, login.text
        user = {'Authorization': 'Bearer ' + login.json()['access_token']}
        preferences = await http.put(api + '/api/v1/preferences/me', json={'categories': {'marketing': False}},
                                     headers=user)
        assert preferences.status_code == 200, preferences.text

        async def submit(label, priority='bulk'):
            response = await http.post(api + '/api/v1/campaigns', headers=operator, json={
                'request_id': str(uuid4()), 'template_id': 'announcement', 'priority': priority,
                'audience': {'kind': 'users', 'user_ids': [user_id]},
                'data': {'headline': label, 'message': label}})
            assert response.status_code == 202, response.text
            return response.json()['id']

        skipped = await submit('Disabled-' + uuid4().hex)

        async def skipped_status():
            response = await http.get(api + '/api/v1/requests/' + skipped, headers=operator)
            return response.json()['deliveries']['skipped'] == 1

        await wait_for(skipped_status)
        assert (await http.put(api + '/api/v1/preferences/me', json={'enabled': True}, headers=user)).status_code == 200
        # A bounded burst tests concurrent real SMTP paths, not an unlimited-load guarantee.
        bulk_tasks = [asyncio.create_task(submit('Bulk-' + uuid4().hex)) for _ in range(30)]
        title = 'Urgent-' + uuid4().hex
        started = asyncio.get_running_loop().time()
        urgent_id = await submit(title, 'urgent')

        async def urgent_arrived():
            response = await http.get(mailpit + '/api/v1/search', params={'query': f'to:{email} subject:{title}'})
            response.raise_for_status()
            return response.json()['messages']

        try:
            assert len(await wait_for(urgent_arrived, timeout=180)) == 1
            elapsed = asyncio.get_running_loop().time() - started
            print(f'urgent SMTP arrival during 30 bulk requests: {elapsed:.2f}s')
            assert elapsed < 180
        finally:
            bulk_ids = await asyncio.gather(*bulk_tasks)

        async def drained():
            states = await asyncio.gather(*[http.get(api + '/api/v1/requests/' + value, headers=operator)
                                            for value in [urgent_id, *bulk_ids]])
            return all(value.json()['deliveries']['sent'] == 1 for value in states)

        await wait_for(drained, timeout=180)


async def test_movie_published_reaches_all_registered_recipients():
    api = os.getenv('E2E_NOTIFICATION_URL')
    if not api:
        pytest.skip('Full Compose stack required')
    auth = os.environ['E2E_AUTH_URL']
    mailpit = os.environ['E2E_MAILPIT_URL']
    async with httpx.AsyncClient(timeout=10) as http:
        emails = [f'{uuid4().hex}@example.org' for _ in range(2)]
        for email in emails:
            result = await http.post(auth + '/auth/register', json={
                'username': email, 'password': 'test-pass-123'})
            assert result.status_code == 201
        title = 'Premiere-' + uuid4().hex
        event = {'event_id': str(uuid4()), 'event_type': 'movie.published',
                 'occurred_at': '2026-09-22T10:00:00Z', 'payload': {
                'movie_id': str(uuid4()), 'title': title, 'url': 'https://cinema.example.org/new'}}
        response = await http.post(api + '/api/v1/events', json=event,
                                   headers={'Authorization': 'Bearer ' + 'c' * 32})
        assert response.status_code == 202
        for email in emails:
            async def find_movie():
                response = await http.get(mailpit + '/api/v1/search', params={'query': f'to:{email} subject:{title}'})
                response.raise_for_status()
                return response.json()['messages']

            matches = await wait_for(find_movie)
            assert len(matches) == 1
