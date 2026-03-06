"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import UTC, datetime
from typing import Any, cast

import httpx
from sqlmodel import func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner
from app.settings import settings

JsonDict = dict[str, Any]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _ensure_200(response: httpx.Response) -> None:
    """Raise an HTTPStatusError unless the response status is exactly 200."""
    if response.status_code != 200:
        raise httpx.HTTPStatusError(
            f"Expected status 200, got {response.status_code}",
            request=response.request,
            response=response,
        )


def _parse_iso_datetime(value: str) -> datetime:
    """Parse API timestamps and store them as naive UTC datetimes."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(UTC).replace(tzinfo=None)


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[JsonDict]:
    """Fetch the lab/task catalog from the autochecker API.

    TODO: Implement this function.
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    url = f"{settings.autochecker_api_url}/api/items"
    auth = (settings.autochecker_email, settings.autochecker_password)

    async with httpx.AsyncClient() as client:
        response = await client.get(url, auth=auth)

    _ensure_200(response)
    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError("Unexpected /api/items payload: expected a JSON array")
    payload_list = cast(list[Any], payload)
    if not all(isinstance(item, dict) for item in payload_list):
        raise ValueError("Unexpected /api/items payload: array elements must be objects")
    return cast(list[JsonDict], payload_list)


async def fetch_logs(since: datetime | None = None) -> list[JsonDict]:
    """Fetch check results from the autochecker API.

    TODO: Implement this function.
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handle pagination: keep fetching while has_more is True
      - Use the submitted_at of the last log as the new "since" value
    - Return the combined list of all log dicts from all pages
    """
    url = f"{settings.autochecker_api_url}/api/logs"
    auth = (settings.autochecker_email, settings.autochecker_password)
    collected: list[JsonDict] = []

    cursor = since.isoformat() if since is not None else None

    async with httpx.AsyncClient() as client:
        while True:
            params: dict[str, str | int] = {"limit": 500}
            if cursor is not None:
                params["since"] = cursor

            response = await client.get(url, params=params, auth=auth)
            _ensure_200(response)

            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("Unexpected /api/logs payload: expected a JSON object")
            payload_dict = cast(JsonDict, payload)

            page_logs_raw = payload_dict.get("logs", [])
            if not isinstance(page_logs_raw, list):
                raise ValueError("Unexpected /api/logs payload: 'logs' must be a JSON array")
            page_logs_list = cast(list[Any], page_logs_raw)
            page_logs: list[JsonDict] = [
                cast(JsonDict, log) for log in page_logs_list if isinstance(log, dict)
            ]
            collected.extend(page_logs)

            has_more = bool(payload_dict.get("has_more", False))
            if not has_more or len(page_logs) == 0:
                break

            last_submitted_at = page_logs[-1].get("submitted_at")
            if not isinstance(last_submitted_at, str):
                break
            cursor = last_submitted_at

    return collected


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[JsonDict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    TODO: Implement this function.
    - Import ItemRecord from app.models.item
    - Process labs first (items where type="lab"):
      - For each lab, check if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERT a new ItemRecord(type="lab", title=lab_title)
    - Then process tasks (items where type="task"):
      - Find the parent lab item by matching the lab field to a lab's title
      - Check if a task with this title and parent_id already exists
      - If not, INSERT a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commit after all inserts
    - Return the number of newly created items
    """
    created = 0
    labs_by_title: dict[str, ItemRecord] = {}

    for raw in items:
        if raw.get("type") != "lab":
            continue

        lab_title = raw.get("title")
        if not isinstance(lab_title, str):
            continue

        cached_lab = labs_by_title.get(lab_title)
        if cached_lab is not None:
            continue

        existing_lab_result = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "lab",
                ItemRecord.title == lab_title,
            )
        )
        lab_item = existing_lab_result.first()
        if lab_item is None:
            lab_item = ItemRecord(type="lab", title=lab_title)
            session.add(lab_item)
            await session.flush()
            created += 1

        labs_by_title[lab_title] = lab_item

    for raw in items:
        if raw.get("type") != "task":
            continue

        task_title = raw.get("title")
        lab_title = raw.get("lab")
        if not isinstance(task_title, str) or not isinstance(lab_title, str):
            continue

        lab_item = labs_by_title.get(lab_title)
        if lab_item is None:
            existing_lab_result = await session.exec(
                select(ItemRecord).where(
                    ItemRecord.type == "lab",
                    ItemRecord.title == lab_title,
                )
            )
            lab_item = existing_lab_result.first()
            if lab_item is None:
                continue
            labs_by_title[lab_title] = lab_item

        existing_task_result = await session.exec(
            select(ItemRecord).where(
                ItemRecord.type == "task",
                ItemRecord.title == task_title,
                ItemRecord.parent_id == lab_item.id,
            )
        )
        existing_task = existing_task_result.first()
        if existing_task is not None:
            continue

        task_item = ItemRecord(type="task", title=task_title, parent_id=lab_item.id)
        session.add(task_item)
        await session.flush()
        created += 1

    await session.commit()
    return created


async def load_logs(logs: list[JsonDict], session: AsyncSession) -> int:
    """Load interaction logs into the database.

    TODO: Implement this function.
    - Import Learner from app.models.learner
    - Import InteractionLog from app.models.interaction
    - Import ItemRecord from app.models.item
    - For each log dict:
      1. Find or create a Learner by external_id (log["student_id"])
         - If creating, set student_group from log["group"]
      2. Find the matching item:
         - If log["task"] is not None, find the task item by title
         - Otherwise, find the lab item by title (from log["lab"])
         - Skip this log if no matching item is found
      3. Check if an InteractionLog with this external_id already exists
         (for idempotent upsert — skip if it does)
      4. Create InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commit after all inserts
    - Return the number of newly created interactions
    """
    created = 0
    learners_by_external_id: dict[str, Learner] = {}
    items_by_key: dict[tuple[str, str], ItemRecord] = {}

    for raw in logs:
        student_id = raw.get("student_id")
        if not isinstance(student_id, str):
            continue

        learner = learners_by_external_id.get(student_id)
        if learner is None:
            learner_result = await session.exec(
                select(Learner).where(Learner.external_id == student_id)
            )
            learner = learner_result.first()
            if learner is None:
                group = raw.get("group")
                learner = Learner(
                    external_id=student_id,
                    student_group=group if isinstance(group, str) else "",
                )
                session.add(learner)
                await session.flush()
            learners_by_external_id[student_id] = learner

        task_title = raw.get("task")
        lab_title = raw.get("lab")
        if task_title is not None:
            if not isinstance(task_title, str):
                continue
            item_key = ("task", task_title)
            item_type = "task"
            item_title = task_title
        else:
            if not isinstance(lab_title, str):
                continue
            item_key = ("lab", lab_title)
            item_type = "lab"
            item_title = lab_title

        item = items_by_key.get(item_key)
        if item is None:
            item_result = await session.exec(
                select(ItemRecord).where(
                    ItemRecord.type == item_type,
                    ItemRecord.title == item_title,
                )
            )
            item = item_result.first()
            if item is None:
                continue
            items_by_key[item_key] = item
        if item.id is None:
            continue

        external_id = raw.get("id")
        if isinstance(external_id, bool):
            continue
        if isinstance(external_id, int):
            interaction_external_id = external_id
        elif isinstance(external_id, str):
            try:
                interaction_external_id = int(external_id)
            except ValueError:
                continue
        else:
            continue

        existing_interaction_result = await session.exec(
            select(InteractionLog).where(
                InteractionLog.external_id == interaction_external_id
            )
        )
        if existing_interaction_result.first() is not None:
            continue

        submitted_at = raw.get("submitted_at")
        if not isinstance(submitted_at, str):
            continue

        score = raw.get("score")
        passed = raw.get("passed")
        total = raw.get("total")
        if learner.id is None:
            continue

        interaction = InteractionLog(
            external_id=interaction_external_id,
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=(
                float(score)
                if isinstance(score, int | float) and not isinstance(score, bool)
                else None
            ),
            checks_passed=passed if isinstance(passed, int) else None,
            checks_total=total if isinstance(total, int) else None,
            created_at=_parse_iso_datetime(submitted_at),
        )
        session.add(interaction)
        await session.flush()
        created += 1

    await session.commit()
    return created


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict[str, int]:
    """Run the full ETL pipeline.

    TODO: Implement this function.
    - Step 1: Fetch items from the API and load them into the database
    - Step 2: Determine the last synced timestamp
      - Query the most recent created_at from InteractionLog
      - If no records exist, since=None (fetch everything)
    - Step 3: Fetch logs since that timestamp and load them
    - Return a dict: {"new_records": <number of new interactions>,
                      "total_records": <total interactions in DB>}
    """
    items = await fetch_items()
    await load_items(items, session)

    since = cast(
        datetime | None,
        (await session.exec(select(func.max(InteractionLog.created_at)))).one(),
    )

    logs = await fetch_logs(since)
    new_records = await load_logs(logs, session)

    total_records = (
        await session.exec(select(func.count()).select_from(InteractionLog))
    ).one()
    total = int(total_records)

    return {"new_records": new_records, "total_records": total}
