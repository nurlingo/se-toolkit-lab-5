"""Router for analytics endpoints.

Each endpoint performs SQL aggregation queries on the interaction data
populated by the ETL pipeline. All endpoints require a `lab` query
parameter to filter results by lab (e.g., "lab-01").
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import case, func
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.database import get_session
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord
from app.models.learner import Learner

router = APIRouter()


async def _get_task_ids(lab: str, session: AsyncSession) -> list[int]:
    """Find the lab item and return the IDs of its child task items."""
    # Convert "lab-04" to "Lab 04" for title matching
    search_term = lab.replace("-", " ").title()
    lab_result = await session.exec(
        select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title.contains(search_term),  # type: ignore[union-attr]
        )
    )
    lab_item = lab_result.first()
    if lab_item is None:
        raise HTTPException(status_code=404, detail=f"Lab '{lab}' not found")

    tasks_result = await session.exec(
        select(ItemRecord.id).where(
            ItemRecord.type == "task",
            ItemRecord.parent_id == lab_item.id,
        )
    )
    return list(tasks_result.all())


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Score distribution histogram for a given lab."""
    task_ids = await _get_task_ids(lab, session)

    bucket = case(
        (InteractionLog.score <= 25, "0-25"),
        (InteractionLog.score <= 50, "26-50"),
        (InteractionLog.score <= 75, "51-75"),
        else_="76-100",
    ).label("bucket")

    stmt = (
        select(bucket, func.count().label("count"))
        .where(
            InteractionLog.item_id.in_(task_ids),  # type: ignore[union-attr]
            InteractionLog.score.is_not(None),  # type: ignore[union-attr]
        )
        .group_by(bucket)
    )
    result = await session.exec(stmt)
    counts = {row.bucket: row.count for row in result.all()}

    buckets = ["0-25", "26-50", "51-75", "76-100"]
    return [{"bucket": b, "count": counts.get(b, 0)} for b in buckets]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-task pass rates for a given lab."""
    task_ids = await _get_task_ids(lab, session)

    stmt = (
        select(
            ItemRecord.title.label("task"),  # type: ignore[union-attr]
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count().label("attempts"),
        )
        .join(InteractionLog, InteractionLog.item_id == ItemRecord.id)
        .where(ItemRecord.id.in_(task_ids))  # type: ignore[union-attr]
        .group_by(ItemRecord.id, ItemRecord.title)
        .order_by(ItemRecord.title)
    )
    result = await session.exec(stmt)
    return [
        {"task": row.task, "avg_score": float(row.avg_score), "attempts": row.attempts}
        for row in result.all()
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Submissions per day for a given lab."""
    task_ids = await _get_task_ids(lab, session)

    date_col = func.date(InteractionLog.created_at).label("date")
    stmt = (
        select(date_col, func.count().label("submissions"))
        .where(InteractionLog.item_id.in_(task_ids))  # type: ignore[union-attr]
        .group_by(date_col)
        .order_by(date_col)
    )
    result = await session.exec(stmt)
    return [
        {"date": str(row.date), "submissions": row.submissions}
        for row in result.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    """Per-group performance for a given lab."""
    task_ids = await _get_task_ids(lab, session)

    stmt = (
        select(
            Learner.student_group.label("group"),  # type: ignore[union-attr]
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(func.distinct(Learner.id)).label("students"),
        )
        .join(Learner, InteractionLog.learner_id == Learner.id)
        .where(InteractionLog.item_id.in_(task_ids))  # type: ignore[union-attr]
        .group_by(Learner.student_group)
        .order_by(Learner.student_group)
    )
    result = await session.exec(stmt)
    return [
        {"group": row.group, "avg_score": float(row.avg_score), "students": row.students}
        for row in result.all()
    ]
