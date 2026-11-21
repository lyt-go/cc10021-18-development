"""目录交换快照接口：表结构跨环境交接的导出、导入、重试与按次核对。"""
from __future__ import annotations

from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.services.exchange_snapshot import ExchangeSnapshotService

router = APIRouter(prefix="/api/table_catalog/exchange", tags=["目录交换快照"])

service = ExchangeSnapshotService()


class SnapshotPayload(BaseModel):
    snapshot: dict[str, Any]
    target_env: str | None = None
    remark: str | None = None


class RetryPayload(BaseModel):
    type_overrides: dict[str, str] = Field(default_factory=dict)
    remark: str | None = None


@router.post("/snapshots/import")
def import_snapshot(payload: SnapshotPayload) -> dict[str, Any]:
    """把带源类型的目录交换快照导入为当前目录的有效结构。"""
    try:
        return service.import_snapshot(
            payload.snapshot, target_env=payload.target_env,
            import_remark=payload.remark)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/snapshots/{snapshot_id}/retry")
def retry_snapshot(snapshot_id: int, payload: RetryPayload) -> dict[str, Any]:
    """对类型转换失败的字段重试适配，未解决列继续保留。"""
    try:
        return service.retry_unresolved(snapshot_id, payload.type_overrides)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/snapshots/{snapshot_id}/reconcile")
def reconcile_snapshot(snapshot_id: int) -> dict[str, Any]:
    """按次核对上传文件源类型与已登记到目录的字段。"""
    try:
        return service.reconcile_snapshot(snapshot_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/snapshots/export")
def export_snapshot(table_name: str, source_env: str = "prod-mysql",
                    target_env: str = "dr-hive") -> JSONResponse:
    """导出一张表为带源类型的目录交换快照 JSON。"""
    try:
        snapshot = service.export_snapshot(table_name, source_env, target_env)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return JSONResponse(
        snapshot,
        headers={
            "Content-Disposition":
                "attachment; filename=\"snapshot.json\"; "
                f"filename*=UTF-8''snapshot_{quote(table_name)}.json"
        },
    )
