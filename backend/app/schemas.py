"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None


class FlushBatchPayload(BaseModel):
    """聚合批次写入请求：复核人员在聚合任务页面发起。"""
    task_name: str
    batch_id: str | None = None
    records: list[dict[str, Any]] | None = None


class RetryBatchPayload(BaseModel):
    batch_id: str


class MigrationSwitchPayload(BaseModel):
    """序号34迁移切换请求：默认切换序号为 34。"""
    switch_id: str = "34"
    task_name: str | None = None
