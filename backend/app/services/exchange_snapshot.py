"""目录交换快照服务：支撑数据表目录跨环境交接的结构导入导出。

domain_chain: 目录交换快照 (exchange_snapshot)
  └─ 导出 → 带源类型的结构快照 JSON
  └─ 导入 → 表目录登记 (table_catalog) + 字段画像 (column_profile)
  └─ 适配 → 元数据变更待核清单 (metadata_diff)
  └─ 核对 → 变更对比按次核对 (metadata_diff)

同一份快照包含两类字段：
1. 可直接对应的字段：源类型与目标类型一致，原样登记；
2. 需适配字段：典型为源端 ``TIMESTAMP WITH TIME ZONE``（带时区时间戳），
   目标库仅允许无时区 ``DATE``，按规则降精度适配，并保留原始字段类型与适配说明。

类型转换失败的字段进入"未解决"清单，不阻断整次导入，可携带人工目标类型重试。
同一文件（内容指纹相同）并发/重复导入只保留一个有效结构。
"""
from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.metadata_diff import MetadataDiffService
from app.store import store

# 源类型 → 目标库允许类型的适配规则。
# 目标库只允许无时区日期/时间类型，带时区时间戳统一降精度为 DATE。
TYPE_ADAPTATION_RULES: dict[str, dict[str, str]] = {
    "TIMESTAMP WITH TIME ZONE": {
        "目标类型": "DATE",
        "适配说明": "源端为带时区时间戳，目标库仅允许无时区日期，按UTC归一后截取日期部分，丢弃时区与时分秒",
    },
    "TIMESTAMPTZ": {
        "目标类型": "DATE",
        "适配说明": "源端为带时区时间戳，目标库仅允许无时区日期，按UTC归一后截取日期部分，丢弃时区与时分秒",
    },
    "BIGINT": {"目标类型": "BIGINT", "适配说明": "源类型与目标类型一致，直接对应"},
    "INTEGER": {"目标类型": "INTEGER", "适配说明": "源类型与目标类型一致，直接对应"},
    "TINYINT": {"目标类型": "TINYINT", "适配说明": "源类型与目标类型一致，直接对应"},
    "VARCHAR(255)": {"目标类型": "VARCHAR(255)", "适配说明": "源类型与目标类型一致，直接对应"},
    "VARCHAR(64)": {"目标类型": "VARCHAR(64)", "适配说明": "源类型与目标类型一致，直接对应"},
    "DECIMAL(18,2)": {"目标类型": "DECIMAL(18,2)", "适配说明": "源类型与目标类型一致，直接对应"},
    "NUMERIC": {"目标类型": "NUMERIC", "适配说明": "源类型与目标类型一致，直接对应"},
    "DOUBLE": {"目标类型": "DOUBLE", "适配说明": "源类型与目标类型一致，直接对应"},
    "TEXT": {"目标类型": "TEXT", "适配说明": "源类型与目标类型一致，直接对应"},
    "STRING": {"目标类型": "STRING", "适配说明": "源类型与目标类型一致，直接对应"},
    "DATE": {"目标类型": "DATE", "适配说明": "源类型与目标类型一致，直接对应"},
    "TIMESTAMP": {"目标类型": "TIMESTAMP", "适配说明": "源类型与目标类型一致，直接对应"},
}

SNAPSHOT_FORMAT = "catalog-exchange-snapshot/v1"


class ExchangeSnapshotService(BaseModuleService):
    MODULE = "exchange_snapshot"
    ENTITY = "目录交换快照"
    OPERATION_TERM = "交接"
    LIST_FIELDS = ["快照编号", "快照名称", "源环境", "目标环境", "表名称", "源表类型",
                   "文件指纹", "导入批次", "字段总数", "已适配字段", "未解决字段", "导入时间", "状态"]
    REQUIRED_FIELDS = ["快照编号", "表名称", "文件指纹"]
    STATUS_ORDER = ["已导入", "存在未解决项", "已重试", "已核对"]
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    # 同一文件并发导入串行化，保证只留下一个有效结构。
    _import_lock = threading.Lock()

    # ------------------------------------------------------------------ 导出
    def export_snapshot(self, table_name: str, source_env: str = "prod-mysql",
                        target_env: str = "dr-hive") -> dict[str, Any]:
        """把目录中已登记的表结构导出为目录交换快照。

        快照中每个字段都携带源端类型（``源数据类型``）；若目录登记时保留了
        原始类型（跨环境导入产生的表），优先使用原始类型，保证快照可再次交接。
        """
        catalog_rows = store.rows("table_catalog")
        table_entry = next(
            (r for r in catalog_rows if r.get("表名称") == table_name), None)
        if table_entry is None:
            raise KeyError(f"表目录中不存在表：{table_name}")

        column_rows = store.rows("catalog_column")
        columns = [dict(r) for r in column_rows if r.get("表名称") == table_name]
        if not columns:
            # 目录里只有样例登记、没有字段级结构时，合成一份含带时区时间戳的结构，
            # 让导出链路本身可独立演示。
            columns = self._fallback_columns(table_name)

        snapshot_columns = []
        for col in columns:
            source_type = col.get("原始字段类型") or col.get("数据类型") or ""
            snapshot_columns.append({
                "字段名称": col.get("字段名称", ""),
                "源数据类型": source_type,
                "可为空": bool(col.get("可为空", True)),
                "是否主键": bool(col.get("是否主键", False)),
                "适配说明": col.get("适配说明", ""),
            })

        snapshot = {
            "格式版本": SNAPSHOT_FORMAT,
            "快照名称": f"{table_name}_交换快照",
            "源环境": source_env,
            "目标环境": target_env,
            "源数据源类型": table_entry.get("所属库名", ""),
            "导出时间": datetime.now(timezone.utc).isoformat(),
            "表清单": [{
                "表名称": table_name,
                "表类型": table_entry.get("表类型", "TABLE"),
                "字段列表": snapshot_columns,
            }],
        }
        snapshot["文件指纹"] = self.fingerprint(snapshot)
        return snapshot

    def _fallback_columns(self, table_name: str) -> list[dict[str, Any]]:
        return [
            {"字段名称": "id", "数据类型": "BIGINT", "原始字段类型": "BIGINT",
             "可为空": False, "是否主键": True, "适配说明": ""},
            {"字段名称": "order_no", "数据类型": "VARCHAR(64)", "原始字段类型": "VARCHAR(64)",
             "可为空": False, "是否主键": False, "适配说明": ""},
            {"字段名称": "pay_time", "数据类型": "DATE",
             "原始字段类型": "TIMESTAMP WITH TIME ZONE",
             "可为空": False, "是否主键": False,
             "适配说明": TYPE_ADAPTATION_RULES["TIMESTAMP WITH TIME ZONE"]["适配说明"]},
            {"字段名称": "amount", "数据类型": "DECIMAL(18,2)",
             "原始字段类型": "DECIMAL(18,2)", "可为空": True, "是否主键": False,
             "适配说明": ""},
        ]

    # ------------------------------------------------------------------ 导入
    def import_snapshot(self, snapshot: dict[str, Any], target_env: str | None = None,
                        import_remark: str | None = None) -> dict[str, Any]:
        """把目录交换快照导入为当前目录的有效结构。

        幂等：按文件内容指纹去重，同一文件重复/并发导入只产生一个有效结构，
        后续导入返回首次导入的结论（标记为重复导入）。
        """
        normalized = self._validate_snapshot(snapshot)
        fingerprint_ = self.fingerprint(normalized)
        now = datetime.now(timezone.utc).isoformat()

        with self._import_lock:
            existing = self._find_valid_import(fingerprint_)
            if existing is not None:
                return self._summarize(existing, duplicate=True,
                                       remark="同一文件已导入，复用首次导入的有效结构")

            batch_hash = hashlib.md5(
                f"{fingerprint_}:{now}".encode()).hexdigest()[:8]
            snapshot_rows = store.rows(self.MODULE)
            new_id = max((int(r.get("id", 0)) for r in snapshot_rows), default=0) + 1

            per_table_results = []
            total_resolved = 0
            total_unresolved = 0
            for table in normalized["表清单"]:
                result = self._import_one_table(
                    table, normalized, batch_hash, new_id,
                    target_env or normalized.get("目标环境", ""))
                per_table_results.append(result)
                total_resolved += result["已适配字段"]
                total_unresolved += result["未解决字段"]

            status = "存在未解决项" if total_unresolved else "已导入"
            entry = {
                "id": new_id,
                "快照编号": f"EXCH-{new_id:04d}",
                "快照名称": normalized.get("快照名称", ""),
                "源环境": normalized.get("源环境", ""),
                "目标环境": target_env or normalized.get("目标环境", ""),
                "表名称": ",".join(t["表名称"] for t in normalized["表清单"]),
                "源表类型": ",".join(str(t.get("表类型", "")) for t in normalized["表清单"]),
                "文件指纹": fingerprint_,
                "导入批次": batch_hash,
                "字段总数": total_resolved + total_unresolved,
                "已适配字段": total_resolved,
                "未解决字段": total_unresolved,
                "导入时间": now,
                "导入备注": import_remark or "",
                "有效结构": True,
                "status": status,
                "pending": total_unresolved > 0,
                "abnormal": total_unresolved > 0,
                "表明细": per_table_results,
            }
            snapshot_rows.append(entry)
            return self._summarize(entry, duplicate=False)

    def _import_one_table(self, table: dict, snapshot: dict, batch_hash: str,
                          snapshot_id: int, target_env: str) -> dict[str, Any]:
        table_name = table["表名称"]
        adapted_columns: list[dict[str, Any]] = []
        unresolved_columns: list[dict[str, Any]] = []
        adaptation_details: list[dict[str, Any]] = []

        for col in table["字段列表"]:
            col_name = col.get("字段名称", "")
            source_type = (col.get("源数据类型") or col.get("数据类型") or "").strip()
            rule = TYPE_ADAPTATION_RULES.get(source_type)
            detail = {
                "字段名称": col_name,
                "原始字段类型": source_type,
                "目标字段类型": rule["目标类型"] if rule else "",
                "适配说明": rule["适配说明"] if rule else "",
                "可空": bool(col.get("可为空", True)),
                "主键": bool(col.get("是否主键", False)),
                "是否解决": rule is not None,
                "失败原因": "" if rule else f"目标库不支持源类型 {source_type}，无可用适配规则",
            }
            adaptation_details.append(detail)
            if rule is None:
                unresolved_columns.append(detail)
                continue

            adapted_columns.append({
                "字段名称": col_name,
                "数据类型": rule["目标类型"],
                "原始字段类型": source_type,
                "适配说明": rule["适配说明"],
                "可为空": detail["可空"],
                "是否主键": detail["主键"],
            })

        # 仅把成功适配的字段登记为目录有效结构，未解决列保留在快照记录中等待重试。
        catalog_rows = store.rows("table_catalog")
        table_id = max((int(r.get("id", 0)) for r in catalog_rows), default=0) + 1
        catalog_entry = {
            "id": table_id,
            "表编号": f"TABL-{table_id:04d}",
            "表名称": table_name,
            "所属库名": target_env or snapshot.get("目标环境", ""),
            "表类型": table.get("表类型", "TABLE"),
            "字段数量": len(adapted_columns),
            "数据行数": "0",
            "存储大小": "0 KB",
            "最后更新": datetime.now(timezone.utc).isoformat(),
            "来源快照": f"EXCH-{snapshot_id:04d}",
            "导入批次": batch_hash,
            "status": "已注册",
            "pending": False,
            "abnormal": False,
        }
        catalog_rows.append(catalog_entry)

        # 字段级登记：保存原始字段类型与适配说明，供画像消费与后续核对。
        column_rows = store.rows("catalog_column")
        for col in adapted_columns:
            col_id = max((int(r.get("id", 0)) for r in column_rows), default=0) + 1
            column_rows.append({
                "id": col_id,
                "字段登记编号": f"COLM-{col_id:04d}",
                "表名称": table_name,
                "导入批次": batch_hash,
                "来源快照": f"EXCH-{snapshot_id:04d}",
                **col,
                "status": "已登记",
                "pending": False,
                "abnormal": False,
            })

        # 只有成功适配、已登记到目录的字段才进入字段画像（实际供画像消费的结构）。
        profile_count = self._dispatch_profiles(table_name, adapted_columns)

        # 适配结论回写元数据变更待核清单。
        MetadataDiffService().record_adaptation_review(
            table_name=table_name,
            batch_hash=batch_hash,
            snapshot_id=f"EXCH-{snapshot_id:04d}",
            adaptation_details=adaptation_details,
        )

        return {
            "表名称": table_name,
            "已适配字段": len(adapted_columns),
            "未解决字段": len(unresolved_columns),
            "画像任务数": profile_count,
            "适配明细": adaptation_details,
            "未解决列": unresolved_columns,
        }

    def _dispatch_profiles(self, table_name: str, columns: list[dict]) -> int:
        profile_rows = store.rows("column_profile")
        for col in columns:
            new_pid = max((int(r.get("id", 0)) for r in profile_rows), default=0) + 1
            profile_rows.append({
                "id": new_pid,
                "画像编号": f"COLU-{new_pid:04d}",
                "关联表名": table_name,
                "字段名称": col["字段名称"],
                "数据类型": col["数据类型"],
                "原始字段类型": col.get("原始字段类型", ""),
                "空值比例": "0.00%",
                "唯一值数": "待计算",
                "最小值": "待计算",
                "最大值": "待计算",
                "画像版本": "v1.0",
                "status": "计算中",
                "pending": True,
                "abnormal": False,
            })
        return len(columns)

    # ------------------------------------------------------------------ 重试
    def retry_unresolved(self, snapshot_id: int,
                         type_overrides: dict[str, str] | None = None) -> dict[str, Any]:
        """对类型转换失败的字段重试适配，保留仍未解决的列。

        ``type_overrides`` 为人工裁定的 字段名 → 目标类型 映射；
        未提供覆盖的未解决列继续保留在未解决清单中，不丢失。
        """
        entry = store.find(self.MODULE, snapshot_id)
        if entry is None:
            raise KeyError(f"快照 {snapshot_id} 不存在")
        type_overrides = type_overrides or {}

        resolved_now: list[dict[str, Any]] = []
        still_unresolved: list[dict[str, Any]] = []
        for table_result in entry["表明细"]:
            for col in list(table_result["未解决列"]):
                override = type_overrides.get(col["字段名称"])
                if not override:
                    still_unresolved.append(col)
                    continue
                resolved = dict(col)
                resolved["目标字段类型"] = override
                resolved["适配说明"] = f"人工重试适配：源类型 {col['原始字段类型']} 裁定为目标类型 {override}"
                resolved["是否解决"] = True
                resolved["失败原因"] = ""
                resolved_now.append((table_result["表名称"], resolved))
                table_result["适配明细"] = [
                    resolved if d["字段名称"] == col["字段名称"] else d
                    for d in table_result["适配明细"]
                ]
            table_result["未解决列"] = [
                c for c in table_result["未解决列"]
                if c["字段名称"] not in {name for name, _ in resolved_now}
            ]

        # 重试成功的字段补登记到目录与画像，并回写待核清单结论。
        column_rows = store.rows("catalog_column")
        for table_name, col in resolved_now:
            col_id = max((int(r.get("id", 0)) for r in column_rows), default=0) + 1
            column_rows.append({
                "id": col_id,
                "字段登记编号": f"COLM-{col_id:04d}",
                "表名称": table_name,
                "导入批次": entry["导入批次"],
                "来源快照": entry["快照编号"],
                "字段名称": col["字段名称"],
                "数据类型": col["目标字段类型"],
                "原始字段类型": col["原始字段类型"],
                "适配说明": col["适配说明"],
                "可为空": col["可空"],
                "是否主键": col["主键"],
                "status": "已登记",
                "pending": False,
                "abnormal": False,
            })
            catalog = next(
                (r for r in store.rows("table_catalog")
                 if r.get("表名称") == table_name and r.get("导入批次") == entry["导入批次"]),
                None)
            if catalog is not None:
                catalog["字段数量"] = int(catalog.get("字段数量", 0)) + 1
            self._dispatch_profiles(table_name, [{
                "字段名称": col["字段名称"],
                "数据类型": col["目标字段类型"],
                "原始字段类型": col["原始字段类型"],
            }])
            MetadataDiffService().resolve_review_item(
                table_name=table_name,
                batch_hash=entry["导入批次"],
                field_name=col["字段名称"],
                target_type=col["目标字段类型"],
                note=col["适配说明"],
            )

        entry["已适配字段"] = entry["已适配字段"] + len(resolved_now)
        entry["未解决字段"] = entry["未解决字段"] - len(resolved_now)
        entry["pending"] = entry["未解决字段"] > 0
        entry["abnormal"] = entry["未解决字段"] > 0
        entry["status"] = "存在未解决项" if entry["未解决字段"] else "已重试"
        return {
            "快照编号": entry["快照编号"],
            "本次解决": len(resolved_now),
            "仍未解决": entry["未解决字段"],
            "未解决列": still_unresolved,
            "状态": entry["status"],
        }

    # ------------------------------------------------------------------ 核对
    def reconcile_snapshot(self, snapshot_id: int) -> dict[str, Any]:
        """按次核对：上传文件中的源类型 vs 已登记到目录的字段。

        以单次导入（快照编号 + 导入批次）为核对单位，逐字段比对：
        - 快照中的源类型是否与字段登记保留的原始字段类型一致；
        - 适配后的目标类型是否真正落到目录字段登记与画像消费结构中；
        - 未解决项是否仍停留在待核清单。
        """
        entry = store.find(self.MODULE, snapshot_id)
        if entry is None:
            raise KeyError(f"快照 {snapshot_id} 不存在")

        column_rows = store.rows("catalog_column")
        profile_rows = store.rows("column_profile")
        checks: list[dict[str, Any]] = []

        for table_result in entry["表明细"]:
            table_name = table_result["表名称"]
            registered = {
                r["字段名称"]: r for r in column_rows
                if r.get("表名称") == table_name and r.get("导入批次") == entry["导入批次"]
            }
            profiled = {
                r["字段名称"]: r for r in profile_rows
                if r.get("关联表名") == table_name
            }
            for detail in table_result["适配明细"]:
                field = detail["字段名称"]
                reg = registered.get(field)
                if not detail["是否解决"]:
                    checks.append({
                        "表名称": table_name,
                        "字段名称": field,
                        "源类型": detail["原始字段类型"],
                        "目录类型": "",
                        "画像类型": "",
                        "核对结论": "未解决：未登记到目录，保留在待核清单",
                        "一致": False,
                    })
                    continue
                profile = profiled.get(field)
                source_match = bool(reg) and reg.get("原始字段类型") == detail["原始字段类型"]
                target_match = bool(reg) and reg.get("数据类型") == detail["目标字段类型"]
                profile_match = bool(profile) and profile.get("数据类型") == detail["目标字段类型"]
                consistent = source_match and target_match and profile_match
                checks.append({
                    "表名称": table_name,
                    "字段名称": field,
                    "源类型": detail["原始字段类型"],
                    "目录类型": reg.get("数据类型", "") if reg else "",
                    "画像类型": profile.get("数据类型", "") if profile else "",
                    "核对结论": "一致：源类型已留存，目标类型已登记并供画像消费"
                    if consistent else "不一致：目录登记与快照适配结论存在偏差",
                    "一致": consistent,
                })

        consistent_count = sum(1 for c in checks if c["一致"])
        unresolved_count = sum(1 for c in checks if not c["一致"] and c["目录类型"] == "")
        result = MetadataDiffService().reconcile_review_batch(
            entry["导入批次"], checks)
        if unresolved_count == 0 and all(c["一致"] for c in checks):
            entry["status"] = "已核对"
            entry["pending"] = False
            entry["abnormal"] = False

        return {
            "快照编号": entry["快照编号"],
            "导入批次": entry["导入批次"],
            "核对字段数": len(checks),
            "一致字段数": consistent_count,
            "未解决项数": unresolved_count,
            "全部一致": all(c["一致"] for c in checks),
            "核对明细": checks,
            "待核清单回写": result,
        }

    # ------------------------------------------------------------------ 工具
    @staticmethod
    def fingerprint(snapshot: dict[str, Any]) -> str:
        """按快照内容计算指纹：忽略导出时间等易变字段，保证同文件同指纹。"""
        payload = {
            "格式版本": snapshot.get("格式版本"),
            "快照名称": snapshot.get("快照名称"),
            "源环境": snapshot.get("源环境"),
            "目标环境": snapshot.get("目标环境"),
            "源数据源类型": snapshot.get("源数据源类型"),
            "表清单": [
                {
                    "表名称": t.get("表名称"),
                    "表类型": t.get("表类型"),
                    "字段列表": [
                        {
                            "字段名称": c.get("字段名称"),
                            "源数据类型": c.get("源数据类型") or c.get("数据类型"),
                            "可为空": c.get("可为空"),
                            "是否主键": c.get("是否主键"),
                        }
                        for c in t.get("字段列表", [])
                    ],
                }
                for t in snapshot.get("表清单", [])
            ],
        }
        canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                               separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def _validate_snapshot(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        if not isinstance(snapshot, dict):
            raise ValueError("快照必须是 JSON 对象")
        tables = snapshot.get("表清单")
        if not isinstance(tables, list) or not tables:
            raise ValueError("快照缺少表清单或表清单为空")
        for table in tables:
            if not table.get("表名称"):
                raise ValueError("快照中存在缺少表名称的表")
            cols = table.get("字段列表")
            if not isinstance(cols, list) or not cols:
                raise ValueError(f"表 {table.get('表名称')} 缺少字段列表")
            for col in cols:
                if not col.get("字段名称"):
                    raise ValueError(f"表 {table.get('表名称')} 存在无名称字段")
                if not (col.get("源数据类型") or col.get("数据类型")):
                    raise ValueError(
                        f"字段 {col.get('字段名称')} 缺少源数据类型")
        return snapshot

    def _find_valid_import(self, fingerprint_: str) -> dict[str, Any] | None:
        return next(
            (r for r in store.rows(self.MODULE)
             if r.get("文件指纹") == fingerprint_ and r.get("有效结构")),
            None)

    def _summarize(self, entry: dict[str, Any], duplicate: bool,
                   remark: str = "") -> dict[str, Any]:
        return {
            "快照编号": entry["快照编号"],
            "文件指纹": entry["文件指纹"],
            "导入批次": entry["导入批次"],
            "字段总数": entry["字段总数"],
            "已适配字段": entry["已适配字段"],
            "未解决字段": entry["未解决字段"],
            "状态": entry["status"],
            "重复导入": duplicate,
            "有效结构唯一": True,
            "说明": remark,
            "表明细": entry["表明细"],
        }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "交接"
        snapshot["desc"] = "跨环境导入导出表结构并保留源类型适配结论"
        snapshot["artifact"] = "目录交换快照"
        snapshot["artifact_desc"] = "带源类型与适配说明的目录交换快照"
        return snapshot
