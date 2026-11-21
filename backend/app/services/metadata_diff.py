"""元数据变更对比服务：检测和记录表结构变更，驱动血缘图谱更新。

domain_chain: 元数据变更对比 (metadata_diff)
  └─ 校验 → 血缘图谱更新 (lineage_graph)
  └─ 关联 → 质量事件 (dq_incident)
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class MetadataDiffService(BaseModuleService):
    MODULE = "metadata_diff"
    ENTITY = "变更记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["变更编号", "变更对象", "变更类型", "旧值", "新值", "变更批次", "影响评估", "变更时间"]
    REQUIRED_FIELDS = ["变更编号", "变更对象", "变更类型"]
    STATUS_ORDER = ["草稿", "待审核", "已确认", "已回滚"]
    ACTION_RULES = {"提交审核": "待审核", "确认变更": "已确认", "执行回滚": "已回滚"}
    NEGATIVE_ACTIONS = []

    def compare_schemas(self, old_columns: list[dict], new_columns: list[dict],
                         table_name: str) -> dict[str, Any]:
        """对比两次元数据采集的表结构差异，生成变更记录。

        解析新老字段列表，识别新增、删除、类型变更等差异，
        校验变更影响范围并写入变更记录，触发血缘图谱的更新。
        """
        old_map = {c.get("字段名称"): c for c in old_columns if c.get("字段名称")}
        new_map = {c.get("字段名称"): c for c in new_columns if c.get("字段名称")}
        old_names = set(old_map.keys())
        new_names = set(new_map.keys())

        additions = new_names - old_names
        deletions = old_names - new_names
        common = old_names & new_names

        changes = []
        # 识别新增字段
        for col_name in additions:
            changes.append({"字段": col_name, "变更类型": "新增",
                            "旧值": "N/A",
                            "新值": f"类型:{new_map[col_name].get('数据类型', 'unknown')}"})
        # 识别删除字段
        for col_name in deletions:
            changes.append({"字段": col_name, "变更类型": "删除",
                            "旧值": f"类型:{old_map[col_name].get('数据类型', 'unknown')}",
                            "新值": "N/A"})
        # 识别类型变更
        for col_name in common:
            old_type = old_map[col_name].get("数据类型", "")
            new_type = new_map[col_name].get("数据类型", "")
            if old_type != new_type:
                changes.append({"字段": col_name, "变更类型": "类型变更",
                                "旧值": old_type, "新值": new_type})

        batch_hash = hashlib.md5(f"{table_name}:{datetime.now(timezone.utc).isoformat()}".encode()).hexdigest()[:8]

        # 写入变更记录
        diff_rows = store.rows(self.MODULE)
        for change in changes:
            new_did = max((int(r.get("id", 0)) for r in diff_rows), default=0) + 1
            diff_rows.append({
                "id": new_did,
                "变更编号": f"META-{new_did:04d}",
                "变更对象": f"{table_name}.{change['字段']}",
                "变更类型": change["变更类型"],
                "旧值": change["旧值"],
                "新值": change["新值"],
                "变更批次": batch_hash,
                "影响评估": self._assess_impact(change["变更类型"], table_name, change["字段"]),
                "变更时间": datetime.now(timezone.utc).isoformat(),
                "status": "已确认" if change["变更类型"] != "删除" else "待审核",
                "pending": change["变更类型"] == "删除",
                "abnormal": change["变更类型"] == "删除",
            })

        # 检测到字段删除：校验血缘完整性并触发更新
        if deletions:
            self._validate_lineage_impact(table_name, list(deletions))

        return {
            "表名称": table_name,
            "旧字段数": len(old_columns),
            "新字段数": len(new_columns),
            "变更总数": len(changes),
            "新增字段": list(additions),
            "删除字段": list(deletions),
            "类型变更": [c for c in changes if c["变更类型"] == "类型变更"],
            "变更批次": batch_hash,
        }

    def _assess_impact(self, change_type: str, table_name: str, field_name: str) -> str:
        """评估变更影响范围。"""
        if change_type == "删除":
            return f"高风险：可能影响下游依赖{table_name}的表，需校验血缘完整性"
        elif change_type == "类型变更":
            return f"中风险：可能影响ETL转换逻辑和数据写入"
        else:
            return "低风险：新增字段不影响已有逻辑"

    def _validate_lineage_impact(self, table_name: str, deleted_fields: list[str]) -> None:
        """校验血缘完整性：检测删除字段是否影响下游表依赖。"""
        lineage_rows = store.rows("lineage_graph")
        affected = [
            r for r in lineage_rows
            if r.get("上游表名") == table_name and r.get("status") == "已生效"
        ]
        for lr in affected:
            lr["abnormal"] = True
            lr["status"] = "待校验"

    # ------------------------------------------------------------------
    # 目录交换快照：适配结论回写待核清单 + 按次核对
    # ------------------------------------------------------------------
    def record_adaptation_review(self, table_name: str, batch_hash: str,
                                  snapshot_id: str,
                                  adaptation_details: list[dict[str, Any]]) -> int:
        """把快照导入的类型适配结论回写到元数据变更待核清单。

        每个字段一条待核记录：成功适配的标记为「待核」（等待人工/按次核对确认），
        类型转换失败的标记为「适配失败」并置为异常待处理，二者都关联目录交换快照
        编号与导入批次，供后续按次核对。
        """
        diff_rows = store.rows(self.MODULE)
        written = 0
        for detail in adaptation_details:
            new_did = max((int(r.get("id", 0)) for r in diff_rows), default=0) + 1
            resolved = bool(detail.get("是否解决"))
            diff_rows.append({
                "id": new_did,
                "变更编号": f"META-{new_did:04d}",
                "变更对象": f"{table_name}.{detail.get('字段名称', '')}",
                "变更类型": "类型适配" if resolved else "适配失败",
                "旧值": f"源类型:{detail.get('原始字段类型', '')}",
                "新值": f"目标类型:{detail.get('目标字段类型', '')}" if resolved
                else f"未解决:{detail.get('失败原因', '')}",
                "变更批次": batch_hash,
                "来源快照": snapshot_id,
                "关联表名": table_name,
                "字段名称": detail.get("字段名称", ""),
                "原始字段类型": detail.get("原始字段类型", ""),
                "目标字段类型": detail.get("目标字段类型", ""),
                "适配说明": detail.get("适配说明", ""),
                "核对结论": "待核" if resolved else "适配失败",
                "影响评估": self._assess_impact(
                    "类型变更" if resolved else "适配失败",
                    table_name, detail.get("字段名称", "")),
                "变更时间": datetime.now(timezone.utc).isoformat(),
                "status": "待核" if resolved else "待审核",
                "pending": True,
                "abnormal": not resolved,
            })
            written += 1
        return written

    def resolve_review_item(self, table_name: str, batch_hash: str,
                             field_name: str, target_type: str,
                             note: str = "") -> dict[str, Any] | None:
        """重试适配成功后，回写待核清单中对应字段的结论。"""
        for row in store.rows(self.MODULE):
            if (row.get("变更批次") == batch_hash
                    and row.get("关联表名") == table_name
                    and row.get("字段名称") == field_name
                    and row.get("核对结论") == "适配失败"):
                row["变更类型"] = "类型适配"
                row["新值"] = f"目标类型:{target_type}"
                row["目标字段类型"] = target_type
                row["适配说明"] = note or row.get("适配说明", "")
                row["核对结论"] = "待核"
                row["影响评估"] = self._assess_impact("类型变更", table_name, field_name)
                row["status"] = "待核"
                row["pending"] = True
                row["abnormal"] = False
                return row
        return None

    def reconcile_review_batch(self, batch_hash: str,
                                checks: list[dict[str, Any]]) -> dict[str, Any]:
        """按次核对完成后，把核对结论回写到同一导入批次的待核清单。"""
        diff_rows = store.rows(self.MODULE)
        index = {
            (r.get("关联表名"), r.get("字段名称")): r
            for r in diff_rows if r.get("变更批次") == batch_hash
        }
        updated = 0
        for check in checks:
            row = index.get((check.get("表名称"), check.get("字段名称")))
            if row is None:
                continue
            row["核对结论"] = "核对一致" if check.get("一致") else "核对不一致"
            if check.get("一致"):
                row["status"] = "已确认"
                row["pending"] = False
                row["abnormal"] = False
            else:
                row["status"] = "待审核"
                row["pending"] = True
                row["abnormal"] = True
            updated += 1
        return {"导入批次": batch_hash, "回写记录数": updated}

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "校验"
        snapshot["desc"] = "对比元数据schema差异并校验血缘影响"
        snapshot["artifact"] = "变更对比记录数据集"
        snapshot["artifact_desc"] = "含影响评估的变更对比记录数据集"
        return snapshot