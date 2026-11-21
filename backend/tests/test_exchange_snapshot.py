"""目录交换快照跨环境交接链路测试。

覆盖验收场景：
1. 导出一张含带时区时间戳（TIMESTAMP WITH TIME ZONE）的表为交换快照；
2. 在另一份目录导入两次，同一文件只留下一个有效结构（含并发导入）；
3. 带时区时间戳适配为目标库无时区 DATE，原始字段类型与适配说明留存；
4. 类型转换失败的字段进入未解决清单，可重试且未解决列保留；
5. 适配结论回写元数据变更待核清单；
6. 跑一次字段变更对比（按次核对），核对快照源类型与目录登记字段；
7. 检查实际供画像消费的结构与未解决项。
"""
from __future__ import annotations

import os
import sys
import threading

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.main import app
from app.services.exchange_snapshot import (
    SNAPSHOT_FORMAT,
    ExchangeSnapshotService,
)
from app.store import store


def make_snapshot(table_name: str, include_bad_type: bool = True) -> dict:
    """构造一份包含两类字段的交换快照：可直接对应列 + 带时区时间戳列。"""
    columns = [
        {"字段名称": "id", "源数据类型": "BIGINT", "可为空": False, "是否主键": True},
        {"字段名称": "order_no", "源数据类型": "VARCHAR(64)", "可为空": False, "是否主键": False},
        {"字段名称": "pay_time", "源数据类型": "TIMESTAMP WITH TIME ZONE",
         "可为空": False, "是否主键": False},
        {"字段名称": "amount", "源数据类型": "DECIMAL(18,2)", "可为空": True, "是否主键": False},
    ]
    if include_bad_type:
        columns.append(
            {"字段名称": "geo_tag", "源数据类型": "GEOGRAPHY(POINT,4326)",
             "可为空": True, "是否主键": False},
        )
    return {
        "格式版本": SNAPSHOT_FORMAT,
        "快照名称": f"{table_name}_交换快照",
        "源环境": "prod-mysql",
        "目标环境": "dr-hive",
        "源数据源类型": "mysql",
        "表清单": [{
            "表名称": table_name,
            "表类型": "TABLE",
            "字段列表": columns,
        }],
    }


@pytest.fixture()
def svc() -> ExchangeSnapshotService:
    return ExchangeSnapshotService()


class TestExportSnapshot:
    def test_export_contains_source_types_and_tz_timestamp(self, svc):
        snapshot = svc.export_snapshot("数据表目录样例1", source_env="prod-mysql")
        assert snapshot["格式版本"] == SNAPSHOT_FORMAT
        assert snapshot["源环境"] == "prod-mysql"
        assert "文件指纹" in snapshot
        columns = snapshot["表清单"][0]["字段列表"]
        pay_time = next(c for c in columns if c["字段名称"] == "pay_time")
        # 导出的快照必须携带源端带时区时间戳类型
        assert pay_time["源数据类型"] == "TIMESTAMP WITH TIME ZONE"
        assert "时区" in pay_time["适配说明"]
        assert "文件指纹" in snapshot

    def test_export_roundtrip_preserves_registered_source_type(self, svc):
        # 导入产生的目录字段登记带原始类型，再次导出时必须带出源类型
        svc.import_snapshot(make_snapshot("e2e_roundtrip_table",
                                          include_bad_type=False),
                            target_env="dr-hive")
        snapshot = svc.export_snapshot("e2e_roundtrip_table")
        types = {c["字段名称"]: c["源数据类型"]
                 for c in snapshot["表清单"][0]["字段列表"]}
        assert types["pay_time"] == "TIMESTAMP WITH TIME ZONE"
        assert types["id"] == "BIGINT"

    def test_export_unknown_table_raises(self, svc):
        with pytest.raises(KeyError):
            svc.export_snapshot("not_exist_table_xyz")


class TestImportSnapshot:
    def test_import_adapts_tz_timestamp_and_preserves_original_type(self, svc):
        snapshot = make_snapshot("e2e_import_adapt")
        result = svc.import_snapshot(snapshot, target_env="dr-hive")
        assert result["重复导入"] is False
        # 5 列：4 列可适配（含带时区时间戳），1 列目标库不支持
        assert result["字段总数"] == 5
        assert result["已适配字段"] == 4
        assert result["未解决字段"] == 1
        assert result["状态"] == "存在未解决项"

        detail = next(
            d for d in result["表明细"][0]["适配明细"]
            if d["字段名称"] == "pay_time")
        assert detail["原始字段类型"] == "TIMESTAMP WITH TIME ZONE"
        assert detail["目标字段类型"] == "DATE"
        assert detail["是否解决"] is True

        # 目录字段登记保留原始类型与适配说明，目标类型为无时区 DATE
        registered = [
            r for r in store.rows("catalog_column")
            if r["表名称"] == "e2e_import_adapt"]
        pay_time = next(r for r in registered if r["字段名称"] == "pay_time")
        assert pay_time["数据类型"] == "DATE"
        assert pay_time["原始字段类型"] == "TIMESTAMP WITH TIME ZONE"
        assert "丢弃时区" in pay_time["适配说明"]
        # 未解决列不得进入目录有效结构
        assert all(r["字段名称"] != "geo_tag" for r in registered)

    def test_duplicate_import_keeps_single_valid_structure(self, svc):
        snapshot = make_snapshot("e2e_dup_import", include_bad_type=False)
        first = svc.import_snapshot(snapshot, target_env="dr-hive")
        second = svc.import_snapshot(snapshot, target_env="dr-hive")
        assert first["重复导入"] is False
        assert second["重复导入"] is True
        assert first["文件指纹"] == second["文件指纹"]
        assert second["导入批次"] == first["导入批次"]

        # 目录中同一张表只有一份有效结构
        catalog_tables = [
            r for r in store.rows("table_catalog")
            if r["表名称"] == "e2e_dup_import" and r.get("导入批次")]
        assert len(catalog_tables) == 1
        snapshot_rows = [
            r for r in store.rows("exchange_snapshot")
            if r["文件指纹"] == first["文件指纹"] and r.get("有效结构")]
        assert len(snapshot_rows) == 1

    def test_concurrent_import_single_winner(self, svc):
        snapshot = make_snapshot("e2e_concurrent_import", include_bad_type=False)
        results: list[dict] = []
        errors: list[Exception] = []

        def worker():
            try:
                results.append(svc.import_snapshot(snapshot, target_env="dr-hive"))
            except Exception as exc:  # pragma: no cover - 并发不应抛错
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert len(results) == 8
        winners = [r for r in results if not r["重复导入"]]
        assert len(winners) == 1
        batches = {r["导入批次"] for r in results}
        assert len(batches) == 1
        catalog_tables = [
            r for r in store.rows("table_catalog")
            if r["表名称"] == "e2e_concurrent_import"]
        assert len(catalog_tables) == 1

    def test_failed_type_conversion_kept_as_unresolved(self, svc):
        snapshot = make_snapshot("e2e_unresolved")
        result = svc.import_snapshot(snapshot, target_env="dr-hive")
        unresolved = result["表明细"][0]["未解决列"]
        assert len(unresolved) == 1
        assert unresolved[0]["字段名称"] == "geo_tag"
        assert "GEOGRAPHY" in unresolved[0]["失败原因"]
        assert unresolved[0]["是否解决"] is False

        # 未解决项不进入画像消费结构
        profiles = [
            r for r in store.rows("column_profile")
            if r["关联表名"] == "e2e_unresolved"]
        assert len(profiles) == 4
        assert all(r["字段名称"] != "geo_tag" for r in profiles)


class TestRetryUnresolved:
    def test_retry_without_override_keeps_unresolved(self, svc):
        snapshot = make_snapshot("e2e_retry_keep")
        imported = svc.import_snapshot(snapshot, target_env="dr-hive")
        sid = next(
            r["id"] for r in store.rows("exchange_snapshot")
            if r["快照编号"] == imported["快照编号"])
        retry = svc.retry_unresolved(sid, type_overrides={})
        assert retry["本次解决"] == 0
        assert retry["仍未解决"] == 1
        assert retry["未解决列"][0]["字段名称"] == "geo_tag"

    def test_retry_with_override_registers_and_profiles(self, svc):
        snapshot = make_snapshot("e2e_retry_resolve")
        imported = svc.import_snapshot(snapshot, target_env="dr-hive")
        sid = next(
            r["id"] for r in store.rows("exchange_snapshot")
            if r["快照编号"] == imported["快照编号"])

        retry = svc.retry_unresolved(sid, {"geo_tag": "VARCHAR(255)"})
        assert retry["本次解决"] == 1
        assert retry["仍未解决"] == 0
        assert retry["状态"] == "已重试"

        registered = next(
            r for r in store.rows("catalog_column")
            if r["表名称"] == "e2e_retry_resolve" and r["字段名称"] == "geo_tag")
        assert registered["数据类型"] == "VARCHAR(255)"
        assert registered["原始字段类型"] == "GEOGRAPHY(POINT,4326)"
        assert "人工重试适配" in registered["适配说明"]

        profile = next(
            r for r in store.rows("column_profile")
            if r["关联表名"] == "e2e_retry_resolve" and r["字段名称"] == "geo_tag")
        assert profile["数据类型"] == "VARCHAR(255)"


class TestReviewChecklistAndReconcile:
    def test_adaptation_written_to_review_checklist(self, svc):
        snapshot = make_snapshot("e2e_checklist")
        svc.import_snapshot(snapshot, target_env="dr-hive")
        diff_rows = [
            r for r in store.rows("metadata_diff")
            if r.get("关联表名") == "e2e_checklist"]
        # 5 个字段各一条待核记录
        assert len(diff_rows) == 5
        pay_time = next(r for r in diff_rows if r["字段名称"] == "pay_time")
        assert pay_time["变更类型"] == "类型适配"
        assert pay_time["原始字段类型"] == "TIMESTAMP WITH TIME ZONE"
        assert pay_time["目标字段类型"] == "DATE"
        assert pay_time["核对结论"] == "待核"
        assert pay_time["来源快照"].startswith("EXCH-")

        failed = next(r for r in diff_rows if r["字段名称"] == "geo_tag")
        assert failed["变更类型"] == "适配失败"
        assert failed["abnormal"] is True
        assert failed["status"] == "待审核"

    def test_reconcile_compares_snapshot_with_catalog_per_batch(self, svc):
        snapshot = make_snapshot("e2e_reconcile")
        imported = svc.import_snapshot(snapshot, target_env="dr-hive")
        sid = next(
            r["id"] for r in store.rows("exchange_snapshot")
            if r["快照编号"] == imported["快照编号"])

        report = svc.reconcile_snapshot(sid)
        assert report["导入批次"] == imported["导入批次"]
        assert report["核对字段数"] == 5
        # 4 个已适配字段全部核对一致，1 个未解决项单独暴露
        assert report["一致字段数"] == 4
        assert report["未解决项数"] == 1
        assert report["全部一致"] is False

        pay_time = next(
            c for c in report["核对明细"] if c["字段名称"] == "pay_time")
        assert pay_time["源类型"] == "TIMESTAMP WITH TIME ZONE"
        assert pay_time["目录类型"] == "DATE"
        assert pay_time["画像类型"] == "DATE"
        assert pay_time["一致"] is True

        # 待核清单结论已按批次回写
        diff_rows = [
            r for r in store.rows("metadata_diff")
            if r.get("变更批次") == imported["导入批次"]]
        confirmed = [r for r in diff_rows if r["核对结论"] == "核对一致"]
        inconsistent = [r for r in diff_rows if r["核对结论"] == "核对不一致"]
        assert len(confirmed) == 4
        assert len(inconsistent) == 1

    def test_reconcile_all_green_after_retry(self, svc):
        snapshot = make_snapshot("e2e_reconcile_retry")
        imported = svc.import_snapshot(snapshot, target_env="dr-hive")
        sid = next(
            r["id"] for r in store.rows("exchange_snapshot")
            if r["快照编号"] == imported["快照编号"])
        svc.retry_unresolved(sid, {"geo_tag": "VARCHAR(255)"})
        report = svc.reconcile_snapshot(sid)
        assert report["全部一致"] is True
        assert report["未解决项数"] == 0
        assert report["一致字段数"] == 5
        entry = store.find("exchange_snapshot", sid)
        assert entry["status"] == "已核对"


class TestHttpEndpoints:
    def test_full_chain_over_http(self):
        client = TestClient(app)
        table_name = "数据表目录样例2"

        # 1. 导出含带时区时间戳的表快照
        resp = client.get(
            "/api/table_catalog/exchange/snapshots/export",
            params={"table_name": table_name, "source_env": "prod-mysql"},
        )
        assert resp.status_code == 200
        snapshot = resp.json()
        assert snapshot["表清单"][0]["字段列表"]

        # 2. 第一次导入
        resp = client.post(
            "/api/table_catalog/exchange/snapshots/import",
            json={"snapshot": snapshot, "target_env": "dr-hive"},
        )
        assert resp.status_code == 200
        first = resp.json()
        snapshot_id = next(
            r["id"] for r in store.rows("exchange_snapshot")
            if r["快照编号"] == first["快照编号"])

        # 3. 第二次导入同一文件 → 复用唯一有效结构
        resp = client.post(
            "/api/table_catalog/exchange/snapshots/import",
            json={"snapshot": snapshot, "target_env": "dr-hive"},
        )
        second = resp.json()
        assert second["重复导入"] is True

        # 4. 按次核对
        resp = client.get(
            f"/api/table_catalog/exchange/snapshots/{snapshot_id}/reconcile")
        assert resp.status_code == 200
        report = resp.json()
        assert report["快照编号"] == first["快照编号"]
        assert report["一致字段数"] >= 1

    def test_import_bad_payload_returns_400(self):
        client = TestClient(app)
        resp = client.post(
            "/api/table_catalog/exchange/snapshots/import",
            json={"snapshot": {"格式版本": SNAPSHOT_FORMAT, "表清单": []}},
        )
        assert resp.status_code == 400

    def test_retry_unknown_snapshot_returns_404(self):
        client = TestClient(app)
        resp = client.post(
            "/api/table_catalog/exchange/snapshots/999999/retry",
            json={"type_overrides": {}},
        )
        assert resp.status_code == 404


class TestCrossEnvironmentEndToEnd:
    """完整验收链路：导出 → 另一份目录导两次 → 变更对比 → 画像结构与未解决项检查。"""

    def test_full_handoff_acceptance(self, svc):
        table_name = "数据表目录样例1"

        # 源环境：把目录中已登记的表导出为带源类型的交换快照
        # （样例表无字段级登记，导出链路合成含带时区时间戳的结构）
        exported = svc.export_snapshot(table_name, source_env="prod-mysql",
                                       target_env="dr-hive")
        pay_time = next(
            c for c in exported["表清单"][0]["字段列表"]
            if c["字段名称"] == "pay_time")
        assert pay_time["源数据类型"] == "TIMESTAMP WITH TIME ZONE"

        # 目标目录：同一文件导入两次（模拟另一份目录的重复交接）
        first = svc.import_snapshot(exported, target_env="dr-hive",
                                    import_remark="首次跨环境交接")
        second = svc.import_snapshot(exported, target_env="dr-hive",
                                     import_remark="重复交接")
        assert first["重复导入"] is False
        assert second["重复导入"] is True
        catalog_tables = [
            r for r in store.rows("table_catalog")
            if r["表名称"] == table_name and r.get("来源快照") == first["快照编号"]]
        assert len(catalog_tables) == 1

        # 字段变更对比：按次核对源类型与目录登记
        sid = next(
            r["id"] for r in store.rows("exchange_snapshot")
            if r["快照编号"] == first["快照编号"])
        report = svc.reconcile_snapshot(sid)
        assert report["全部一致"] is True
        for check in report["核对明细"]:
            assert check["一致"] is True

        # 实际供画像消费的结构：时间戳列已变为无时区 DATE，且保留原始类型
        profiles = [
            r for r in store.rows("column_profile")
            if r["关联表名"] == table_name]
        profile_types = {r["字段名称"]: r["数据类型"] for r in profiles}
        assert profile_types["pay_time"] == "DATE"
        profile_pay = next(r for r in profiles if r["字段名称"] == "pay_time")
        assert profile_pay["原始字段类型"] == "TIMESTAMP WITH TIME ZONE"

        # 带失败类型的快照：未解决项在核对中显式保留
        bad = svc.import_snapshot(
            make_snapshot("e2e_full_handoff_bad"), target_env="dr-hive")
        bad_sid = next(
            r["id"] for r in store.rows("exchange_snapshot")
            if r["快照编号"] == bad["快照编号"])
        bad_report = svc.reconcile_snapshot(bad_sid)
        assert bad_report["未解决项数"] == 1
        unresolved_check = next(
            c for c in bad_report["核对明细"] if not c["一致"])
        assert unresolved_check["字段名称"] == "geo_tag"
        assert unresolved_check["目录类型"] == ""
        # 未解决项仍在待核清单
        review = next(
            r for r in store.rows("metadata_diff")
            if r.get("关联表名") == "e2e_full_handoff_bad"
            and r["字段名称"] == "geo_tag")
        assert review["核对结论"] == "核对不一致"
        assert review["abnormal"] is True

        # 重试后未解决项清零并进入画像
        svc.retry_unresolved(bad_sid, {"geo_tag": "VARCHAR(255)"})
        final_report = svc.reconcile_snapshot(bad_sid)
        assert final_report["全部一致"] is True
        assert any(
            r["字段名称"] == "geo_tag" and r["数据类型"] == "VARCHAR(255)"
            for r in store.rows("column_profile")
            if r["关联表名"] == "e2e_full_handoff_bad")
