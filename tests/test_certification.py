"""认证徽标功能测试：数据结构、YAML 加载、allowlist 校验、renderer 注入。"""
from __future__ import annotations

from pathlib import Path

import pytest

from newsprism.types import CERTIFICATION_CODES, Certification, SourceCertification
from newsprism.config import load_certifications


class TestCertificationTypes:
    def test_certification_codes_is_complete_allowlist(self):
        # 白名单必须包含这 5 个代号
        for code in ("TNI", "NG", "AF", "MBFC", "JTI"):
            assert code in CERTIFICATION_CODES, f"Missing code: {code}"

    def test_certification_codes_have_bilingual_labels(self):
        for code, (zh, en) in CERTIFICATION_CODES.items():
            assert isinstance(zh, str) and zh, f"{code} missing zh label"
            assert isinstance(en, str) and en, f"{code} missing en label"

    def test_source_certification_is_frozen(self):
        cert = SourceCertification(
            source_name="BBC News",
            certifications=(),
            detail_zh="x",
            detail_en="y",
        )
        with pytest.raises(Exception):
            cert.source_name = "X"  # frozen dataclass 不可变

    def test_certification_is_frozen(self):
        c = Certification(code="TNI", label_zh="x", label_en="y")
        with pytest.raises(Exception):
            c.code = "NG"


class TestLoadCertifications:
    def test_loads_known_sources(self, tmp_path):
        yaml_text = """
"BBC News":
  certifications: [TNI, NG]
  detail: "test"
  detail_en: "test en"
"""
        path = tmp_path / "sources-certification.yaml"
        path.write_text(yaml_text, encoding="utf-8")
        result = load_certifications(path)
        assert "BBC News" in result
        assert tuple(c.code for c in result["BBC News"].certifications) == ("TNI", "NG")
        assert result["BBC News"].detail_zh == "test"
        assert result["BBC News"].detail_en == "test en"

    def test_missing_file_returns_empty(self, tmp_path):
        # 文件不存在不应 crash，返回空 dict（功能可选）
        assert load_certifications(tmp_path / "nonexistent.yaml") == {}

    def test_unknown_code_raises(self, tmp_path):
        yaml_text = '"X":\n  certifications: [FAKE_CODE]\n  detail: "x"\n'
        path = tmp_path / "bad.yaml"
        path.write_text(yaml_text, encoding="utf-8")
        with pytest.raises(ValueError, match="Unknown certification"):
            load_certifications(path)

    def test_empty_certifications_ok(self, tmp_path):
        # 空认证列表（理论上不该写，但不应 crash）
        yaml_text = '"X":\n  certifications: []\n  detail: ""\n'
        path = tmp_path / "empty.yaml"
        path.write_text(yaml_text, encoding="utf-8")
        assert load_certifications(path)["X"].certifications == ()

    def test_empty_file_returns_empty_dict(self, tmp_path):
        path = tmp_path / "empty_file.yaml"
        path.write_text("", encoding="utf-8")
        assert load_certifications(path) == {}

    def test_certification_label_resolved_from_allowlist(self, tmp_path):
        # label_zh/label_en 应从 CERTIFICATION_CODES 解析，不读 YAML 里的
        yaml_text = '"X":\n  certifications: [TNI]\n  detail: "x"\n'
        path = tmp_path / "labels.yaml"
        path.write_text(yaml_text, encoding="utf-8")
        result = load_certifications(path)
        cert = result["X"].certifications[0]
        assert cert.label_zh == CERTIFICATION_CODES["TNI"][0]
        assert cert.label_en == CERTIFICATION_CODES["TNI"][1]
