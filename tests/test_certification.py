"""认证徽标功能测试：数据结构、YAML 加载、allowlist 校验、renderer 注入。"""
from __future__ import annotations

from pathlib import Path

import pytest

from newsprism.types import CERTIFICATION_CODES, Certification, SourceCertification


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
