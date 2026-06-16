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


class TestConfigCertificationsField:
    def test_config_has_certifications_field(self):
        from newsprism.config import Config
        # Config dataclass 必须有 certifications 字段
        assert "certifications" in Config.__dataclass_fields__

    def test_load_config_returns_certifications_dict(self):
        from newsprism.config import load_config
        cfg = load_config()
        # 默认路径 config/sources-certification.yaml 此时还不存在（Task 4 才创建）
        # 但字段必须存在且是 dict（空 dict 也 OK）
        assert isinstance(cfg.certifications, dict)


class TestRealCertificationConfig:
    def test_real_config_loads_without_error(self):
        path = Path("config/sources-certification.yaml")
        result = load_certifications(path)
        assert isinstance(result, dict)
        assert len(result) >= 20, f"Expected ≥20 certified sources, got {len(result)}"

    def test_real_config_all_codes_in_allowlist(self):
        path = Path("config/sources-certification.yaml")
        result = load_certifications(path)
        for source_name, cert in result.items():
            for c in cert.certifications:
                assert c.code in CERTIFICATION_CODES, (
                    f"Source '{source_name}' has illegal code '{c.code}'"
                )

    def test_real_config_keys_match_config_yaml_names(self):
        # YAML key 必须在 config.yaml 的 name 字段里
        import yaml as _yaml
        cfg_names = {
            s["name"]
            for s in _yaml.safe_load(Path("config/config.yaml").read_text(encoding="utf-8"))["sources"]
        }
        cert_keys = set(load_certifications(Path("config/sources-certification.yaml")).keys())
        orphans = cert_keys - cfg_names
        assert not orphans, f"Certification YAML has sources not in config.yaml: {orphans}"

    def test_bbc_has_strong_certification(self):
        path = Path("config/sources-certification.yaml")
        result = load_certifications(path)
        bbc = result["BBC News"]
        assert "TNI" in [c.code for c in bbc.certifications]
        assert "NG" in [c.code for c in bbc.certifications]


class TestRendererCertificationInjection:
    def test_source_entry_has_certification_fields(self):
        from newsprism.runtime.renderer import HtmlRenderer
        cert_map = {
            "BBC News": SourceCertification(
                source_name="BBC News",
                certifications=(Certification("TNI", "x", "y"),),
                detail_zh="BBC 详情",
                detail_en="BBC detail",
            ),
        }
        r = HtmlRenderer(source_certifications=cert_map)
        # _build_source_entry 需要 article_meta dict
        entry = r._build_source_entry("BBC News", {})
        assert entry["has_certification"] is True
        assert entry["cert_detail_zh"] == "BBC 详情"
        assert entry["cert_detail_en"] == "BBC detail"
        assert entry["cert_codes"] == ["TNI"]

    def test_source_entry_without_certification(self):
        from newsprism.runtime.renderer import HtmlRenderer
        r = HtmlRenderer(source_certifications={})
        entry = r._build_source_entry("IT之家", {})
        assert entry["has_certification"] is False
        assert entry["cert_detail_zh"] == ""
        assert entry["cert_codes"] == []

    def test_renderer_accepts_source_certifications_kwarg(self):
        from newsprism.runtime.renderer import HtmlRenderer
        # 确认 __init__ 接受该参数且不报错
        r = HtmlRenderer(source_certifications={"X": SourceCertification("X", (), "", "")})
        assert "X" in r.source_certifications

    def test_renderer_defaults_to_empty_certifications(self):
        from newsprism.runtime.renderer import HtmlRenderer
        r = HtmlRenderer()
        assert r.source_certifications == {}


class TestCertBadgeMacroRendering:
    """模板级渲染测试：确认 cert_badge 宏输出正确的 HTML（防回归）。"""

    def _render_badge(self, src_dict):
        from jinja2 import Environment, FileSystemLoader, select_autoescape
        env = Environment(
            loader=FileSystemLoader("templates"),
            autoescape=select_autoescape(["html"]),
        )
        template = env.get_template("report-template.html")
        return str(template.module.cert_badge(src_dict))

    def test_certified_source_emits_badge_span(self):
        src = {
            "has_certification": True,
            "cert_detail_zh": "BBC 持 TNI 认证",
            "cert_detail_en": "BBC holds TNI",
            "cert_codes": ["TNI", "NG"],
        }
        html = self._render_badge(src)
        assert 'class="cert-badge"' in html
        assert "✓" in html

    def test_uncertified_source_emits_nothing(self):
        src = {
            "has_certification": False,
            "cert_detail_zh": "",
            "cert_detail_en": "",
            "cert_codes": [],
        }
        html = self._render_badge(src)
        assert "cert-badge" not in html
        assert html.strip() == ""

    def test_title_uses_plain_text_not_html_tags(self):
        """回归测试：title 属性不能含 <span> 标签（bilingual_text 会注入标签）。"""
        src = {
            "has_certification": True,
            "cert_detail_zh": "BBC 详情",
            "cert_detail_en": "BBC detail",
            "cert_codes": ["TNI"],
        }
        html = self._render_badge(src)
        # title 属性值应是纯文本，不能含 data-lang-zh/en 的 span 标签
        assert "data-lang-zh" not in html, (
            f"title 属性含 HTML 标签（应为纯文本）: {html}"
        )
        assert "data-lang-en" not in html
        # 应同时含中英详情
        assert "BBC 详情" in html
        assert "BBC detail" in html
