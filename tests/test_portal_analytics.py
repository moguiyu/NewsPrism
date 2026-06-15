from newsprism.runtime.portal import analytics as A


def _row(**kw):
    base = dict(id=1, cluster_id=1, report_date="2026-06-14", dims={}, composite=0.5,
                selected=1, display_category="国际时政", status="publishable",
                flags=[], subject_regions=[], verdict=None, cluster_summary="s")
    base.update(kw)
    return base


def test_filter_by_category_and_selection():
    rows = [_row(display_category="国际时政", selected=1),
            _row(display_category="体育运动", selected=0)]
    out = A.filter_rows(rows, categories=["国际时政"], selection="selected")
    assert len(out) == 1 and out[0]["display_category"] == "国际时政"


def test_filter_composite_range_and_subject():
    rows = [_row(composite=0.2, subject_regions=["us"]),
            _row(composite=0.9, subject_regions=["il"])]
    out = A.filter_rows(rows, composite_min=0.5, subject_regions=["il"])
    assert len(out) == 1 and out[0]["composite"] == 0.9


def test_matrix_category_dimension_averages():
    rows = [_row(display_category="国际时政", dims={"scope": 8, "severity": 6}),
            _row(display_category="国际时政", dims={"scope": 4, "severity": 2})]
    m = A.matrix_category_dimension(rows)
    assert m["国际时政"]["scope"] == 6.0
    assert m["国际时政"]["severity"] == 4.0


def test_matrix_subject_category_counts_selected_only():
    rows = [_row(selected=1, subject_regions=["us"], display_category="科技创新"),
            _row(selected=1, subject_regions=["us", "cn"], display_category="科技创新"),
            _row(selected=0, subject_regions=["us"], display_category="科技创新")]
    m = A.matrix_subject_category(rows)
    assert m["us"]["科技创新"] == 2  # candidate excluded
    assert m["cn"]["科技创新"] == 1


def test_matrix_source_subject_counts():
    rows = [_row(cluster_id=10, selected=1, subject_regions=["il"]),
            _row(cluster_id=11, selected=1, subject_regions=["cn"])]
    source_rows = [{"cluster_id": 10, "origin_region": "us", "source_name": "AP"},
                   {"cluster_id": 11, "origin_region": "us", "source_name": "AP"},
                   {"cluster_id": 11, "origin_region": "cn", "source_name": "Xinhua"}]
    m = A.matrix_source_subject(rows, source_rows)
    assert m["us"]["il"] == 1
    assert m["us"]["cn"] == 1
    assert m["cn"]["cn"] == 1
