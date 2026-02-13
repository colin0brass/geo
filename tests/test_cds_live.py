"""Live CDS integration tests.

These tests are included in normal pytest runs and skip automatically when
CDS credentials are not configured (typically ~/.cdsapirc).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from cds import CDS, Location


HAS_CDS_CONFIG = (Path.home() / ".cdsapirc").exists()


@pytest.mark.slow
@pytest.mark.integration
@pytest.mark.skipif(not HAS_CDS_CONFIG, reason="Missing CDS credentials file ~/.cdsapirc")
def test_live_cds_month_data_minimal_request(tmp_path):
    """Fetch a minimal real CDS sample (single day/hour) and validate structure."""
    cds = CDS(cache_dir=tmp_path)
    loc = Location(name="Cambridge, UK", lat=52.21, lon=0.12, tz="Europe/London")

    ds = cds.get_month_data(
        location=loc,
        year=2024,
        month=1,
        day=15,
        hour=12,
        half_box_deg=0.1,
    )

    assert "t2m" in ds.data_vars
    assert ds["t2m"].size > 0
