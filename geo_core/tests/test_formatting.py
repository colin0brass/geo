from geo_core.formatting import condense_year_ranges


def test_condense_year_ranges_single_year():
    assert condense_year_ranges([2025]) == "2025"


def test_condense_year_ranges_contiguous():
    assert condense_year_ranges([1990, 1991, 1992, 1993, 1994, 1995]) == "1990-1995"
    assert condense_year_ranges([2020, 2021, 2022, 2023, 2024, 2025]) == "2020-2025"


def test_condense_year_ranges_with_gaps():
    assert condense_year_ranges([1990, 1991, 1995, 2000, 2001, 2002]) == "1990-1991, 1995, 2000-2002"
    assert condense_year_ranges([2020, 2022, 2024]) == "2020, 2022, 2024"


def test_condense_year_ranges_empty():
    assert condense_year_ranges([]) == ""


def test_condense_year_ranges_two_years():
    assert condense_year_ranges([2024, 2025]) == "2024-2025"
