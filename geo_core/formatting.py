"""Formatting helpers shared across CLI and package layers."""

from __future__ import annotations


def condense_year_ranges(years: list[int]) -> str:
    """
    Condense contiguous year ranges into readable format.

    Args:
        years: Sorted list of years.

    Returns:
        String representation with ranges (for example: "1990-2000, 2005, 2010-2015").
    """
    if not years:
        return ""

    ranges = []
    start = years[0]
    end = years[0]

    for index in range(1, len(years)):
        if years[index] == end + 1:
            end = years[index]
        else:
            if start == end:
                ranges.append(str(start))
            else:
                ranges.append(f"{start}-{end}")
            start = years[index]
            end = years[index]

    if start == end:
        ranges.append(str(start))
    else:
        ranges.append(f"{start}-{end}")

    return ", ".join(ranges)
