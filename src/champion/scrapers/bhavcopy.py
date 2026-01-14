"""Compat shim: expose `BhavcopyScraper` at `champion.scrapers.bhavcopy`.

Some CLI paths import `champion.scrapers.bhavcopy` while implementation
lives under `champion.scrapers.nse`. Provide a tiny re-export to keep
backfill and other tools working.
"""
from champion.scrapers.nse.bhavcopy import BhavcopyScraper

__all__ = ["BhavcopyScraper"]
