"""Simple XBRL/XML parser for quarterly financials.

This parser extracts common facts and context metadata from company XBRL
instance documents and returns a normalized dict suitable for the
`quarterly_financials` ClickHouse table.

The implementation is intentionally conservative: it extracts a fixed set
of commonly-used tags (Revenue, Profit, EPS, Assets, Liabilities etc.) and
stores any unmapped facts into `metadata` to avoid data loss.
"""
from __future__ import annotations

import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _to_float(text: str | None) -> float | None:
    if text is None:
        return None
    s = str(text).strip()
    if s == "" or s == "-" or s.lower() == "nan":
        return None
    try:
        # remove common grouping characters
        s2 = s.replace(",", "")
        return float(s2)
    except Exception:
        return None


def parse_xbrl_file(path: Path) -> dict[str, Any]:
    """Parse an XBRL/XML file and return a normalized dict.

    Args:
        path: Path to XBRL/XML file

    Returns:
        dict with keys matching `quarterly_financials` where available and
        `metadata` containing unmapped facts.
    """
    tree = ET.parse(str(path))
    root = tree.getroot()

    # collect contexts: id -> {entity_identifier, period_start, period_end, instant}
    contexts: dict[str, dict[str, Any]] = {}
    for ctx in root.findall(".//{http://www.xbrl.org/2003/instance}context"):
        cid = ctx.get("id")
        if not cid:
            continue
        ent = ctx.find("{http://www.xbrl.org/2003/instance}entity")
        ident = None
        if ent is not None:
            idn = ent.find("{http://www.xbrl.org/2003/instance}identifier")
            if idn is not None:
                ident = idn.text
        period = ctx.find("{http://www.xbrl.org/2003/instance}period")
        pstart = pend = instant = None
        if period is not None:
            sd = period.find("{http://www.xbrl.org/2003/instance}startDate")
            ed = period.find("{http://www.xbrl.org/2003/instance}endDate")
            ins = period.find("{http://www.xbrl.org/2003/instance}instant")
            if sd is not None and sd.text:
                try:
                    pstart = datetime.fromisoformat(sd.text.strip()).date()
                except Exception:
                    pstart = None
            if ed is not None and ed.text:
                try:
                    pend = datetime.fromisoformat(ed.text.strip()).date()
                except Exception:
                    pend = None
            if ins is not None and ins.text:
                try:
                    instant = datetime.fromisoformat(ins.text.strip()).date()
                except Exception:
                    instant = None

        contexts[cid] = {
            "entity_identifier": ident,
            "period_start": pstart,
            "period_end": pend,
            "instant": instant,
        }

    # units mapping (id -> measure text)
    units: dict[str, str] = {}
    for unit in root.findall(".//{http://www.xbrl.org/2003/instance}unit"):
        uid = unit.get("id")
        if not uid:
            continue
        measure = unit.find("{http://www.xbrl.org/2003/instance}measure")
        if measure is not None and measure.text:
            units[uid] = measure.text.strip()

    # mapping heuristics: local tag -> target field name
    field_map = {
        "RevenueFromOperations": "revenue",
        "SegmentRevenueFromOperations": "revenue",
        "Income": "income",
        "OtherIncome": "other_income",
        "EmployeeBenefitExpense": "employee_benefit_expense",
        "FinanceCosts": "interest_expense",
        "SegmentFinanceCosts": "interest_expense",
        "DepreciationDepletionAndAmortisationExpense": "depreciation",
        "OtherExpenses": "other_expenses",
        "DescriptionOfOtherExpenses": "other_expenses_description",
        "Expenses": "expenses",
        "ProfitBeforeExceptionalItemsAndTax": "operating_profit",
        "ProfitBeforeTax": "profit_before_tax",
        "ProfitLossForPeriodFromContinuingOperations": "net_profit",
        "ProfitLossForPeriod": "net_profit",
        "ProfitOrLossAttributableToOwnersOfParent": "profit_or_loss_attributable_to_parent",
        "ProfitOrLossAttributableToNonControllingInterests": "profit_or_loss_attributable_to_non_controlling",
        "TaxExpense": "tax_expense",
        "CurrentTax": "current_tax",
        "DeferredTax": "deferred_tax",
        "NetMovementInRegulatoryDeferralAccountBalancesRelatedToProfitOrLossAndTheRelatedDeferredTaxMovement": "regulatory_deferral_movement",
        "PaidUpValueOfEquityShareCapital": "paid_up_value_of_equity_share_capital",
        "FaceValueOfEquityShareCapital": "face_value_of_equity_share_capital",
        "BasicEarningsLossPerShareFromContinuingOperations": "eps",
        "DilutedEarningsLossPerShareFromContinuingOperations": "diluted_eps",
        "BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations": "eps",
        "DilutedEarningsLossPerShareFromContinuingAndDiscontinuedOperations": "diluted_eps",
        "TotalAssets": "total_assets",
        "SegmentAssets": "total_assets",
        "TotalLiabilities": "total_liabilities",
        "SegmentLiabilities": "total_liabilities",
        "Equity": "equity",
        "TotalDebt": "total_debt",
        "CurrentAssets": "current_assets",
        "CurrentLiabilities": "current_liabilities",
        "CashAndCashEquivalents": "cash_and_equivalents",
        "Inventories": "inventories",
        "InterSegmentRevenue": "inter_segment_revenue",
        "SegmentProfitLossBeforeTaxAndFinanceCosts": "segment_profit_before_tax_and_finance_costs",
        "SegmentProfitBeforeTax": "segment_profit_before_tax",
        "OtherUnallocableExpenditureNetOffUnAllocableIncome": "other_unallocable_expenditure",
        "ComprehensiveIncomeForThePeriod": "comprehensive_income_for_the_period",
        "ComprehensiveIncomeForThePeriodAttributableToOwnersOfParent": "comprehensive_income_attributable_to_parent",
        "ComprehensiveIncomeForThePeriodAttributableToOwnersOfParentNonControllingInterests": "comprehensive_income_attributable_to_non_controlling",
        "SegmentRevenue": "segment_revenue",
    }

    record: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.utcnow(),
        "ingest_time": datetime.utcnow(),
        "source": "BSE",
        "schema_version": "xbrl-v1",
        "entity_id": None,
        "symbol": None,
        "company_name": None,
        "cin": None,
        "period_end_date": None,
        "period_type": None,
        "statement_type": None,
        "filing_date": None,
        # financials
        "revenue": None,
        "operating_profit": None,
        "net_profit": None,
        "depreciation": None,
        "interest_expense": None,
        "tax_expense": None,
        "total_assets": None,
        "total_liabilities": None,
        "equity": None,
        "total_debt": None,
        "current_assets": None,
        "current_liabilities": None,
        "cash_and_equivalents": None,
        "inventories": None,
        "eps": None,
        "book_value_per_share": None,
        "metadata": {},
        "_xbrl_raw_values": {},
    }

    # detect level of rounding (Crores, Lakhs, Thousands) to derive scaling
    level_rounding = None
    for el in root.iter():
        if _local_name(el.tag) == "LevelOfRoundingUsedInFinancialStatements" and el.text:
            level_rounding = el.text.strip().lower()
            break
    rounding_divisor = 1.0
    if level_rounding:
        if "crore" in level_rounding:
            rounding_divisor = 1e7
        elif "lakh" in level_rounding or "lakhs" in level_rounding:
            rounding_divisor = 1e5
        elif "thousand" in level_rounding or "thousands" in level_rounding:
            rounding_divisor = 1e3

    # iterate over facts (top-level children excluding contexts/units/schemaRef)
    for elem in list(root):
        tag_local = _local_name(elem.tag)
        # Skip structural elements
        if tag_local in ("context", "unit", "schemaRef"):
            continue
        # facts may be deeply namespaced; also allow nested
        if elem.attrib and "contextRef" in elem.attrib:
            ctxt = elem.attrib.get("contextRef")
            unit_ref = elem.attrib.get("unitRef")
            decimals = elem.attrib.get("decimals")
            value_text = elem.text.strip() if elem.text else None

            mapped = None
            # direct mapping by exact name
            if tag_local in field_map:
                mapped = field_map[tag_local]
            else:
                # fallback: match common keywords
                tl = tag_local.lower()
                if "revenue" in tl and record.get("revenue") is None:
                    mapped = "revenue"
                elif "profit" in tl and ("loss" not in tl or "net" in tl or "profitbefore" in tl):
                    # try to detect operating vs net
                    if "beforetax" in tl or "profitbefore" in tl:
                        mapped = "operating_profit"
                    elif "net" in tl or "forperiod" in tl or "profitlossforperiod" in tl:
                        mapped = "net_profit"
                elif "eps" in tl or "earnings" in tl:
                    mapped = "eps"
                elif "asset" in tl and record.get("total_assets") is None:
                    mapped = "total_assets"
                elif "liabil" in tl and record.get("total_liabilities") is None:
                    mapped = "total_liabilities"
                elif "equity" in tl and record.get("equity") is None:
                    mapped = "equity"

            # assign value (apply scaling using decimals or detected rounding)
            raw_val = _to_float(value_text)
            scaled_val = None
            try:
                # decide whether to apply monetary rounding scaling for this fact
                should_scale = True
                if unit_ref:
                    unit_text = units.get(unit_ref, str(unit_ref)).lower()
                    # don't scale per-share or pure/unitless measures
                    if (
                        "share" in unit_text
                        or "per" in unit_text
                        and "share" in unit_text
                        or "xbrli:shares" in unit_text
                        or "pure" in unit_text
                    ):
                        should_scale = False

                # if decimals attribute present and numeric and negative, prefer that
                if decimals and decimals.upper() != "INF":
                    try:
                        dec_i = int(decimals)
                        if dec_i < 0 and raw_val is not None:
                            divisor = 10 ** (-dec_i)
                            scaled_val = raw_val / divisor
                    except Exception:
                        scaled_val = None

                # fallback to rounding divisor inferred from file (Crores/Lakhs)
                if scaled_val is None and raw_val is not None:
                    if should_scale and rounding_divisor != 1.0:
                        scaled_val = raw_val / rounding_divisor
                    else:
                        scaled_val = raw_val
            except Exception:
                scaled_val = raw_val

            if mapped:
                record[mapped] = scaled_val
            else:
                # not mapped: push to metadata
                k = tag_local
                # if duplicate key, append index
                if k in record["metadata"]:
                    i = 1
                    while f"{k}_{i}" in record["metadata"]:
                        i += 1
                    k = f"{k}_{i}"
                record["metadata"][k] = value_text

            # always store raw original numeric in a raw map for auditing
            if raw_val is not None:
                record["_xbrl_raw_values"][tag_local] = raw_val

            # attach entity/context info if available
            if ctxt and ctxt in contexts:
                ctx = contexts[ctxt]
                # prefer period_end if present
                if ctx.get("period_end") and record.get("period_end_date") is None:
                    record["period_end_date"] = ctx.get("period_end")
                if ctx.get("entity_identifier") and record.get("entity_id") is None:
                    record["entity_id"] = ctx.get("entity_identifier")

    # fill symbol from entity_id when possible (NSESymbol scheme sometimes present)
    if record.get("entity_id"):
        # entity id may include scheme; try extracting last segment
        ent = record["entity_id"]
        if isinstance(ent, str) and "/" in ent:
            record["symbol"] = ent.rsplit("/", 1)[-1]
        else:
            record["symbol"] = ent

    # ensure period_end_date is a date
    if isinstance(record.get("period_end_date"), datetime):
        record["period_end_date"] = record["period_end_date"].date()

    return record
