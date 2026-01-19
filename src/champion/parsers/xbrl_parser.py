"""Simple XBRL/XML parser for quarterly financials.

This parser extracts all facts from company XBRL instance documents and 
returns a normalized dict with snake_case column names matching the 
`quarterly_financials` ClickHouse table schema.
"""

from __future__ import annotations

import re
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _camel_to_snake(name: str) -> str:
    """Convert CamelCase to snake_case."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.lower()


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

    # mapping heuristics: local tag -> target field name (matching ClickHouse schema)
    field_map = {
        "RevenueFromOperations": "revenue_from_operations",
        "SegmentRevenueFromOperations": "segment_revenue_from_operations",
        "Income": "income",
        "OtherIncome": "other_income",
        "EmployeeBenefitExpense": "employee_benefit_expense",
        "FinanceCosts": "finance_costs",
        "SegmentFinanceCosts": "segment_finance_costs",
        "DepreciationDepletionAndAmortisationExpense": "depreciation_depletion_and_amortisation_expense",
        "OtherExpenses": "other_expenses",
        "DescriptionOfOtherExpenses": "description_of_other_expenses",
        "Expenses": "expenses",
        "ProfitBeforeExceptionalItemsAndTax": "profit_before_exceptional_items_and_tax",
        "ProfitBeforeTax": "profit_before_tax",
        "ProfitLossForPeriodFromContinuingOperations": "profit_loss_for_period_from_continuing_operations",
        "ProfitLossForPeriod": "profit_loss_for_period",
        "ProfitOrLossAttributableToOwnersOfParent": "profit_or_loss_attributable_to_owners_of_parent",
        "ProfitOrLossAttributableToNonControllingInterests": "profit_or_loss_attributable_to_non_controlling_interests",
        "TaxExpense": "tax_expense",
        "CurrentTax": "current_tax",
        "DeferredTax": "deferred_tax",
        "NetMovementInRegulatoryDeferralAccountBalancesRelatedToProfitOrLossAndTheRelatedDeferredTaxMovement": "net_movement_in_regulatory_deferral_account_balances_related_to_profit_or_loss_and_the_related_deferred_tax_movement",
        "PaidUpValueOfEquityShareCapital": "paid_up_value_of_equity_share_capital",
        "FaceValueOfEquityShareCapital": "face_value_of_equity_share_capital",
        "BasicEarningsLossPerShareFromContinuingOperations": "basic_earnings_loss_per_share_from_continuing_operations",
        "DilutedEarningsLossPerShareFromContinuingOperations": "diluted_earnings_loss_per_share_from_continuing_operations",
        "BasicEarningsLossPerShareFromContinuingAndDiscontinuedOperations": "basic_earnings_loss_per_share_from_continuing_and_discontinued_operations",
        "DilutedEarningsLossPerShareFromContinuingAndDiscontinuedOperations": "diluted_earnings_loss_per_share_from_continuing_and_discontinued_operations",
        "TotalAssets": "total_assets",
        "SegmentAssets": "segment_assets",
        "TotalLiabilities": "total_liabilities",
        "SegmentLiabilities": "segment_liabilities",
        "Equity": "equity",
        "TotalDebt": "total_debt",
        "CurrentAssets": "current_assets",
        "CurrentLiabilities": "current_liabilities",
        "CashAndCashEquivalents": "cash_and_cash_equivalents",
        "Inventories": "inventories",
        "InterSegmentRevenue": "inter_segment_revenue",
        "SegmentProfitLossBeforeTaxAndFinanceCosts": "segment_profit_loss_before_tax_and_finance_costs",
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

            # Convert CamelCase tag to snake_case column name
            col_name = _camel_to_snake(tag_local)

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

            # Store value directly with snake_case column name
            # Only store if not already set (first value wins for duplicates)
            if col_name not in record or record.get(col_name) is None:
                record[col_name] = scaled_val

            # attach entity/context info if available
            if ctxt and ctxt in contexts:
                ctx = contexts[ctxt]
                # prefer period_end if present
                if ctx.get("period_end") and not record.get("period_end_date"):
                    record["period_end_date"] = ctx.get("period_end")
                if ctx.get("entity_identifier") and not record.get("entity_id"):
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
