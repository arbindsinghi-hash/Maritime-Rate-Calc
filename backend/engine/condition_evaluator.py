"""
Data-driven condition evaluator for tariff exemptions, reductions, and surcharges.

Replaces the 82-line if/elif chain with a registry of condition matchers.
Each condition is mapped to a callable that takes (req) and returns bool.
"""

from __future__ import annotations

from typing import Callable, Dict, List

from backend.models.schemas import CalculationRequest


def _make_vessel_type_check(*types: str) -> Callable[[CalculationRequest], bool]:
    """Create a condition checker for vessel type membership."""
    type_set = set(types)
    return lambda req: req.technical_specs.vessel_type.value in type_set


def _make_cert_check(*certs: str) -> Callable[[CalculationRequest], bool]:
    """Create a condition checker for certification presence."""
    cert_set = set(certs)
    return lambda req: bool(cert_set.intersection(c.lower() for c in req.operational_data.certifications))


# Registry of condition string → checker function
_CONDITION_REGISTRY: Dict[str, Callable[[CalculationRequest], bool]] = {
    # Vessel type conditions
    "bonafide_coaster": lambda req: (
        req.operational_data.is_coaster or req.technical_specs.vessel_type.value == "coaster"
    ),
    "passenger_vessel": _make_vessel_type_check("passenger_vessel"),
    "small_vessel_4_2_visiting": _make_vessel_type_check("small_vessel", "pleasure_vessel"),
    "section_4_2_small_pleasure": _make_vessel_type_check("small_vessel", "pleasure_vessel"),
    "non_self_propelled_small_pleasure_not_gain": _make_vessel_type_check("small_vessel", "pleasure_vessel"),

    # Purpose conditions
    "sole_purpose_bunkers_stores_water": lambda req: req.operational_data.purpose.value == "bunkers_stores_water",
    "entire_stay_max_48_hours": lambda req: req.operational_data.stay_hours <= 48,
    "not_cargo_working_first_30_days": lambda req: (
        not req.operational_data.is_cargo_working and req.operational_data.days_alongside <= 30
    ),
    "not_cargo_working_first_30_days_only": lambda req: (
        not req.operational_data.is_cargo_working and req.operational_data.days_alongside <= 30
    ),
    "in_port_longer_than_30_days_not_cargo_working": lambda req: (
        not req.operational_data.is_cargo_working and req.operational_data.days_alongside > 30
    ),

    # Stay duration
    "stay_less_than_12_hours": lambda req: req.operational_data.stay_hours < 12,

    # Certifications / Green
    "double_hull_or_segregated_ballast_or_green_award": _make_cert_check(
        "double_hull", "segregated_ballast", "green_award",
    ),
    "double_hull": _make_cert_check("double_hull"),
    "segregated_ballast": _make_cert_check("segregated_ballast"),
    "green_award": _make_cert_check("green_award"),

    # Government/military exemptions
    "saps_sandf": _make_cert_check("saps_sandf"),
    "saps_sandf_except_on_request": _make_cert_check("saps_sandf"),
    "samsa": _make_cert_check("samsa"),
    "sa_medical_research": _make_cert_check("sa_medical_research"),

    # Not cargo working
    "not_cargo_working": lambda req: not req.operational_data.is_cargo_working,
    "vessel_occupying_berth_not_handling_cargo": lambda req: not req.operational_data.is_cargo_working,
}

# Conditions that always evaluate to False (require runtime context not in the request)
_ALWAYS_FALSE_CONDITIONS = {
    "outside_working_hours",
    "vessel_not_ready_30_min_after_notified_time",
    "cancellation_within_30_min_pilot_not_boarded",
    "durban_cancellation_within_60_min_pilot_not_boarded",
    "standby_cancelled_after_commenced",
    "vessel_arrives_30_min_after_notified_time",
    "additional_tug_requested_or_safety",
    "vessel_without_own_power",
    "vessel_without_power_plus_additional_tug",
    "anchorage_outside_port",
    "return_from_anchorage_harbour_master_order",
    "second_call_return_from_anchorage_port_order",
    "time_in_drydock_floating_dock_syncrolift_slipway",
    "small_pleasure_4_2_registered_port",
    "fishing_licensed_deat_saldanha",
    "small_vessel_or_hulk_or_pleasure_vessel",
    "visiting_vessel_not_engaged_in_trade",
    "not_at_commercial_berth",
    "coastwise_movement",
    "transhipment_movement",
    "late_or_non_submission_cargo_dues_order",
    "bunkers_water_for_vessel_own_consumption_at_commercial_berth",
    "cargo_landed_in_error_reshipped_same_vessel",
    "fish_local_consumption_leased_berth_licensed_fishing_vessel",
}


def condition_matches(condition: str, req: CalculationRequest) -> bool:
    """Evaluate a single condition string against request data."""
    cond = condition.lower().strip()

    if cond in _ALWAYS_FALSE_CONDITIONS:
        return False

    checker = _CONDITION_REGISTRY.get(cond)
    if checker is not None:
        return checker(req)

    return False


def all_conditions_match(conditions: List[str], req: CalculationRequest) -> bool:
    """All conditions in the list must match."""
    return all(condition_matches(c, req) for c in conditions)
