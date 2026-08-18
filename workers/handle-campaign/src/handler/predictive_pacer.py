# -*- coding: utf-8 -*-
"""
Predictive dial pacing (H5).

Pure helpers for gamma and C_dial so unit tests do not need Redis/Gearman.
Cableado en AverageWorker._allowed_parallel_predictive.
"""

from __future__ import annotations

from math import floor
from typing import Any, Dict, Optional


MODE_WARMUP = 'warmup'
MODE_PREDICTIVE = 'predictive'
MODE_THROTTLED = 'throttled'  # latched kill-switch → progressive R=1
MODE_PROGRESSIVE_FALLBACK = 'progressive_fallback'


def compute_gamma(
        drop_rate: Optional[float],
        d_max: float,
        aggressiveness: float = 1.0,
        gamma_floor: float = 0.2,
) -> float:
    """
    Factor γ ∈ [0, aggressiveness] según margen a D_max.

    - D is None → tratar como 0 (sin muestra de drop)
    - D <= 0.5 * D_max → aggressiveness
    - 0.5 * D_max < D < D_max → interpolación lineal → gamma_floor
    - D >= D_max → 0 (decide_predictive_pace aplica soft floor o force_throttle)
    """
    try:
        d_max_f = float(d_max)
    except (TypeError, ValueError):
        d_max_f = 0.0
    if d_max_f <= 0:
        return 0.0

    try:
        agg = float(aggressiveness if aggressiveness is not None else 1.0)
    except (TypeError, ValueError):
        agg = 1.0
    agg = max(0.1, min(agg, 5.0))

    try:
        floor_g = float(gamma_floor)
    except (TypeError, ValueError):
        floor_g = 0.2
    floor_g = max(0.0, min(floor_g, agg))

    if drop_rate is None:
        d = 0.0
    else:
        try:
            d = max(0.0, float(drop_rate))
        except (TypeError, ValueError):
            d = 0.0

    if d >= d_max_f:
        return 0.0

    half = 0.5 * d_max_f
    if d <= half:
        return agg

    # half < d < d_max → map [half, d_max] → [agg, floor_g]
    span = d_max_f - half
    if span <= 0:
        return floor_g
    t = (d - half) / span  # (0, 1)
    return agg + t * (floor_g - agg)


def compute_c_dial(
        a_free: float,
        a_expected: float,
        c_ringing: float,
        p_hit: float,
        gamma: float,
        p_hit_floor: float = 0.05,
) -> int:
    """
    C_dial = max(0, floor(((A_free + A_expected - C_ringing * P_hit) / P_hit) * gamma))
    """
    try:
        p = float(p_hit)
    except (TypeError, ValueError):
        return 0
    try:
        floor_p = float(p_hit_floor)
    except (TypeError, ValueError):
        floor_p = 0.05
    p = max(p, max(0.0, floor_p))
    if p <= 0:
        return 0

    try:
        g = float(gamma)
    except (TypeError, ValueError):
        g = 0.0
    if g <= 0:
        return 0

    try:
        a_free_f = max(0.0, float(a_free or 0.0))
    except (TypeError, ValueError):
        a_free_f = 0.0
    try:
        a_exp_f = max(0.0, float(a_expected or 0.0))
    except (TypeError, ValueError):
        a_exp_f = 0.0
    try:
        ringing_f = max(0.0, float(c_ringing or 0.0))
    except (TypeError, ValueError):
        ringing_f = 0.0

    raw = ((a_free_f + a_exp_f - ringing_f * p) / p) * g
    return max(0, int(floor(raw)))


def decide_predictive_pace(
        *,
        a_free: float,
        a_expected: float,
        c_ringing: float,
        p_hit: Optional[float],
        drop_rate: Optional[float],
        d_max: float,
        aggressiveness: float = 1.0,
        warmup: bool = False,
        p_hit_floor: float = 0.05,
        gamma_floor: float = 0.2,
        force_throttle: bool = False,
) -> Dict[str, Any]:
    """
    Decide pacing action for a predictive campaign tick.

    Returns dict:
      mode: warmup | predictive | throttled | progressive_fallback
      gamma, c_dial, reason
      use_progressive_r1: bool — caller should dial with progressive boost=1.0

    force_throttle: hard kill-switch (streak ≥ K / latch) → progressive R=1.
    Si D ≥ D_max y no force_throttle: soft hold con γ = gamma_floor.
    """
    if warmup or p_hit is None:
        return {
            'mode': MODE_WARMUP if warmup else MODE_PROGRESSIVE_FALLBACK,
            'gamma': 0.0,
            'c_dial': 0,
            'use_progressive_r1': True,
            'reason': 'warmup' if warmup else 'p_hit_unavailable',
        }

    if force_throttle:
        return {
            'mode': MODE_THROTTLED,
            'gamma': 0.0,
            'c_dial': 0,
            'use_progressive_r1': True,
            'reason': 'throttle_streak',
        }

    gamma = compute_gamma(
        drop_rate, d_max, aggressiveness=aggressiveness, gamma_floor=gamma_floor,
    )
    reason = 'ok'
    if gamma <= 0:
        # D ≥ D_max but streak not yet latched → soft overdial floor
        try:
            floor_g = float(gamma_floor)
        except (TypeError, ValueError):
            floor_g = 0.2
        try:
            agg = float(aggressiveness if aggressiveness is not None else 1.0)
        except (TypeError, ValueError):
            agg = 1.0
        agg = max(0.1, min(agg, 5.0))
        gamma = max(0.0, min(floor_g, agg))
        reason = 'drop_over_dmax_soft'

    c_dial = compute_c_dial(
        a_free, a_expected, c_ringing, p_hit, gamma, p_hit_floor=p_hit_floor,
    )
    return {
        'mode': MODE_PREDICTIVE,
        'gamma': gamma,
        'c_dial': c_dial,
        'use_progressive_r1': False,
        'reason': reason,
    }


def apply_channel_caps(c_dial: int, num_available_channels: int) -> int:
    """Cap C_dial by free channel headroom (max_channels - in-flight)."""
    try:
        available = int(num_available_channels)
    except (TypeError, ValueError):
        available = 0
    try:
        dial = int(c_dial)
    except (TypeError, ValueError):
        dial = 0
    return max(0, min(dial, max(0, available)))
