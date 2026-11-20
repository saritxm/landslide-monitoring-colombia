"""Outlier Detection and Handling Module.

Implements Interquartile Range (IQR) and Z-score methods to identify, flag,
and handle numerical anomalies (e.g. extreme precipitation spikes, casualty counts).
"""

from typing import Dict, Tuple
import numpy as np
import pandas as pd


def detect_outliers_iqr(
    series: pd.Series,
    multiplier: float = 1.5,
    exclude_zeros: bool = False,
) -> Tuple[pd.Series, Dict[str, float]]:
    """Detects outliers using the Interquartile Range (IQR) method.

    Args:
        series: Numeric Pandas Series.
        multiplier: IQR factor (1.5 for standard outliers, 3.0 for extreme).
        exclude_zeros: If True, calculates Q1/Q3 and bounds using only positive values (> 0),
                       avoiding zero-inflation collapse in precipitation or casualty data.

    Returns:
        (is_outlier_boolean_series, stats_dict)
    """
    clean_s = series.dropna()
    if exclude_zeros:
        clean_s = clean_s[clean_s > 0]

    if len(clean_s) < 4:
        return (
            pd.Series(False, index=series.index),
            {
                "q1": 0.0,
                "q3": 0.0,
                "iqr": 0.0,
                "lower_bound": 0.0,
                "upper_bound": 0.0,
                "sample_size": len(clean_s),
                "outlier_count": 0,
                "outlier_pct": 0.0,
            },
        )

    q1 = float(clean_s.quantile(0.25))
    q3 = float(clean_s.quantile(0.75))
    iqr = q3 - q1

    lower_bound = q1 - multiplier * iqr
    upper_bound = q3 + multiplier * iqr

    if exclude_zeros:
        # Zero and negative values are not considered extreme event outliers
        is_outlier = (series > 0) & (series > upper_bound)
    else:
        is_outlier = (series < lower_bound) | (series > upper_bound)

    stats = {
        "q1": q1,
        "q3": q3,
        "iqr": iqr,
        "lower_bound": lower_bound,
        "upper_bound": upper_bound,
        "sample_size": int(len(clean_s)),
        "outlier_count": int(is_outlier.sum()),
        "outlier_pct": float(is_outlier.mean() * 100),
    }
    return is_outlier.fillna(False), stats


def detect_outliers_zscore(
    series: pd.Series,
    threshold: float = 3.0,
    exclude_zeros: bool = False,
) -> Tuple[pd.Series, pd.Series, Dict[str, float]]:
    """Detects outliers using standard Z-score.

    Args:
        series: Numeric Pandas Series.
        threshold: Standard deviation cutoff (default 3.0).
        exclude_zeros: If True, calculates mean and std using only positive values (> 0),
                       assigning z-score 0.0 to zero/dry observations.

    Returns:
        (is_outlier_boolean_series, z_scores_series, stats_dict)
    """
    clean_s = series.dropna()
    if exclude_zeros:
        clean_s = clean_s[clean_s > 0]

    if len(clean_s) < 4:
        return (
            pd.Series(False, index=series.index),
            pd.Series(0.0, index=series.index),
            {
                "mean": 0.0,
                "std": 0.0,
                "threshold": threshold,
                "sample_size": len(clean_s),
                "outlier_count": 0,
                "outlier_pct": 0.0,
            },
        )

    mean = float(clean_s.mean())
    std = float(clean_s.std())

    if std == 0.0 or np.isnan(std):
        z_scores = pd.Series(0.0, index=series.index)
        is_outlier = pd.Series(False, index=series.index)
    else:
        z_scores = pd.Series(0.0, index=series.index, dtype="float64")
        if exclude_zeros:
            wet_mask = series > 0
            z_scores.loc[wet_mask] = (series.loc[wet_mask] - mean) / std
            is_outlier = wet_mask & (z_scores > threshold)
        else:
            z_scores = (series - mean) / std
            is_outlier = z_scores.abs() > threshold

    stats = {
        "mean": mean,
        "std": std,
        "threshold": threshold,
        "sample_size": int(len(clean_s)),
        "outlier_count": int(is_outlier.sum()),
        "outlier_pct": float(is_outlier.mean() * 100),
    }
    return is_outlier.fillna(False), z_scores, stats
