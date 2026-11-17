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
) -> Tuple[pd.Series, Dict[str, float]]:
    """Detects outliers using the Interquartile Range (IQR) method.

    Args:
        series: Numeric Pandas Series.
        multiplier: IQR factor (1.5 for standard outliers, 3.0 for extreme).

    Returns:
        (is_outlier_boolean_series, stats_dict)
    """
    clean_s = series.dropna()
    if len(clean_s) < 4:
        return pd.Series(False, index=series.index), {}

    q1 = float(clean_s.quantile(0.25))
    q3 = float(clean_s.quantile(0.75))
    iqr = q3 - q1

    lower_bound = q1 - multiplier * iqr
    upper_bound = q3 + multiplier * iqr

    is_outlier = (series < lower_bound) | (series > upper_bound)

    stats = {
        "q1": q1,
        "q3": q3,
        "iqr": iqr,
        "lower_bound": lower_bound,
        "upper_bound": upper_bound,
        "outlier_count": int(is_outlier.sum()),
        "outlier_pct": float(is_outlier.mean() * 100),
    }
    return is_outlier, stats


def detect_outliers_zscore(
    series: pd.Series,
    threshold: float = 3.0,
) -> Tuple[pd.Series, pd.Series, Dict[str, float]]:
    """Detects outliers using standard Z-score.

    Args:
        series: Numeric Pandas Series.
        threshold: Standard deviation cutoff (default 3.0).

    Returns:
        (is_outlier_boolean_series, z_scores_series, stats_dict)
    """
    clean_s = series.dropna()
    if len(clean_s) < 4:
        return pd.Series(False, index=series.index), pd.Series(0.0, index=series.index), {}

    mean = float(clean_s.mean())
    std = float(clean_s.std())

    if std == 0.0 or np.isnan(std):
        z_scores = pd.Series(0.0, index=series.index)
        is_outlier = pd.Series(False, index=series.index)
    else:
        z_scores = (series - mean) / std
        is_outlier = z_scores.abs() > threshold

    stats = {
        "mean": mean,
        "std": std,
        "threshold": threshold,
        "outlier_count": int(is_outlier.sum()),
        "outlier_pct": float(is_outlier.mean() * 100),
    }
    return is_outlier, z_scores, stats
