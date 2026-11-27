"""Fit and apply an explicit calibration between two archive RV pipelines."""

import hashlib
import itertools
import json
import math
import statistics
from dataclasses import dataclass


@dataclass(frozen=True)
class Calibration:
    pipeline_a: str
    pipeline_b: str
    frame_a: str
    frame_b: str
    offset_b_minus_a_km_s: float
    offset_error_km_s: float
    scatter_km_s: float

    def __post_init__(self):
        first = self.pipeline_a.split(":")
        second = self.pipeline_b.split(":")
        if (len(first) != 4 or len(second) != 4 or not all(first + second)
                or first[0] == second[0] or self.pipeline_a >= self.pipeline_b):
            raise ValueError("Calibration needs two different surveys, sorted by pipeline ID")
        if not isinstance(self.frame_a, str) or not isinstance(self.frame_b, str) or not all(
            frame in {"survey_native", "barycentric", "heliocentric"}
            for frame in (self.frame_a, self.frame_b)
        ):
            raise ValueError("Calibration needs explicit accepted RV frame labels")
        if (not all(math.isfinite(value) for value in
                    (self.offset_b_minus_a_km_s, self.offset_error_km_s, self.scatter_km_s))
                or self.offset_error_km_s <= 0 or self.scatter_km_s < 0):
            raise ValueError("Calibration offset and uncertainties must be finite and valid")

    def residual(self, a, b):
        signed = b["rv_km_s"] - a["rv_km_s"] - self.offset_b_minus_a_km_s
        uncertainty = math.hypot(a["effective_error_km_s"], b["effective_error_km_s"],
                                 self.offset_error_km_s, self.scatter_km_s)
        return signed, uncertainty


def calibration_id(record):
    return hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()[:16]


def load_calibrations(path):
    if path is None:
        return {}
    artifact = json.loads(path.read_text())
    if artifact.get("version") != 1 or not artifact.get("pairs"):
        raise ValueError("Expected a version 1 calibration artifact with at least one pair")
    result = {}
    for record in artifact["pairs"]:
        payload = {key: value for key, value in record.items() if key != "id"}
        if record.get("id") != calibration_id(payload):
            raise ValueError("Calibration artifact has changed since it was generated")
        calibration = Calibration(**record["calibration"])
        min_snr = record["validation"]["min_snr"]
        if not math.isfinite(min_snr) or min_snr < 0:
            raise ValueError("Calibration needs a valid minimum S/N")
        key = calibration.pipeline_a, calibration.pipeline_b
        if key in result:
            raise ValueError(f"Duplicate calibration for {key}")
        result[key] = calibration, record["id"], min_snr
    return result


def best_cross_pair(first, second, calibration, min_delta, min_sigma, min_baseline):
    best = None
    for a, b in itertools.product(first, second):
        if a["rv_frame"] != calibration.frame_a or b["rv_frame"] != calibration.frame_b:
            continue
        baseline = abs(b["mjd"] - a["mjd"])
        if baseline < min_baseline:
            continue
        residual, uncertainty = calibration.residual(a, b)
        delta = abs(residual)
        sigma = delta / uncertainty
        rank = sigma, delta, baseline
        if delta >= min_delta and sigma >= min_sigma and (best is None or rank > best[0]):
            best = rank, a, b, residual, uncertainty
    return best


def median_absolute_deviation(values, center):
    return statistics.median(abs(value - center) for value in values)


def fit_calibration(groups, pipelines, frames, min_training, min_holdout, max_span, control_ids):
    pipeline_a, pipeline_b = pipelines
    frame_a, frame_b = frames
    # Validate identifiers and frames before scanning the full input.
    Calibration(pipeline_a, pipeline_b, frame_a, frame_b, 0.0, 1.0, 0.0)
    controls = []
    matched = 0
    for identity, visits in groups:
        if not identity.startswith("gaia-dr3:") or not all(pipeline in visits for pipeline in pipelines):
            continue
        matched += 1
        if control_ids is not None and identity.split(":", 1)[1] not in control_ids:
            continue
        a, b = ([visit for visit in visits[pipeline] if visit["rv_frame"] == frame]
                for pipeline, frame in zip(pipelines, frames, strict=True))
        if min(len(a), len(b)) < 2:
            continue
        values_a, values_b = ([visit["rv_km_s"] for visit in group] for group in (a, b))
        if max(max(values_a) - min(values_a), max(values_b) - min(values_b)) > max_span:
            continue
        difference = statistics.median(values_b) - statistics.median(values_a)
        pair_error = math.hypot(*(statistics.median(v["effective_error_km_s"] for v in group)
                                  for group in (a, b)))
        controls.append((identity, difference, pair_error))

    if len(controls) < min_training + min_holdout:
        raise ValueError(f"Only {len(controls)} quiet repeat controls fit {pipelines}; need "
                         f"{min_training} training plus {min_holdout} holdout")
    controls.sort(key=lambda control: hashlib.sha256(control[0].encode()).digest())
    holdout_size = min(max(min_holdout, len(controls) // 3), len(controls) - min_training)
    holdout, training = controls[:holdout_size], controls[holdout_size:]
    differences = [difference for _, difference, _ in training]
    offset = statistics.median(differences)
    scatter = 1.4826 * median_absolute_deviation(differences, offset)
    # The per-visit floors are deliberately retained in this uncertainty.
    representative_error = statistics.median(error for _, _, error in training)
    offset_error = math.hypot(representative_error, scatter) / math.sqrt(len(training))
    calibration = Calibration(pipeline_a, pipeline_b, frame_a, frame_b, offset, offset_error, scatter)
    calibrated = [difference - offset for _, difference, _ in holdout]
    standardized = [abs(residual) / math.hypot(error, scatter, offset_error)
                    for residual, (_, _, error) in zip(calibrated, holdout, strict=True)]
    if statistics.median(standardized) > 1 or sum(value > 3 for value in standardized) > .1 * len(holdout):
        raise ValueError("Held-out residuals exceed the calibration validation limits")
    return calibration, {
        "matched_sources": matched, "max_control_span_km_s": max_span,
        "training_sources": len(training), "holdout_sources": len(holdout),
        "holdout_median_residual_km_s": statistics.median(calibrated),
        "holdout_rms_km_s": math.sqrt(statistics.mean(value ** 2 for value in calibrated)),
        "holdout_max_abs_residual_km_s": max(map(abs, calibrated)),
        "holdout_max_abs_sigma": max(standardized),
    }
