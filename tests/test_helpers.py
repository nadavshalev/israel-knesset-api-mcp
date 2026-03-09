"""Unit tests for core/helpers.py."""

import sys
from pathlib import Path
from typing import Annotated
import unittest

from pydantic import Field

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.helpers import normalize_inputs


def typed_vote_filters(
    knesset_num: int | None = None,
    bill_id: int | None = None,
    from_date: str | None = None,
    accepted: bool | None = None,
) -> dict:
    return normalize_inputs(locals())


def annotated_filters(
    knesset_num: Annotated[int | None, Field(description="Knesset number")] = None,
    name: Annotated[str | None, Field(description="Name contains text")] = None,
    from_date: Annotated[str | None, Field(description="Start date (YYYY-MM-DD)")] = None,
    accepted: Annotated[bool | None, Field(description="Accepted filter")] = None,
) -> dict:
    return normalize_inputs(locals())


class TestNormalizeInputs(unittest.TestCase):
    # --- empty / whitespace strings become None ---

    def test_empty_string_in_integer_field_becomes_none(self):
        normalized = typed_vote_filters(
            knesset_num="",
            bill_id="   ",
            from_date="2026-03-01",
        )
        self.assertIsNone(normalized["knesset_num"])
        self.assertIsNone(normalized["bill_id"])
        self.assertEqual(normalized["from_date"], "2026-03-01")

    def test_none_string_becomes_none(self):
        normalized = typed_vote_filters(knesset_num="None", from_date="null")
        self.assertIsNone(normalized["knesset_num"])
        self.assertIsNone(normalized["from_date"])

    def test_undefined_string_becomes_none(self):
        normalized = typed_vote_filters(from_date="undefined")
        self.assertIsNone(normalized["from_date"])

    # --- int coercion ---

    def test_integer_string_is_converted(self):
        normalized = typed_vote_filters(knesset_num="20")
        self.assertEqual(normalized["knesset_num"], 20)

    def test_integer_passthrough(self):
        normalized = typed_vote_filters(knesset_num=25)
        self.assertEqual(normalized["knesset_num"], 25)

    def test_float_to_int_whole_number(self):
        normalized = typed_vote_filters(knesset_num=20.0)
        self.assertEqual(normalized["knesset_num"], 20)

    def test_float_to_int_fractional_raises(self):
        with self.assertRaises(ValueError):
            typed_vote_filters(knesset_num=20.5)

    def test_invalid_integer_raises(self):
        with self.assertRaises(ValueError):
            typed_vote_filters(knesset_num="abc")

    def test_bool_for_int_raises(self):
        with self.assertRaises(ValueError):
            typed_vote_filters(knesset_num=True)

    def test_list_for_int_raises(self):
        with self.assertRaises(ValueError):
            typed_vote_filters(knesset_num=[1, 2])

    # --- bool coercion ---

    def test_boolean_string_is_converted(self):
        self.assertTrue(typed_vote_filters(accepted="true")["accepted"])
        self.assertFalse(typed_vote_filters(accepted="false")["accepted"])
        self.assertTrue(typed_vote_filters(accepted="yes")["accepted"])
        self.assertFalse(typed_vote_filters(accepted="no")["accepted"])
        self.assertTrue(typed_vote_filters(accepted="1")["accepted"])
        self.assertFalse(typed_vote_filters(accepted="0")["accepted"])

    def test_boolean_passthrough(self):
        self.assertTrue(typed_vote_filters(accepted=True)["accepted"])
        self.assertFalse(typed_vote_filters(accepted=False)["accepted"])

    def test_int_to_bool(self):
        self.assertTrue(typed_vote_filters(accepted=1)["accepted"])
        self.assertFalse(typed_vote_filters(accepted=0)["accepted"])

    def test_int_2_for_bool_raises(self):
        with self.assertRaises(ValueError):
            typed_vote_filters(accepted=2)

    def test_invalid_bool_string_raises(self):
        with self.assertRaises(ValueError):
            typed_vote_filters(accepted="maybe")

    # --- str coercion ---

    def test_int_to_str(self):
        # int for a str field gets coerced to str, but may fail date validation
        result = normalize_inputs(
            {"name": 123},
            annotations={"name": str | None},
        )
        self.assertEqual(result["name"], "123")

    def test_bool_for_str_raises(self):
        with self.assertRaises(ValueError):
            typed_vote_filters(from_date=True)

    def test_list_for_str_raises(self):
        with self.assertRaises(ValueError):
            typed_vote_filters(from_date=["2026-03-01"])

    def test_string_max_length_rejected(self):
        long_str = "a" * 501
        with self.assertRaises(ValueError) as ctx:
            normalize_inputs(
                {"name": long_str},
                annotations={"name": str | None},
            )
        self.assertIn("too long", str(ctx.exception))

    def test_string_at_max_length_accepted(self):
        value = "a" * 500
        result = normalize_inputs(
            {"name": value},
            annotations={"name": str | None},
        )
        self.assertEqual(result["name"], value)

    # --- date validation ---

    def test_valid_date_accepted(self):
        normalized = typed_vote_filters(from_date="2026-03-01")
        self.assertEqual(normalized["from_date"], "2026-03-01")

    def test_invalid_date_format_raises(self):
        with self.assertRaises(ValueError) as ctx:
            typed_vote_filters(from_date="yesterday")
        self.assertIn("YYYY-MM-DD", str(ctx.exception))

    def test_date_without_hyphens_raises(self):
        with self.assertRaises(ValueError):
            typed_vote_filters(from_date="20260301")

    def test_date_none_skips_validation(self):
        normalized = typed_vote_filters(from_date=None)
        self.assertIsNone(normalized["from_date"])

    # --- Annotated type hints ---

    def test_annotated_int_coercion(self):
        normalized = annotated_filters(knesset_num="20")
        self.assertEqual(normalized["knesset_num"], 20)

    def test_annotated_str_passthrough(self):
        normalized = annotated_filters(name="test")
        self.assertEqual(normalized["name"], "test")

    def test_annotated_bool_coercion(self):
        normalized = annotated_filters(accepted="true")
        self.assertTrue(normalized["accepted"])

    def test_annotated_empty_string_becomes_none(self):
        normalized = annotated_filters(knesset_num="", name="  ")
        self.assertIsNone(normalized["knesset_num"])
        self.assertIsNone(normalized["name"])

    def test_annotated_date_validated(self):
        normalized = annotated_filters(from_date="2026-03-01")
        self.assertEqual(normalized["from_date"], "2026-03-01")

    def test_annotated_invalid_date_raises(self):
        with self.assertRaises(ValueError):
            annotated_filters(from_date="not-a-date")

    # --- explicit annotations kwarg ---

    def test_explicit_annotations(self):
        annotations = {"knesset_num": int | None, "name": str | None}
        result = normalize_inputs(
            {"knesset_num": "25", "name": 123},
            annotations=annotations,
        )
        self.assertEqual(result["knesset_num"], 25)
        self.assertEqual(result["name"], "123")

    def test_explicit_annotated_annotations(self):
        """Annotated types work when passed explicitly too."""
        annotations = {
            "num": Annotated[int | None, Field(description="A number")],
        }
        result = normalize_inputs({"num": "42"}, annotations=annotations)
        self.assertEqual(result["num"], 42)

    def test_explicit_annotations_override_introspection(self):
        """When annotations are passed explicitly, frame introspection is skipped."""
        result = normalize_inputs(
            {"value": "42"},
            annotations={"value": int},
        )
        self.assertEqual(result["value"], 42)

    # --- no annotation = passthrough ---

    def test_unknown_key_passes_through(self):
        result = normalize_inputs(
            {"unknown_param": [1, 2, 3]},
            annotations={},
        )
        self.assertEqual(result["unknown_param"], [1, 2, 3])

    # --- None passthrough ---

    def test_none_stays_none(self):
        normalized = typed_vote_filters(
            knesset_num=None, accepted=None, from_date=None
        )
        self.assertIsNone(normalized["knesset_num"])
        self.assertIsNone(normalized["accepted"])
        self.assertIsNone(normalized["from_date"])


if __name__ == "__main__":
    unittest.main()
