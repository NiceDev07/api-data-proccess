"""
Tests unitarios por step — un TestCase por clase de pipeline.

Cada test crea el DataFrame mínimo que el pipeline necesita recibir
(es decir, en el estado en que llega del paso anterior en la cadena real).
No se leen archivos CSV; todo el estado de entrada se construye inline.
"""
import math
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import polars as pl
import pytest

from modules.process.app.normalizers.email import EmailNormalizer
from modules.process.app.normalizers.number import NumberNormalizer
from modules.process.app.pipelines.sms.assign_cost import AssignCost
from modules.process.app.pipelines.callblasting.assign_cost import AssignCostCallBlasting
from modules.process.app.pipelines.email.assign_cost import AssignCostEmail
from modules.process.app.pipelines.sms.assign_operator import AssignOperator
from modules.process.app.pipelines.sms.calculate_credits import CalculateCredits
from modules.process.app.pipelines.callblasting.calculate_credits import CalculateCreditsCallBlasting
from modules.process.app.pipelines.email.calculate_credits import CalculateCreditsEmail
from modules.process.app.pipelines.callblasting.calculate_duration_custom import CalculateDurationCustom
from modules.process.app.pipelines.callblasting.calculate_duration_standard import CalculateDurationStandard
from modules.process.app.pipelines.sms.calculate_pdu import CalculatePDU
from modules.process.app.pipelines.sms.clean_data import CleanData
from modules.process.app.pipelines.email.clean_data import CleanDataEmail
from modules.process.app.pipelines.sms.concat_prefix import ConcatPrefix
from modules.process.app.pipelines.sms.custom_message import CustomMessage
from modules.process.app.pipelines.sms.exclution import Exclution
from modules.process.app.pipelines.email.exclution import ExclutionEmail
from modules.process.app.pipelines.email.extract_email_domain import ExtractEmailDomain
from modules.process.app.pipelines.sms.landing import Landing
from modules.process.app.pipelines.email.validate_email import ValidateEmail
from modules.process.app.pipelines.sms.validate_phone_length import ValidatePhoneLength
from modules.process.app.pipelines.sms.validate_regulations import ValidateRegulations
from modules.process.app.regulations.sms import (
    # CharLimitRegulation,  # desactivada — ver app/regulations/sms.py
    ShortNameRegulation,
    SpecialCharRegulation,
)
from modules.process.domain.constants.cols import Cols
from modules.process.domain.constants.reasons import ExclusionReason
from modules.process.domain.models.process_dto import ConfigListExclusion, RulesCountry

from modules.process.test.conftest import (
    BASE_RULES_CHILE,
    BASE_RULES_SMS,
    base_email_df,
    base_phone_df,
    make_ctx,
    make_excl_config,
)

pytestmark = pytest.mark.anyio


# ─────────────────────────────────────────────────────────────────────────────
# CleanData
# ─────────────────────────────────────────────────────────────────────────────

class TestCleanData:
    _ctx = make_ctx(demographic="phone", rules=BASE_RULES_SMS)  # mobile=10, fixed=7
    _pipe = CleanData(NumberNormalizer())

    async def test_strips_spaces_and_keeps_digits(self):
        df = pl.DataFrame({"phone": ["300 597 3563"]})
        result = await self._pipe.execute(df, self._ctx)
        assert result["phone"][0] == 3005973563

    async def test_removes_decimal_part(self):
        df = pl.DataFrame({"phone": ["3005973563.0"]})
        result = await self._pipe.execute(df, self._ctx)
        assert result["phone"][0] == 3005973563

    async def test_filters_null_rows(self):
        df = pl.DataFrame({"phone": [None, "3005973563"]})
        result = await self._pipe.execute(df, self._ctx)
        assert len(result) == 1

    async def test_filters_numbers_too_short(self):
        # MIN = max(mobile=10, fixed=7) = 10 → 9-digit number must be filtered
        df = pl.DataFrame({"phone": ["123456789"]})   # 9 digits
        result = await self._pipe.execute(df, self._ctx)
        assert len(result) == 0

    async def test_keeps_valid_mobile(self):
        df = pl.DataFrame({"phone": ["3005973563", "3208392650"]})
        result = await self._pipe.execute(df, self._ctx)
        assert len(result) == 2


# ─────────────────────────────────────────────────────────────────────────────
# ConcatPrefix
# ─────────────────────────────────────────────────────────────────────────────

class TestConcatPrefix:
    async def test_colombia_prefix(self):
        df = base_phone_df([3005973563])
        ctx = make_ctx(demographic="phone", rules=BASE_RULES_SMS)  # codeCountry=57, mobile=10
        result = await ConcatPrefix().execute(df, ctx)
        assert result[Cols.number_concat][0] == 573005973563

    async def test_chile_prefix(self):
        df = pl.DataFrame(
            {"phone": [912345678], Cols.is_ok: [True], Cols.error_code: [None]},
            schema={"phone": pl.Int64, Cols.is_ok: pl.Boolean, Cols.error_code: pl.Utf8},
        )
        ctx = make_ctx(demographic="phone", rules=BASE_RULES_CHILE)  # codeCountry=56, mobile=9
        result = await ConcatPrefix().execute(df, ctx)
        assert result[Cols.number_concat][0] == 56912345678


# ─────────────────────────────────────────────────────────────────────────────
# CustomMessage
# ─────────────────────────────────────────────────────────────────────────────

class TestCustomMessage:
    async def test_no_tags_assigns_literal(self):
        df = pl.DataFrame({"phone": [1, 2]})
        ctx = make_ctx(content="Mensaje fijo sin tags.")
        result = await CustomMessage().execute(df, ctx)
        assert (result[Cols.message] == "Mensaje fijo sin tags.").all()

    async def test_single_tag_replaced(self):
        df = pl.DataFrame({"phone": [1], "nombre": ["Juan"]})
        ctx = make_ctx(content="Hola {nombre}, bienvenido.")
        result = await CustomMessage().execute(df, ctx)
        assert result[Cols.message][0] == "Hola Juan, bienvenido."

    async def test_multiple_tags_replaced_in_order(self):
        df = pl.DataFrame({"phone": [1], "name": ["Ana"], "city": ["Bogotá"]})
        ctx = make_ctx(content="{name} vive en {city}.")
        result = await CustomMessage().execute(df, ctx)
        assert result[Cols.message][0] == "Ana vive en Bogotá."

    async def test_each_row_gets_its_own_value(self):
        df = pl.DataFrame({"phone": [1, 2], "nombre": ["Juan", "María"]})
        ctx = make_ctx(content="Hola {nombre}")
        result = await CustomMessage().execute(df, ctx)
        assert result[Cols.message][0] == "Hola Juan"
        assert result[Cols.message][1] == "Hola María"

    async def test_null_tag_value_does_not_blank_full_row(self):
        # Regresión: celdas vacías en columnas de tag dejaban el mensaje
        # completo en null por concat_str, y el proveedor descartaba el envío.
        df = pl.DataFrame({"phone": [1, 2, 3], "referencia": ["ABC", None, "XYZ"]})
        ctx = make_ctx(content="Su referencia es {referencia}.")
        result = await CustomMessage().execute(df, ctx)
        assert result[Cols.message][0] == "Su referencia es ABC."
        assert result[Cols.message][1] == "Su referencia es ."
        assert result[Cols.message][2] == "Su referencia es XYZ."


# ─────────────────────────────────────────────────────────────────────────────
# Landing
# ─────────────────────────────────────────────────────────────────────────────

class TestLanding:
    async def test_non_landing_subservice_passthrough(self):
        df = pl.DataFrame({Cols.message: ["Sin URL"]})
        ctx = make_ctx(sub_service="informative", content="Sin URL")
        result = await Landing().execute(df, ctx)
        assert result.shape == df.shape

    async def test_landing_with_url_passthrough(self):
        df = pl.DataFrame({Cols.message: ["Visita https://short.url ahora"]})
        ctx = make_ctx(sub_service="landing", content="Visita https://short.url ahora")
        result = await Landing().execute(df, ctx)
        assert result.shape == df.shape

    async def test_landing_without_url_raises(self):
        df = pl.DataFrame({Cols.message: ["Sin enlace"]})
        ctx = make_ctx(sub_service="landing", content="Sin enlace")
        with pytest.raises(ValueError, match="URL"):
            await Landing().execute(df, ctx)


# ─────────────────────────────────────────────────────────────────────────────
# CalculatePDU
# ─────────────────────────────────────────────────────────────────────────────

class TestCalculatePDU:
    _pipe = CalculatePDU()
    _ctx  = make_ctx(sub_service="informative")

    async def test_single_pdu_ascii_at_limit(self):
        df = pl.DataFrame({Cols.message: ["A" * 160]})
        result = await self._pipe.execute(df, self._ctx)
        assert result[Cols.pdu][0] == 1

    async def test_two_pdu_ascii_over_limit(self):
        # ceil(161 / 153) = 2
        df = pl.DataFrame({Cols.message: ["A" * 161]})
        result = await self._pipe.execute(df, self._ctx)
        assert result[Cols.pdu][0] == 2

    async def test_three_pdu_ascii(self):
        # ceil(307 / 153) = ceil(2.007) = 3
        df = pl.DataFrame({Cols.message: ["A" * 307]})
        result = await self._pipe.execute(df, self._ctx)
        assert result[Cols.pdu][0] == 3

    async def test_single_pdu_unicode_at_limit(self):
        # "á" is 2 bytes → is_special=True, base=70
        df = pl.DataFrame({Cols.message: ["á" * 70]})
        result = await self._pipe.execute(df, self._ctx)
        assert result[Cols.pdu][0] == 1
        assert result[Cols.is_special][0] is True

    async def test_two_pdu_unicode_over_limit(self):
        # ceil(71 / 67) = 2
        df = pl.DataFrame({Cols.message: ["á" * 71]})
        result = await self._pipe.execute(df, self._ctx)
        assert result[Cols.pdu][0] == 2

    async def test_ascii_not_flagged_as_special(self):
        df = pl.DataFrame({Cols.message: ["Hello"]})
        result = await self._pipe.execute(df, self._ctx)
        assert result[Cols.is_special][0] is False


# ─────────────────────────────────────────────────────────────────────────────
# CalculateCredits
# ─────────────────────────────────────────────────────────────────────────────

class TestCalculateCredits:
    async def test_credits_equal_pdu_times_cost(self):
        df = pl.DataFrame({Cols.pdu: [1, 2, 3], Cols.cost: [0.5, 0.5, 0.5]})
        result = await CalculateCredits().execute(df, make_ctx())
        assert result[Cols.credits].to_list() == pytest.approx([0.5, 1.0, 1.5])

    async def test_zero_cost_gives_zero_credits(self):
        df = pl.DataFrame({Cols.pdu: [2], Cols.cost: [0.0]})
        result = await CalculateCredits().execute(df, make_ctx())
        assert result[Cols.credits][0] == pytest.approx(0.0)


# ─────────────────────────────────────────────────────────────────────────────
# AssignCost (join-based)
# ─────────────────────────────────────────────────────────────────────────────

class TestAssignCost:
    async def _run(self, number: str, prefix_costs: list[tuple]) -> pl.DataFrame:
        df = pl.DataFrame({Cols.number_concat: [number]})
        mock = MagicMock()
        mock.get_costs = AsyncMock(return_value=prefix_costs)
        return await AssignCost(mock, service="sms").execute(df, make_ctx())

    async def test_exact_prefix_match(self):
        result = await self._run("569123456789", [("569", 0.05, "MOVISTAR"), ("56", 0.01, "CHILE")])
        assert result[Cols.cost][0] == pytest.approx(0.05)
        assert result[Cols.cost_operator][0] == "MOVISTAR"

    async def test_longer_prefix_wins(self):
        result = await self._run(
            "56912345678",
            [("56", 0.01, "SHORT"), ("569", 0.05, "MEDIUM"), ("5691", 0.10, "LONG")],
        )
        assert result[Cols.cost][0] == pytest.approx(0.10)
        assert result[Cols.cost_operator][0] == "LONG"

    async def test_no_match_returns_default(self):
        result = await self._run("999999999", [("56", 0.05, "CHILE")])
        assert result[Cols.cost][0] == pytest.approx(0.0)
        assert result[Cols.cost_operator][0] == ""

    async def test_empty_prefix_table_returns_default(self):
        result = await self._run("569123456", [])
        assert result[Cols.cost][0] == pytest.approx(0.0)

    async def test_multiple_rows_independent_matching(self):
        df = pl.DataFrame({Cols.number_concat: ["56912345678", "56712345678"]})
        mock = MagicMock()
        mock.get_costs = AsyncMock(return_value=[("569", 0.05, "A"), ("567", 0.03, "B")])
        result = await AssignCost(mock, service="sms").execute(df, make_ctx())
        assert result[Cols.cost][0] == pytest.approx(0.05)
        assert result[Cols.cost][1] == pytest.approx(0.03)


# ─────────────────────────────────────────────────────────────────────────────
# AssignCostCallBlasting
# ─────────────────────────────────────────────────────────────────────────────

def _cb_df(numbers: list[str]) -> pl.DataFrame:
    return pl.DataFrame({
        Cols.number_concat: numbers,
        Cols.is_ok:         [True] * len(numbers),
        Cols.error_code:    [""] * len(numbers),
    })


class TestAssignCostCallBlasting:
    async def test_assigns_all_four_fields(self):
        df = _cb_df(["573005973563"])
        mock = MagicMock()
        mock.get_costs_cb = AsyncMock(return_value=[("57", 60.0, "COLOMBIA", 30.0, 15.0)])
        ctx = make_ctx(sub_service="standard")
        result = await AssignCostCallBlasting(mock).execute(df, ctx)
        assert result[Cols.cost][0]          == pytest.approx(60.0)
        assert result[Cols.cost_operator][0] == "COLOMBIA"
        assert result[Cols.initial][0]       == pytest.approx(30.0)
        assert result[Cols.incremental][0]   == pytest.approx(15.0)
        assert result[Cols.is_ok][0] is True

    async def test_empty_table_returns_zeros(self):
        df = _cb_df(["573005973563"])
        mock = MagicMock()
        mock.get_costs_cb = AsyncMock(return_value=[])
        result = await AssignCostCallBlasting(mock).execute(df, make_ctx())
        assert result[Cols.cost][0]    == pytest.approx(0.0)
        assert result[Cols.initial][0] == pytest.approx(0.0)

    async def test_no_cost_match_marks_excluded(self):
        """Número sin tarifa configurada se excluye con NO_COST."""
        df = _cb_df(["999999999999"])
        mock = MagicMock()
        mock.get_costs_cb = AsyncMock(return_value=[("57", 60.0, "COLOMBIA", 30.0, 15.0)])
        result = await AssignCostCallBlasting(mock).execute(df, make_ctx())
        assert result[Cols.is_ok][0] is False
        assert result[Cols.error_code][0] == "NO_COST"


# ─────────────────────────────────────────────────────────────────────────────
# AssignCostEmail
# ─────────────────────────────────────────────────────────────────────────────

class TestAssignCostEmail:
    async def _run(self, cost: float) -> pl.DataFrame:
        df = pl.DataFrame({Cols.email_domain: ["gmail.com"]})
        mock = MagicMock()
        mock.get_email_cost = AsyncMock(return_value=cost)
        return await AssignCostEmail(mock).execute(df, make_ctx())

    async def test_domain_prefix_match(self):
        result = await self._run(0.02)
        assert result[Cols.cost][0] == pytest.approx(0.02)

    async def test_catchall_empty_prefix(self):
        result = await self._run(0.01)
        assert result[Cols.cost][0] == pytest.approx(0.01)

    async def test_no_match_no_catchall_returns_default(self):
        result = await self._run(0.0)
        assert result[Cols.cost][0] == pytest.approx(0.0)


# ─────────────────────────────────────────────────────────────────────────────
# AssignOperator
# ─────────────────────────────────────────────────────────────────────────────

class TestAssignOperator:
    async def _mock_service(self, starts, ends, operators):
        mock = MagicMock()
        mock.get_ranges = AsyncMock(return_value=(
            np.array(starts, dtype=np.int64),
            np.array(ends,   dtype=np.int64),
            np.array(operators, dtype="U50"),
        ))
        return mock

    async def test_number_in_range_gets_operator(self):
        df = base_phone_df([3005973563])
        svc = await self._mock_service([3000000000], [3099999999], ["CLARO"])
        result = await AssignOperator(svc).execute(df, make_ctx(demographic="phone"))
        assert result[Cols.number_operator][0] == "CLARO"
        assert result[Cols.is_ok][0] is True

    async def test_number_outside_ranges_marked_no_operator(self):
        df = base_phone_df([9999999999])
        svc = await self._mock_service([3000000000], [3099999999], ["CLARO"])
        result = await AssignOperator(svc).execute(df, make_ctx(demographic="phone"))
        assert result[Cols.is_ok][0] is False
        assert result[Cols.error_code][0] == ExclusionReason.NO_OPERATOR

    async def test_multiple_numbers_correct_ranges(self):
        df = base_phone_df([3005973563, 3208392650])
        svc = await self._mock_service(
            [3000000000, 3200000000],
            [3099999999, 3299999999],
            ["CLARO", "MOVISTAR"],
        )
        result = await AssignOperator(svc).execute(df, make_ctx(demographic="phone"))
        assert result[Cols.number_operator][0] == "CLARO"
        assert result[Cols.number_operator][1] == "MOVISTAR"

    async def test_already_excluded_stays_excluded(self):
        df = pl.DataFrame(
            {"phone": [9999999999], Cols.is_ok: [False], Cols.error_code: [ExclusionReason.EXCLUSION_LIST]},
            schema={"phone": pl.Int64, Cols.is_ok: pl.Boolean, Cols.error_code: pl.Utf8},
        )
        svc = await self._mock_service([3000000000], [3099999999], ["CLARO"])
        result = await AssignOperator(svc).execute(df, make_ctx(demographic="phone"))
        # El error_code original se preserva — AssignOperator no sobreescribe exclusiones previas.
        assert result[Cols.is_ok][0] is False
        assert result[Cols.error_code][0] == ExclusionReason.EXCLUSION_LIST


# ─────────────────────────────────────────────────────────────────────────────
# Exclution
# ─────────────────────────────────────────────────────────────────────────────

class TestExclution:
    _normalizer = NumberNormalizer()

    async def test_disabled_marks_all_ok(self):
        df = pl.DataFrame({"phone": [111, 222]}, schema={"phone": pl.Int64})
        mock = MagicMock()
        result = await Exclution(mock, self._normalizer).execute(df, make_ctx(use_exclusion=False))
        assert result[Cols.is_ok].all()

    async def test_excludes_matching_number(self):
        df = pl.DataFrame({"phone": [3005973563, 3208392650]}, schema={"phone": pl.Int64})
        excl_df = pl.DataFrame({"phone": [3005973563]}, schema={"phone": pl.Int64})
        mock = MagicMock()
        mock.get_df = AsyncMock(return_value=excl_df)
        ctx = make_ctx(use_exclusion=True, excl_config=make_excl_config())
        result = await Exclution(mock, self._normalizer).execute(df, ctx)
        assert result[Cols.is_ok][0] is False
        assert result[Cols.error_code][0] == ExclusionReason.EXCLUSION_LIST
        assert result[Cols.is_ok][1] is True

    async def test_empty_exclusion_list_marks_all_ok(self):
        df = pl.DataFrame({"phone": [123456789]}, schema={"phone": pl.Int64})
        mock = MagicMock()
        mock.get_df = AsyncMock(return_value=pl.DataFrame({"phone": []}, schema={"phone": pl.Int64}))
        ctx = make_ctx(use_exclusion=True, excl_config=make_excl_config())
        result = await Exclution(mock, self._normalizer).execute(df, ctx)
        assert result[Cols.is_ok][0] is True


# ─────────────────────────────────────────────────────────────────────────────
# CleanDataEmail
# ─────────────────────────────────────────────────────────────────────────────

class TestCleanDataEmail:
    _pipe = CleanDataEmail(EmailNormalizer())
    _ctx  = make_ctx(demographic="email")

    async def test_lowercases_email(self):
        df = pl.DataFrame({"email": ["USER@GMAIL.COM"]})
        result = await self._pipe.execute(df, self._ctx)
        assert result["email"][0] == "user@gmail.com"

    async def test_strips_whitespace(self):
        df = pl.DataFrame({"email": ["  user@gmail.com  "]})
        result = await self._pipe.execute(df, self._ctx)
        assert result["email"][0] == "user@gmail.com"

    async def test_filters_null_rows(self):
        df = pl.DataFrame({"email": [None, "valid@test.com"]})
        result = await self._pipe.execute(df, self._ctx)
        assert len(result) == 1
        assert result["email"][0] == "valid@test.com"

    async def test_filters_empty_string(self):
        df = pl.DataFrame({"email": ["", "valid@test.com"]})
        result = await self._pipe.execute(df, self._ctx)
        assert len(result) == 1


# ─────────────────────────────────────────────────────────────────────────────
# ExtractEmailDomain
# ─────────────────────────────────────────────────────────────────────────────

class TestExtractEmailDomain:
    _pipe = ExtractEmailDomain()
    _ctx  = make_ctx(demographic="email")

    async def test_extracts_simple_domain(self):
        df = pl.DataFrame({Cols.email: ["user@gmail.com"]})
        result = await self._pipe.execute(df, self._ctx)
        assert result[Cols.email_domain][0] == "gmail.com"

    async def test_extracts_subdomain(self):
        df = pl.DataFrame({Cols.email: ["user@mail.company.org"]})
        result = await self._pipe.execute(df, self._ctx)
        assert result[Cols.email_domain][0] == "mail.company.org"

    async def test_multiple_rows(self):
        df = pl.DataFrame({Cols.email: ["a@gmail.com", "b@hotmail.com"]})
        result = await self._pipe.execute(df, self._ctx)
        assert result[Cols.email_domain].to_list() == ["gmail.com", "hotmail.com"]


# ─────────────────────────────────────────────────────────────────────────────
# ValidateEmail
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateEmail:
    _pipe = ValidateEmail()
    _ctx  = make_ctx(demographic="email")

    async def test_valid_email_stays_ok(self):
        df = base_email_df(["test@gmail.com"])
        result = await self._pipe.execute(df, self._ctx)
        assert result[Cols.is_ok][0] is True

    async def test_invalid_email_marked_false(self):
        df = base_email_df(["not-an-email"])
        result = await self._pipe.execute(df, self._ctx)
        assert result[Cols.is_ok][0] is False
        assert result[Cols.error_code][0] == ExclusionReason.INVALID_EMAIL

    async def test_already_excluded_keeps_original_code(self):
        df = pl.DataFrame(
            {Cols.email: ["bad"], Cols.is_ok: [False], Cols.error_code: [ExclusionReason.EXCLUSION_LIST]},
            schema={Cols.email: pl.Utf8, Cols.is_ok: pl.Boolean, Cols.error_code: pl.Utf8},
        )
        result = await self._pipe.execute(df, self._ctx)
        assert result[Cols.error_code][0] == ExclusionReason.EXCLUSION_LIST

    async def test_mixed_batch(self):
        df = base_email_df(["valid@test.com", "invalid", "also@valid.io"])
        result = await self._pipe.execute(df, self._ctx)
        assert result[Cols.is_ok].to_list() == [True, False, True]


# ─────────────────────────────────────────────────────────────────────────────
# ExclutionEmail
# ─────────────────────────────────────────────────────────────────────────────

class TestExclutionEmail:
    _normalizer = EmailNormalizer()

    async def test_disabled_marks_all_ok(self):
        df = pl.DataFrame({"email": ["a@b.com"]})
        mock = MagicMock()
        result = await ExclutionEmail(mock, self._normalizer).execute(
            df, make_ctx(demographic="email", use_exclusion=False)
        )
        assert result[Cols.is_ok][0] is True

    async def test_excludes_matching_email(self):
        df = pl.DataFrame({Cols.email: ["bad@test.com", "good@test.com"]})
        excl_df = pl.DataFrame({"email": ["bad@test.com"]})
        mock = MagicMock()
        mock.get_df = AsyncMock(return_value=excl_df)
        ctx = make_ctx(demographic="email", use_exclusion=True, excl_config=make_excl_config(demographic="email"))
        result = await ExclutionEmail(mock, self._normalizer).execute(df, ctx)
        assert result[Cols.is_ok][0] is False
        assert result[Cols.error_code][0] == ExclusionReason.EXCLUSION_LIST
        assert result[Cols.is_ok][1] is True


# ─────────────────────────────────────────────────────────────────────────────
# ValidatePhoneLength
# ─────────────────────────────────────────────────────────────────────────────

class TestValidatePhoneLength:
    # Colombia: mobile=10, fixed=7
    _ctx = make_ctx(demographic="phone", rules=BASE_RULES_SMS)

    async def test_valid_mobile_length_stays_ok(self):
        df = base_phone_df([3005973563])  # 10 digits
        result = await ValidatePhoneLength().execute(df, self._ctx)
        assert result[Cols.is_ok][0] is True

    async def test_invalid_length_marked_false(self):
        # 9 digits: neither mobile(10) nor fixed(7)
        df = base_phone_df([300597356])
        result = await ValidatePhoneLength().execute(df, self._ctx)
        assert result[Cols.is_ok][0] is False
        assert result[Cols.error_code][0] == ExclusionReason.INVALID_NUMBER_LENGTH

    async def test_fixed_length_accepted(self):
        df = base_phone_df([1234567])   # 7 digits = fixed
        result = await ValidatePhoneLength().execute(df, self._ctx)
        assert result[Cols.is_ok][0] is True

    async def test_already_excluded_not_overwritten(self):
        df = pl.DataFrame(
            {"phone": [300597356], Cols.is_ok: [False], Cols.error_code: [ExclusionReason.EXCLUSION_LIST]},
            schema={"phone": pl.Int64, Cols.is_ok: pl.Boolean, Cols.error_code: pl.Utf8},
        )
        result = await ValidatePhoneLength().execute(df, self._ctx)
        assert result[Cols.error_code][0] == ExclusionReason.EXCLUSION_LIST


# ─────────────────────────────────────────────────────────────────────────────
# ShortNameRegulation
# ─────────────────────────────────────────────────────────────────────────────

class TestShortNameRegulation:
    _reg = ShortNameRegulation()

    def _df(self, messages: list[str]) -> pl.DataFrame:
        return pl.DataFrame(
            {Cols.message: messages, Cols.is_ok: [True] * len(messages), Cols.error_code: [None] * len(messages)},
            schema={Cols.message: pl.Utf8, Cols.is_ok: pl.Boolean, Cols.error_code: pl.Utf8},
        )

    def test_disabled_noop(self):
        df = self._df(["Sin shortname"])
        ctx = make_ctx(shortname="TEST", content="Sin shortname")
        from modules.process.domain.models.process_dto import RulesCountry
        rules = RulesCountry(**{**BASE_RULES_SMS.model_dump(), "useShortName": False})
        ctx2 = make_ctx(shortname="TEST", rules=rules)
        result = self._reg.validate(self._df(["Sin shortname"]), ctx2)
        assert result[Cols.is_ok][0] is True

    def test_shortname_in_template_fast_path(self):
        ctx = make_ctx(shortname="SAEM3", content="Oferta SAEM3 disponible")
        rules = RulesCountry(**{**BASE_RULES_SMS.model_dump(), "useShortName": True})
        ctx2 = make_ctx(shortname="SAEM3", content="Oferta SAEM3 disponible", rules=rules)
        result = self._reg.validate(self._df(["Oferta SAEM3 disponible"]), ctx2)
        assert result[Cols.is_ok][0] is True

    def test_shortname_missing_marks_row(self):
        rules = RulesCountry(**{**BASE_RULES_SMS.model_dump(), "useShortName": True})
        ctx = make_ctx(shortname="EMPRESA", content="Mensaje sin empresa", rules=rules)
        result = self._reg.validate(self._df(["Mensaje sin empresa"]), ctx)
        assert result[Cols.is_ok][0] is False
        assert result[Cols.error_code][0] == ExclusionReason.SHORTNAME_MISSING

    def test_partial_rows_with_shortname(self):
        rules = RulesCountry(**{**BASE_RULES_SMS.model_dump(), "useShortName": True})
        ctx = make_ctx(shortname="CORP", rules=rules)
        df = self._df(["Hola de parte de CORP", "Hola sin shortname"])
        result = self._reg.validate(df, ctx)
        assert result[Cols.is_ok][0] is True
        assert result[Cols.is_ok][1] is False


# ─────────────────────────────────────────────────────────────────────────────
# SpecialCharRegulation
# ─────────────────────────────────────────────────────────────────────────────

class TestSpecialCharRegulation:
    _reg = SpecialCharRegulation()

    def _df(self, is_special_flags: list[bool]) -> pl.DataFrame:
        return pl.DataFrame(
            {Cols.is_special: is_special_flags, Cols.is_ok: [True] * len(is_special_flags), Cols.error_code: [None] * len(is_special_flags)},
            schema={Cols.is_special: pl.Boolean, Cols.is_ok: pl.Boolean, Cols.error_code: pl.Utf8},
        )

    def test_country_allows_special_noop(self):
        rules = RulesCountry(**{**BASE_RULES_SMS.model_dump(), "useCharacterSpecial": True})
        ctx = make_ctx(rules=rules)
        df = self._df([True, False])
        result = self._reg.validate(df, ctx)
        assert result[Cols.is_ok].all()

    def test_country_disallows_marks_special_rows(self):
        rules = RulesCountry(**{**BASE_RULES_SMS.model_dump(), "useCharacterSpecial": False})
        ctx = make_ctx(rules=rules)
        df = self._df([True, False])
        result = self._reg.validate(df, ctx)
        assert result[Cols.is_ok][0] is False
        assert result[Cols.error_code][0] == ExclusionReason.SPECIAL_CHAR_NOT_ALLOWED
        assert result[Cols.is_ok][1] is True


# ─────────────────────────────────────────────────────────────────────────────
# CharLimitRegulation — DESACTIVADA. Tests comentados por si se reactiva.
# ─────────────────────────────────────────────────────────────────────────────

# class TestCharLimitRegulation:
#     _reg = SpecialCharRegulation()
#
#     def _df(self, lengths: list[int], is_special: list[bool]) -> pl.DataFrame:
#         return pl.DataFrame(
#             {
#                 Cols.length:     lengths,
#                 Cols.is_special: is_special,
#                 Cols.is_ok:      [True] * len(lengths),
#                 Cols.error_code: [None] * len(lengths),
#             },
#             schema={
#                 Cols.length: pl.Int32, Cols.is_special: pl.Boolean,
#                 Cols.is_ok: pl.Boolean, Cols.error_code: pl.Utf8,
#             },
#         )
#
#     def test_within_standard_limit_ok(self):
#         rules = RulesCountry(**{**BASE_RULES_SMS.model_dump(), "limitCharacter": 160, "useCharacterSpecial": True})
#         ctx = make_ctx(rules=rules)
#         df = self._df([160], [False])
#         result = CharLimitRegulation().validate(df, ctx)
#         assert result[Cols.is_ok][0] is True
#
#     def test_over_standard_limit_marked(self):
#         rules = RulesCountry(**{**BASE_RULES_SMS.model_dump(), "limitCharacter": 160})
#         ctx = make_ctx(rules=rules)
#         df = self._df([161], [False])
#         result = CharLimitRegulation().validate(df, ctx)
#         assert result[Cols.is_ok][0] is False
#         assert result[Cols.error_code][0] == ExclusionReason.CHAR_LIMIT_EXCEEDED
#
#     def test_special_char_uses_special_limit(self):
#         rules = RulesCountry(**{**BASE_RULES_SMS.model_dump(), "limitCharacter": 160, "limitCharacterSpecial": 70})
#         ctx = make_ctx(rules=rules)
#         df = self._df([71], [True])
#         result = CharLimitRegulation().validate(df, ctx)
#         assert result[Cols.is_ok][0] is False
#         assert result[Cols.error_code][0] == ExclusionReason.CHAR_LIMIT_EXCEEDED


# ─────────────────────────────────────────────────────────────────────────────
# ValidateRegulations (pipeline)
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateRegulations:
    async def test_applies_all_regulations_in_order(self):
        """Dos regulaciones: la primera marca row 0, la segunda marca row 1."""
        from modules.process.domain.interfaces.regulation import IRegulation
        from modules.process.domain.models.process_dto import DataProcessingDTO

        class MarkFirstRegulation(IRegulation):
            def validate(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
                return df.with_columns(
                    pl.when(pl.col(Cols.is_ok) & (pl.col("idx") == 0))
                    .then(pl.lit(False)).otherwise(pl.col(Cols.is_ok)).alias(Cols.is_ok),
                    pl.when(pl.col("idx") == 0)
                    .then(pl.lit("REG_A")).otherwise(pl.col(Cols.error_code)).alias(Cols.error_code),
                )

        class MarkSecondRegulation(IRegulation):
            def validate(self, df: pl.DataFrame, ctx: DataProcessingDTO) -> pl.DataFrame:
                return df.with_columns(
                    pl.when(pl.col(Cols.is_ok) & (pl.col("idx") == 1))
                    .then(pl.lit(False)).otherwise(pl.col(Cols.is_ok)).alias(Cols.is_ok),
                    pl.when(pl.col("idx") == 1)
                    .then(pl.lit("REG_B")).otherwise(pl.col(Cols.error_code)).alias(Cols.error_code),
                )

        df = pl.DataFrame(
            {"idx": [0, 1], Cols.is_ok: [True, True], Cols.error_code: [None, None]},
            schema={"idx": pl.Int32, Cols.is_ok: pl.Boolean, Cols.error_code: pl.Utf8},
        )
        pipe = ValidateRegulations([MarkFirstRegulation(), MarkSecondRegulation()])
        result = await pipe.execute(df, make_ctx())
        assert result[Cols.is_ok][0] is False
        assert result[Cols.error_code][0] == "REG_A"
        assert result[Cols.is_ok][1] is False
        assert result[Cols.error_code][1] == "REG_B"


# ─────────────────────────────────────────────────────────────────────────────
# CalculateDurationCustom (per-record)
# ─────────────────────────────────────────────────────────────────────────────

class TestCalculateDurationCustom:
    _pipe = CalculateDurationCustom()

    async def test_duration_per_record(self):
        # Fórmula: ceil(words / 170 * 60 + 7)
        # 85 words  → ceil(30 + 7)  = ceil(37)  = 37
        # 170 words → ceil(60 + 7)  = ceil(67)  = 67
        words_85  = " ".join(["hola"] * 85)
        words_170 = " ".join(["test"] * 170)
        df = pl.DataFrame(
            {Cols.message: [words_85, words_170], Cols.is_ok: [True, True]},
        )
        result = await self._pipe.execute(df, make_ctx())
        assert result[Cols.seconds][0] == 37   # <-- CAMBIAR a 35 para forzar fallo
        assert result[Cols.seconds][1] == 67   # <-- CAMBIAR a 65 para forzar fallo

    async def test_single_word_gets_minimum_plus_margin(self):
        df = pl.DataFrame({Cols.message: ["hola"], Cols.is_ok: [True]})
        result = await self._pipe.execute(df, make_ctx())
        # ceil(1/170*60 + 7) = ceil(0.35 + 7) = ceil(7.35) = 8
        assert result[Cols.seconds][0] == 8    # <-- CAMBIAR a 6 para forzar fallo

    async def test_compact_text_fallback(self):
        """Texto largo sin espacios: fallback len//5 para estimar palabras."""
        # 1 palabra detectada pero 110 caracteres → fallback: max(1, 110//5) = 22 palabras
        # ceil(22/170*60 + 7) = ceil(7.76 + 7) = ceil(14.76) = 15
        compact = "a" * 110
        df = pl.DataFrame({Cols.message: [compact], Cols.is_ok: [True]})
        result = await self._pipe.execute(df, make_ctx())
        assert result[Cols.seconds][0] == 15   # <-- CAMBIAR a 8 para forzar fallo

    async def test_excludes_tmp_columns(self):
        df = pl.DataFrame({Cols.message: ["uno dos tres"], Cols.is_ok: [True]})
        result = await self._pipe.execute(df, make_ctx())
        assert "__cb_word_count__" not in result.columns  # <-- CAMBIAR a "message" para forzar fallo


# ─────────────────────────────────────────────────────────────────────────────
# CalculateDurationStandard
# ─────────────────────────────────────────────────────────────────────────────

class TestCalculateDurationStandard:
    async def test_from_audio_path_calls_provider(self):
        """El provider devuelve ceil(audio) + 5s de margen — el pipeline lo usa directo."""
        df = pl.DataFrame({"phone": [1, 2]})
        provider = MagicMock()
        provider.get_duration = AsyncMock(return_value=36)  # ceil(30.5)+5 ya viene del provider
        ctx = make_ctx(audio_path="/audio/test.mp3")
        result = await CalculateDurationStandard(provider).execute(df, ctx)
        provider.get_duration.assert_called_once_with("/audio/test.mp3")
        assert (result[Cols.seconds] == 36).all()  # <-- CAMBIAR a 30 para forzar fallo

    async def test_duration_applied_to_all_records(self):
        """El mismo valor de duración se aplica a todos los registros."""
        df = pl.DataFrame({"phone": [1, 2, 3]})
        provider = MagicMock()
        provider.get_duration = AsyncMock(return_value=11)  # ceil(5.319)+5 = 11
        ctx = make_ctx(audio_path="/audio/test.mp3")
        result = await CalculateDurationStandard(provider).execute(df, ctx)
        assert (result[Cols.seconds] == 11).all()  # <-- CAMBIAR a 10 para forzar fallo

    async def test_minimum_one_second(self):
        """Si el provider devuelve 0 (audio vacío), max(1,0) garantiza al menos 1 segundo."""
        df = pl.DataFrame({"phone": [1]})
        provider = MagicMock()
        provider.get_duration = AsyncMock(return_value=0)
        ctx = make_ctx(audio_path="/audio/silence.mp3")
        result = await CalculateDurationStandard(provider).execute(df, ctx)
        assert (result[Cols.seconds] == 1).all()   # <-- CAMBIAR a 0 para forzar fallo


# ─────────────────────────────────────────────────────────────────────────────
# CalculateCreditsCallBlasting
# ─────────────────────────────────────────────────────────────────────────────

class TestCalculateCreditsCallBlasting:
    _pipe = CalculateCreditsCallBlasting()

    def _df(self, seconds, initial, incremental, cost):
        return pl.DataFrame({
            Cols.seconds:     [seconds],
            Cols.initial:     [float(initial)],
            Cols.incremental: [float(incremental)],
            Cols.cost:        [float(cost)],
        })

    async def test_above_initial_uses_ceil_formula(self):
        # seconds=45, initial=30, incremental=15, cost=60
        # cycles=ceil(45/15)=3 → credits=3×15×(60/60)=45
        result = await self._pipe.execute(self._df(45, 30, 15, 60.0), make_ctx())
        assert result[Cols.credits][0] == pytest.approx(45.0)

    async def test_below_initial_uses_initial_as_cycles(self):
        # seconds=10, initial=30, incremental=1, cost=60
        # cycles=30 → credits=30×1×1=30
        result = await self._pipe.execute(self._df(10, 30, 1, 60.0), make_ctx())
        assert result[Cols.credits][0] == pytest.approx(30.0)

    async def test_exact_initial_uses_ceil_formula(self):
        # seconds=30, initial=30 → seconds > initial is False → cycles=initial=30
        result = await self._pipe.execute(self._df(30, 30, 1, 60.0), make_ctx())
        assert result[Cols.credits][0] == pytest.approx(30.0)

    async def test_fractional_cost_per_second(self):
        # cost=30 (per minute) → cost_per_second=0.5
        # seconds=60, initial=1, incremental=1 → cycles=60 → credits=60×1×0.5=30
        result = await self._pipe.execute(self._df(60, 1, 1, 30.0), make_ctx())
        assert result[Cols.credits][0] == pytest.approx(30.0)


# ─────────────────────────────────────────────────────────────────────────────
# CalculateCreditsEmail
# ─────────────────────────────────────────────────────────────────────────────

class TestCalculateCreditsEmail:
    async def test_credits_equal_cost(self):
        df = pl.DataFrame({Cols.cost: [0.05, 0.10, 0.02]})
        result = await CalculateCreditsEmail().execute(df, make_ctx())
        assert result[Cols.credits].to_list() == pytest.approx([0.05, 0.10, 0.02])

    async def test_rounded_to_three_decimals(self):
        df = pl.DataFrame({Cols.cost: [0.123456]})
        result = await CalculateCreditsEmail().execute(df, make_ctx())
        assert result[Cols.credits][0] == pytest.approx(0.123, abs=1e-3)


# ─────────────────────────────────────────────────────────────────────────────
# CustomSubject — error codes
# ─────────────────────────────────────────────────────────────────────────────

class TestCustomSubject:
    async def test_missing_subject_raises_with_code(self):
        from modules.process.app.pipelines.email.custom_subject import CustomSubject
        df = pl.DataFrame({Cols.email: ["a@b.com"]})
        ctx = make_ctx(subject=None)
        with pytest.raises(ValueError, match="SUBJECT_REQUIRED"):
            await CustomSubject().execute(df, ctx)

    async def test_subject_present_adds_column(self):
        from modules.process.app.pipelines.email.custom_subject import CustomSubject
        df = pl.DataFrame({Cols.email: ["a@b.com"]})
        ctx = make_ctx(subject="Hola")
        result = await CustomSubject().execute(df, ctx)
        assert Cols.subject in (result.collect_schema().names() if isinstance(result, pl.LazyFrame) else result.columns)


# ─────────────────────────────────────────────────────────────────────────────
# CleanDataEmail — error codes
# ─────────────────────────────────────────────────────────────────────────────

class TestCleanDataEmailErrors:
    async def test_missing_column_raises_with_code(self):
        df = pl.DataFrame({"otro_campo": ["a@b.com"]})
        ctx = make_ctx(demographic="email")
        with pytest.raises(ValueError, match="COLUMN_NOT_FOUND"):
            await CleanDataEmail(EmailNormalizer()).execute(df, ctx)

    async def test_missing_column_does_not_expose_available_columns(self):
        df = pl.DataFrame({"otro_campo": ["a@b.com"]})
        ctx = make_ctx(demographic="email")
        with pytest.raises(ValueError) as exc_info:
            await CleanDataEmail(EmailNormalizer()).execute(df, ctx)
        assert "otro_campo" not in str(exc_info.value)


