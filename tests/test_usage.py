import datetime

from ismcore.model.base_model_usage_and_limits import (
    Usage,
    UnitType,
    UnitSubType,
    UserProjectCurrentUsageReport,
)


def test_usage_entity():
    usage = Usage(
        resource_id="12345",
        resource_type="/provider/selector/id",

        #
        transaction_time=datetime.datetime.utcnow(),

        #
        unit_type=UnitType.TOKEN,
        unit_subtype=UnitSubType.INPUT,
        unit_count=123,
    )

    json_data = usage.model_dump_json(indent=2)
    print(json_data)


def test_user_project_current_usage_report_creation():
    """Test creating a UserProjectCurrentUsageReport instance."""
    report = UserProjectCurrentUsageReport(
        user_id="user123",
        project_id="project456",
        pct_minute_tokens_used=50.0,
        pct_hour_tokens_used=60.0,
        pct_day_tokens_used=70.0,
        pct_month_tokens_used=80.0,
        pct_year_tokens_used=85.0,
        pct_minute_cost_used=45.0,
        pct_hour_cost_used=55.0,
        pct_day_cost_used=65.0,
        pct_month_cost_used=75.0,
        pct_year_cost_used=80.0,
    )

    assert report.user_id == "user123"
    assert report.project_id == "project456"
    assert report.pct_minute_tokens_used == 50.0
    assert report.pct_hour_tokens_used == 60.0
    assert report.pct_day_tokens_used == 70.0
    assert report.pct_month_tokens_used == 80.0
    assert report.pct_year_tokens_used == 85.0
    assert report.pct_minute_cost_used == 45.0
    assert report.pct_hour_cost_used == 55.0
    assert report.pct_day_cost_used == 65.0
    assert report.pct_month_cost_used == 75.0
    assert report.pct_year_cost_used == 80.0


def test_user_project_current_usage_report_minimal():
    """Test creating a minimal UserProjectCurrentUsageReport with only required fields."""
    report = UserProjectCurrentUsageReport(user_id="user123")

    assert report.user_id == "user123"
    assert report.project_id is None
    assert report.pct_minute_tokens_used is None
    assert report.pct_hour_tokens_used is None
    assert report.pct_day_tokens_used is None
    assert report.pct_month_tokens_used is None
    assert report.pct_year_tokens_used is None
    assert report.pct_minute_cost_used is None
    assert report.pct_hour_cost_used is None
    assert report.pct_day_cost_used is None
    assert report.pct_month_cost_used is None
    assert report.pct_year_cost_used is None


def test_is_allowed_ok_all_below_warn():
    """Test is_allowed returns 'ok' when all percentages are below warning threshold."""
    report = UserProjectCurrentUsageReport(
        user_id="user123",
        pct_minute_tokens_used=50.0,
        pct_hour_tokens_used=60.0,
        pct_day_tokens_used=70.0,
        pct_month_tokens_used=80.0,
        pct_minute_cost_used=45.0,
        pct_hour_cost_used=55.0,
    )

    decision, message = report.is_allowed(warn_pct=90.0, block_pct=100.0)
    assert decision == "ok"
    assert message == "within allowed limits"


def test_is_allowed_warn_at_threshold():
    """Test is_allowed returns 'warn' when usage is at warning threshold."""
    report = UserProjectCurrentUsageReport(
        user_id="user123",
        pct_minute_tokens_used=90.0,
    )

    decision, message = report.is_allowed(warn_pct=90.0, block_pct=100.0)
    assert decision == "warn"
    assert "minute token nearing cap (90.00%)" in message


def test_is_allowed_warn_above_threshold():
    """Test is_allowed returns 'warn' when usage is above warning but below block threshold."""
    report = UserProjectCurrentUsageReport(
        user_id="user123",
        pct_day_cost_used=95.0,
        pct_hour_tokens_used=92.0,
    )

    decision, message = report.is_allowed(warn_pct=90.0, block_pct=100.0)
    assert decision == "warn"
    assert "hour token nearing cap (92.00%)" in message
    assert "day cost nearing cap (95.00%)" in message


def test_is_allowed_block_at_threshold():
    """Test is_allowed returns 'block' when usage is at block threshold."""
    report = UserProjectCurrentUsageReport(
        user_id="user123",
        pct_month_tokens_used=100.0,
    )

    decision, message = report.is_allowed(warn_pct=90.0, block_pct=100.0)
    assert decision == "block"
    assert "month token cap exceeded (100.00%)" in message


def test_is_allowed_block_above_threshold():
    """Test is_allowed returns 'block' when usage exceeds block threshold."""
    report = UserProjectCurrentUsageReport(
        user_id="user123",
        pct_year_cost_used=105.0,
    )

    decision, message = report.is_allowed(warn_pct=90.0, block_pct=100.0)
    assert decision == "block"
    assert "year cost cap exceeded (105.00%)" in message


def test_is_allowed_block_takes_precedence():
    """Test is_allowed returns 'block' even if some metrics are only at warn level."""
    report = UserProjectCurrentUsageReport(
        user_id="user123",
        pct_minute_tokens_used=92.0,  # warn level
        pct_hour_tokens_used=110.0,   # block level
    )

    decision, message = report.is_allowed(warn_pct=90.0, block_pct=100.0)
    assert decision == "block"
    assert "minute token nearing cap (92.00%)" in message
    assert "hour token cap exceeded (110.00%)" in message


def test_is_allowed_multiple_blocks():
    """Test is_allowed handles multiple exceeded caps."""
    report = UserProjectCurrentUsageReport(
        user_id="user123",
        pct_day_tokens_used=105.0,
        pct_day_cost_used=103.0,
    )

    decision, message = report.is_allowed(warn_pct=90.0, block_pct=100.0)
    assert decision == "block"
    assert "day token cap exceeded (105.00%)" in message
    assert "day cost cap exceeded (103.00%)" in message


def test_is_allowed_ignores_none_values():
    """Test is_allowed ignores None values (no cap set)."""
    report = UserProjectCurrentUsageReport(
        user_id="user123",
        pct_minute_tokens_used=None,
        pct_hour_tokens_used=50.0,
        pct_day_tokens_used=None,
    )

    decision, message = report.is_allowed(warn_pct=90.0, block_pct=100.0)
    assert decision == "ok"
    assert message == "within allowed limits"


def test_is_allowed_all_none():
    """Test is_allowed when all caps are None."""
    report = UserProjectCurrentUsageReport(user_id="user123")

    decision, message = report.is_allowed(warn_pct=90.0, block_pct=100.0)
    assert decision == "ok"
    assert message == "within allowed limits"


def test_is_allowed_custom_thresholds():
    """Test is_allowed with custom warning and block thresholds."""
    report = UserProjectCurrentUsageReport(
        user_id="user123",
        pct_minute_tokens_used=75.0,
    )

    # With lower thresholds, should trigger warn
    decision, message = report.is_allowed(warn_pct=70.0, block_pct=90.0)
    assert decision == "warn"
    assert "minute token nearing cap (75.00%)" in message

    # Same report with default thresholds should be ok
    decision, message = report.is_allowed(warn_pct=90.0, block_pct=100.0)
    assert decision == "ok"


def test_is_allowed_edge_case_just_below_warn():
    """Test is_allowed when just below warning threshold."""
    report = UserProjectCurrentUsageReport(
        user_id="user123",
        pct_hour_cost_used=89.99,
    )

    decision, message = report.is_allowed(warn_pct=90.0, block_pct=100.0)
    assert decision == "ok"
    assert message == "within allowed limits"


def test_is_allowed_edge_case_just_below_block():
    """Test is_allowed when just below block threshold but above warn."""
    report = UserProjectCurrentUsageReport(
        user_id="user123",
        pct_month_cost_used=99.99,
    )

    decision, message = report.is_allowed(warn_pct=90.0, block_pct=100.0)
    assert decision == "warn"
    assert "month cost nearing cap (99.99%)" in message
