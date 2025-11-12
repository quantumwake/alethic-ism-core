import datetime as dt
from enum import Enum
from typing import Optional, List, Dict, Literal, Tuple
from pydantic import BaseModel, Field

class UnitType(Enum):
    TOKEN = "TOKEN"
    COMPUTE = "COMPUTE"
    STORAGE = "STORAGE"

class UnitSubType(Enum):
    INPUT = "INPUT"
    OUTPUT = "OUTPUT"

class Usage(BaseModel):
    id: Optional[int] = None
    transaction_time: Optional[dt.datetime] = dt.datetime.now()

    project_id: Optional[str] = None

    resource_id: str
    resource_type: str

    unit_type: UnitType
    unit_subtype: UnitSubType
    unit_count: int

    metadata: Optional[dict] = None

    class Config:
        from_attributes = True


class UsageReport(BaseModel):
    user_id: str # the user id for whom the report is generated
    resource_id: Optional[str] = None # added resource_id for more granular reporting (which resource accumulated the usage)
    resource_type: Optional[str] = None # added resource_type for more granular reporting (which resource type "provider" accumulated the usage)
    project_id: Optional[str] = None # added project_id for more granular reporting
    minute: Optional[int] = None # added minute for more granular reporting (might be None for hourly reports)
    hour: Optional[int] = None # added hour for more granular reporting (might be None for daily reports)
    day: Optional[int] = None # added day for more granular reporting (might be None for monthly reports)
    month: Optional[int] = None # added month for more granular reporting (might be None for yearly reports)
    year: Optional[int] = None # added year for more granular reporting (should always be present, even for daily reports)
    unit_type: Optional[str] = None # the unit type, e.g., TOKEN, might be something different in the future
    input_cost_divisor : Optional[float] = 1000.0 # the cost divisor used per N tokens for input cost calculations
    input_price: Optional[float] = 0.0 # price per N input tokens as per input_cost_divisor
    input_tokens: Optional[int] = 0 # total input tokens used
    input_cost: Optional[float] = 0.0 # total input cost calculated
    input_count: Optional[int] = 0 # total input events counted
    output_cost_divisor : Optional[float] = 1000.0  # the cost divisor used per N tokens for output cost calculations
    output_price: Optional[float] = 0.0 # price per N output tokens as per output_cost_divisor
    output_tokens: Optional[int] = 0 # total output tokens used
    output_cost: Optional[float] = 0.0 # total output cost calculated
    output_count: Optional[int] = 0 # total output events counted
    total_tokens: Optional[int] = 0 # total tokens used (input_tokens + output_tokens)
    total_cost: Optional[float] = 0.0 # total cost calculated (input_cost + output_cost)


Decision = Literal["ok", "warn", "block"]

class UserProjectCurrentUsageReport(BaseModel):
    """Represents the current usage % per user/project vs. tier limits."""
    user_id: str
    project_id: Optional[str] = None

    ### tier / quota details
    limit_token_per_minute:   Optional[int] = None
    limit_token_per_hour:     Optional[int] = None
    limit_token_per_day:      Optional[int] = None
    limit_token_per_month:    Optional[int] = None
    limit_token_per_year:     Optional[int] = None

    limit_cost_per_minute:    Optional[float] = None
    limit_cost_per_hour:      Optional[float] = None
    limit_cost_per_day:       Optional[float] = None
    limit_cost_per_month:     Optional[float] = None
    limit_cost_per_year:      Optional[float] = None

    ### calculated cost for given period (indicates the current period, as in current minute, current hour, current day, ...)
    cur_minute_total_cost:    Optional[float] = Field(None)
    cur_hour_total_cost:      Optional[float] = Field(None)
    cur_day_total_cost:       Optional[float] = Field(None)
    cur_month_total_cost:     Optional[float] = Field(None)
    cur_year_total_cost:      Optional[float] = Field(None)

    # token % used
    pct_minute_tokens_used: Optional[float] = Field(None, description="0..100, NULL if no minute cap")
    pct_hour_tokens_used:   Optional[float] = Field(None, description="0..100, NULL if no hour cap")
    pct_day_tokens_used:    Optional[float] = Field(None, description="0..100, NULL if no day cap")
    pct_month_tokens_used:  Optional[float] = Field(None, description="0..100, NULL if no month cap")
    pct_year_tokens_used:   Optional[float] = Field(None, description="0..100, NULL if no year cap")

    # cost % used
    pct_minute_cost_used: Optional[float] = Field(None, description="0..100, NULL if no minute cost cap")
    pct_hour_cost_used:   Optional[float] = Field(None, description="0..100, NULL if no hour cost cap")
    pct_day_cost_used:    Optional[float] = Field(None, description="0..100, NULL if no day cost cap")
    pct_month_cost_used:  Optional[float] = Field(None, description="0..100, NULL if no month cost cap")
    pct_year_cost_used:   Optional[float] = Field(None, description="0..100, NULL if no year cost cap")

    def is_allowed(
            self,
            warn_pct: float = 90.0,
            block_pct: float = 100.0,
    ) -> Tuple[Decision, str]:
        """
        Short-circuit evaluation:
          - Return immediately on the first cap EXCEEDED (block).
          - Else return the first WARN encountered.
          - Else OK.
        Priority order: minute → hour → day → month → year, tokens first then cost.
        """
        checks: list[tuple[str, Optional[float]]] = [
            ("minute token", self.pct_minute_tokens_used),
            ("hour token", self.pct_hour_tokens_used),
            ("day token", self.pct_day_tokens_used),
            ("month token", self.pct_month_tokens_used),
            ("year token", self.pct_year_tokens_used),
            ("minute cost", self.pct_minute_cost_used),
            ("hour cost", self.pct_hour_cost_used),
            ("day cost", self.pct_day_cost_used),
            ("month cost", self.pct_month_cost_used),
            ("year cost", self.pct_year_cost_used),
        ]

        first_warn: Optional[str] = None

        for label, value in checks:
            if value is None:
                continue
            if value >= block_pct:
                return "block", f"{label} cap exceeded ({value:.2f}%)"
            if first_warn is None and value >= warn_pct:
                first_warn = f"{label} nearing cap ({value:.2f}%)"

        if first_warn is not None:
            return "warn", first_warn

        return "ok", "within allowed limits"
