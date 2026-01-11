from pydantic import BaseModel
from datetime import date, datetime

class MachineIn(BaseModel):
    machine_id: str
    name: str
    family: str | None = None
    capacity_per_shift: float | None = None
    setup_time: float | None = None

class DemandIn(BaseModel):
    order_id: str
    item_id: str
    due_date: date
    qty: float
    priority: int = 0
    customer: str | None = None

class ScheduleOpOut(BaseModel):
    op_id: int
    order_id: str
    item_id: str
    article_name: str | None = None
    machine_id: str
    start_ts: datetime
    end_ts: datetime
    lateness_min: float
