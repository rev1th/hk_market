from pydantic.dataclasses import dataclass
import datetime as dtm

from common.models.base_instrument import BaseInstrument
from common.models.future import Future
from common.chrono.daycount import DayCount


@dataclass
class EquityIndex(BaseInstrument):
    pass

@dataclass
class EquityIndexFuture(Future):
    _underlying: EquityIndex
    _daycount: DayCount = DayCount.ACT365

    def __post_init__(self):
        if self.name is None:
            self.name = f"{self._underlying.name}_{self._expiry}"
    
    def get_expiry_dcf(self, date: dtm.date) -> float:
        return self._daycount.get_dcf(date, self._expiry)
