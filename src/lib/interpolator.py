
from pydantic.dataclasses import dataclass
from dataclasses import InitVar
from scipy import interpolate


@dataclass
class Interpolator3D:
    _xyz_init: InitVar[list[tuple[float, float, float]]]

    def __post_init__(self, xyz_init):
        self._xs, self._ys, self._zs = list(map(list, zip(*xyz_init)))
    
    @property
    def size(self):
        return len(self._xs)
    
    @classmethod
    def default(cls):
        return Spline3D
    
    @classmethod
    def fromString(cls, type: str):
        if type in ('Default', 'Spline'):
            return Spline3D
        else:
            raise NotImplementedError(f"{type} not supported yet")

    def get_value(self, _: float, __: float):
        raise RuntimeError("Abstract function: get_value")

# https://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.bisplrep.html#scipy.interpolate.bisplrep
BUFFER_EXTRAP = 0.1
@dataclass
class Spline3D(Interpolator3D):

    def __post_init__(self, xyz_init):
        super().__post_init__(xyz_init)
        yb, ye = min(self._ys), max(self._ys)
        yb, ye = yb - abs(yb) * BUFFER_EXTRAP, ye + abs(ye) * BUFFER_EXTRAP
        self.bispline_tck = interpolate.bisplrep(self._xs, self._ys, self._zs, yb=yb, ye=ye, kx=1, ky=1)

    def get_value(self, x: float, y: float) -> float:
        return interpolate.bisplev(x, y, self.bispline_tck)
