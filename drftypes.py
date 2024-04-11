from dataclasses import dataclass
import logging
from typing import NewType, Optional

ResourceID = NewType("ResourceID", int)

CPU = ResourceID(0)
RAM = ResourceID(1)

class ResourceVec:
    def __init__(self, values: list[float]) -> None:
        self._values = values

    def __str__(self) -> str:
        return f"{self._values}"

    def n_dims(self) -> int:
        return len(self._values)

    def values(self) -> list[float]:
        return self._values

    def get(self, id: ResourceID) -> float:
        return self._values[id]

    def add(self, a1: "ResourceVec") -> "ResourceVec":
        assert len(self._values) == len(a1._values)
        result = []
        for i in range(len(self._values)):
            result.append(self._values[i] + a1._values[i])
        return ResourceVec(result)

    def sub(self, a1: "ResourceVec") -> "ResourceVec":
        assert len(self._values) == len(a1._values)
        result = []
        for i in range(len(self._values)):
            result.append(self._values[i] - a1._values[i])
        return ResourceVec(result)

    def le(self, a1: "ResourceVec") -> bool:
        assert len(self._values) == len(a1._values)

        for i in range(len(self._values)):
            if self._values[i] > a1._values[i]:
                return False
        return True


@dataclass
class User:
  user_id: str
  per_task_req: ResourceVec
  n_tasks: int

  def used_resources(self) -> ResourceVec:
    return ResourceVec([x * self.n_tasks for x in self.per_task_req.values()])

  def dominant_share(self, capacity: ResourceVec) -> tuple[ResourceID, float]:
    assert capacity.n_dims() == self.per_task_req.n_dims()
    used = self.used_resources()
    max_share = 0.0
    dominant_resource_id = ResourceID(-1)
    for resource_id, cap in enumerate(capacity.values()):
      share = used.values()[resource_id] / cap
      if share > max_share:
        max_share = share
        dominant_resource_id = ResourceID(resource_id)
    return dominant_resource_id, max_share
