from dataclasses import dataclass
import logging
from typing import NewType, Optional

ResourceID = NewType("ResourceID", int)
ResourceVec = list[float]

CPU = ResourceID(0)
RAM = ResourceID(1)

def resource_vec_add(a0: ResourceVec, a1: ResourceVec) -> ResourceVec:
  assert len(a0) == len(a1)
  result: ResourceVec = []
  for i in range(len(a0)):
    result.append(a0[i] + a1[i])
  return result


def resource_vec_sub(a0: ResourceVec, a1: ResourceVec) -> ResourceVec:
  assert len(a0) == len(a1)
  result: ResourceVec = []
  for i in range(len(a0)):
    result.append(a0[i] - a1[i])
  return result


def resource_vec_le(a0: ResourceVec, a1: ResourceVec) -> bool:
  assert len(a0) == len(a1)
  for i in range(len(a0)):
    if a0[i] > a1[i]:
      return False
  return True


@dataclass
class User:
  user_id: str
  per_task_req: ResourceVec
  n_tasks: int

  def used_resources(self) -> ResourceVec:
    return [x * self.n_tasks for x in self.per_task_req]

  def dominant_share(self, capacity: ResourceVec) -> tuple[ResourceID, float]:
    assert len(capacity) == len(self.per_task_req)
    used = self.used_resources()
    max_share = 0.0
    dominant_resource_id = ResourceID(-1)
    for resource_id, cap in enumerate(capacity):
      share = used[resource_id] / cap
      if share > max_share:
        max_share = share
        dominant_resource_id = ResourceID(resource_id)
    return dominant_resource_id, max_share
