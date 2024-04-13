# Base DRF implementation.
#
# Dominant Resource Fairness: Fair Allocation of Multiple Resource Types
# Ali Ghodsi, Matei Zaharia, Benjamin Hindman, Andy Konwinski, Scott Shenker, Ion Stoica
#
# NSDI 2011

import logging

import drftypes as dt
from typing import Optional


@dataclass
class User:
  user_id: dt.UserID
  per_task_req: ResourceVec
  n_tasks: int

  def allocated(self) -> ResourceVec:
    """Amount of resources allocated to this user"""
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

class Scheduler:

  def __init__(self, capacity: dt.ResourceVec, users: list[User]) -> None:
    self._capacity = capacity
    self._usage = dt.ResourceVec([0, 0])
    self._users = users

  def usage(self) -> dt.ResourceVec:
    return self._usage

  def unused(self) -> dt.ResourceVec:
    return self._capacity.sub(self._usage)

  def schedule(self) -> bool:
    user: Optional[User] = None
    min_dominant_share = 99999.0
    for u in self._users:
      resource_id, share = u.dominant_share(self._capacity)
      if share < min_dominant_share:
        min_dominant_share = share
        user = u
    assert user
    demand = user.per_task_req
    want_usage = self._usage.add(demand)
    if not want_usage.le(self._capacity):
      return False

    self._usage = want_usage
    user.n_tasks += 1
    logging.info(f"Picked user {user} usage {self._usage}")
    return True
