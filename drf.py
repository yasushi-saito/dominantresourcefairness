# Base DRF implementation.
#
# Dominant Resource Fairness: Fair Allocation of Multiple Resource Types
# Ali Ghodsi, Matei Zaharia, Benjamin Hindman, Andy Konwinski, Scott Shenker, Ion Stoica
#
# NSDI 2011

import logging
from dataclasses import dataclass

import drftypes as dt
from typing import Optional


@dataclass
class User:
  user_id: dt.UserID
  # Per task demand
  req: dt.ResourceVec
  n_tasks: int = 0

  def allocated(self) -> dt.ResourceVec:
    """Amount of resources allocated to this user"""
    return dt.ResourceVec([x * self.n_tasks for x in self.req.values()])

  def dominant_share(self, capacity: dt.ResourceVec) -> tuple[dt.ResourceID, float]:
    assert capacity.n_dims() == self.req.n_dims()
    used = self.allocated()
    max_share = 0.0
    dominant_resource_id = dt.INVALID_RESOURCE_ID
    for resource_id, cap in enumerate(capacity.values()):
      share = used.values()[resource_id] / cap
      if share > max_share:
        max_share = share
        dominant_resource_id = dt.ResourceID(resource_id)
    return dominant_resource_id, max_share

class Scheduler:

  def __init__(self, capacity: dt.ResourceVec, users: list[User]) -> None:
    self._capacity = capacity
    self._usage = dt.ResourceVec([0, 0])
    self._users = users

  def usage(self) -> dt.ResourceVec:
    return self._usage

  def unused(self) -> dt.ResourceVec:
    return self._capacity - self._usage

  def schedule(self) -> bool:
    user: Optional[User] = None
    min_dominant_share = 99999.0
    for u in self._users:
      resource_id, share = u.dominant_share(self._capacity)
      if share < min_dominant_share:
        min_dominant_share = share
        user = u
    assert user
    demand = user.req
    want_usage = self._usage + demand
    if not want_usage.le(self._capacity):
      return False

    self._usage = want_usage
    user.n_tasks += 1
    logging.info(f"Picked user {user} usage {self._usage}")
    return True
