from dataclasses import dataclass
import logging
from typing import Callable, Iterable, NewType, Optional, Protocol, TypeVar

ResourceID = NewType("ResourceID", int)

CPU = ResourceID(0)
RAM = ResourceID(1)
INVALID_RESOURCE_ID = ResourceID(-1)

ALL_RESOURCE_TYPES = [CPU, RAM]

ServerID = NewType("ServerID", str)
UserID = NewType("UserID", str)


class ResourceVec:

  def __init__(self, values: list[float]) -> None:
    self._values = values

  def __str__(self) -> str:
    return f"{self._values}"

  def __repr__(self) -> str:
    return f"{self._values}"

  def __eq__(self, a: object) -> bool:
    assert type(a) == ResourceVec
    assert len(self._values) == len(a._values)
    for i in range(len(self._values)):
      if self._values[i] != a._values[i]:
        return False
    return True

  def __ne__(self, a: object) -> bool:
    assert type(a) == ResourceVec
    return not (self == a)

  def __getitem__(self, r: ResourceID) -> float:
    return self._values[r]

  def __add__(self, a1: "ResourceVec") -> "ResourceVec":
    assert len(self._values) == len(a1._values)
    result = []
    for i in range(len(self._values)):
      result.append(self._values[i] + a1._values[i])
    return ResourceVec(result)

  def __sub__(self, a1: "ResourceVec") -> "ResourceVec":
    assert len(self._values) == len(a1._values)
    result = []
    for i in range(len(self._values)):
      result.append(self._values[i] - a1._values[i])
    return ResourceVec(result)

  def __mul__(self, v: float) -> "ResourceVec":
    result = []
    for i in range(len(self._values)):
      result.append(self._values[i] * v)
    return ResourceVec(result)

  def n_dims(self) -> int:
    return len(self._values)

  def values(self) -> list[float]:
    return self._values

  def get(self, id: ResourceID) -> float:
    return self._values[id]

  # def add(self, a1: "ResourceVec") -> "ResourceVec":
  #   return self + a1

  # def sub(self, a1: "ResourceVec") -> "ResourceVec":
  #   return self - a1

  # def mul(self, v: float) -> "ResourceVec":
  #   return self * v

  def le(self, a1: "ResourceVec") -> bool:
    """Check if every element is no greather than the counterpart in "a1".

    Note that we don't use an operator overload for "<=" because this method
    doesn't
    satisfy the "<=" for typical arithmetic types.
    """

    assert len(self._values) == len(a1._values)

    for i in range(len(self._values)):
      if self._values[i] > a1._values[i]:
        return False
    return True

  @staticmethod
  def zeros() -> "ResourceVec":
    """Create an ResourceVec where all elements are 0.0"""
    return ResourceVec([0.0 for r in ALL_RESOURCE_TYPES])


_T = TypeVar("_T")
_V = TypeVar("_V")

INFINITY = 9999999999.0
INFINITISIMAL = 0.01


def max_in_list(ll: Iterable[_T], fn: Callable[[_T], float]) -> float:
  max_value = -INFINITY
  for l in ll:
    if (v := fn(l)) > max_value:
      max_value = v
  return max_value


def min_in_list(ll: Iterable[_T], fn: Callable[[_T], float]) -> float:
  min_value = INFINITY
  for l in ll:
    if (v := fn(l)) < min_value:
      min_value = v
  return min_value


def argmax(ll: Iterable[_T], fn: Callable[[_T], float]) -> _T:
  max_value = -INFINITY
  max_key = None
  for l in ll:
    if (v := fn(l)) > max_value:
      max_value = v
      max_key = l
  assert max_key
  return max_key


# Sigma operator
def sum(ll: Iterable[_T], fn: Callable[[_T], _V], zero: _V) -> _V:
  v = zero
  for l in ll:
    v = v + fn(l)  # type: ignore
  return v
