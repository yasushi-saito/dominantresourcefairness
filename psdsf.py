# Per-Server Dominant-Share Fairness (PS-DSF): A Multi-Resource Fair Allocation
# Mechanism for Heterogeneous Servers
#
# Jalal Khamse-Ashari, Ioannis Lambadaris, George Kesidis, Bhuvan Urgaonkar, Yiqiang Zhao

# https://github.com/apache/mesos/compare/master...yuquanshan:mesos:multi-scheduler


from dataclasses import dataclass
import logging
from typing import Iterable, Optional

import drftypes as dt

# K: # of servers = len(_servers)
# N: # of users = len(_users)

# d_{n,r}: demand vector for user n and resource r = u.req[r]
# a_{n,r}: quantity allocated to user n for resource r
# c_{i,r}: capacity of server i for resource r
# x_{n,i} (∈R): number of tasks allocated for user and the server
# x_n : number of tasks of user n = User.total_n_tasks()
# c_r: fleet-wide capacity for resource r
#   c_r = Sigma_{i}(c_{i,r})


# s_n: global dominant share of user n.
#   s_n = x_n * max_{r}(d_{n,r} / c_r)


class User:

  def __init__(self, id: dt.UserID, req: dt.ResourceVec, weight=1.0) -> None:
    self.id = id
    self.req = req  # per-task resource demands
    self.weight = weight
    # x_{n,i} (∈R): number of tasks allocated for user and the server
    self._n_allocated = dict["Server", float]()

  def __hash__(self) -> int:
    return hash(self.id)

  def __repr__(self) -> str:
    def n_allocated_str(self):
      return {s.id: v for s, v in self._n_allocated.items()}

    return (
        f"User(id={self.id} req={self.req} n_allocated={n_allocated_str(self)})"
    )

  def n_allocated(self, s: "Server") -> float:
    return self._n_allocated.get(s, 0.0)

  def set_n_allocated(self, s: "Server", v: float) -> None:
    assert v > 0
    self._n_allocated[s] = v
    s.allocated.add(self)
    logging.info(f"ADD: u={self} s={s}")

  def add_n_allocated(self, s: "Server", v: float) -> None:
    assert v > 0
    if s not in self._n_allocated:
      self._n_allocated[s] = 0
      s.allocated.add(self)
    self._n_allocated[s] += v
    logging.info(f"ADD: u={self} s={s}")

  def total_n_tasks(self) -> float:
    total = 0.0
    for n_tasks in self._n_allocated.values():
      assert n_tasks > 0
      total += n_tasks
    return total

  def total_usage(self) -> dt.ResourceVec:
    total = dt.ResourceVec.zeros()
    for n_tasks in self._n_allocated.values():
      assert n_tasks > 0
      total = total + self.req * n_tasks
    return total

  def per_server_usage(self, s: "Server") -> dt.ResourceVec:
    if s not in self._n_allocated:
      return dt.ResourceVec.zeros()
    return self.req * self._n_allocated[s]


class Server:

  def __init__(self, id: dt.UserID, capacity: dt.ResourceVec) -> None:
    self.id = id
    self.capacity = capacity
    self.allocated = set[User]()

  def __hash__(self) -> int:
    return hash(self.id)

  def __repr__(self) -> str:
    def allocated_str(self):
      return {s.id for s in self.allocated}

    return (
        f"Server(id={self.id} cap={self.capacity} allocated={allocated_str(self)})"
    )

  def total_usage(self) -> dt.ResourceVec:
    total = dt.ResourceVec.zeros()
    for u in self.allocated:
      assert self in u._n_allocated, f"u={u} s={self.id} alloc={self.allocated}"
      total = total + u.req * u.n_allocated(self)
    return total

  def free(self) -> dt.ResourceVec:
    """Amount of unallocated resources on this server."""
    return self.capacity - self.total_usage()


# γ_{n,i} : # of tasks that can be executed by user n when monopolizing server i.
def _n_tasks_assuming_idle_server(u: User, s: Server) -> float:
  def _n(r: dt.ResourceID) -> float:
    if u.req[r] <= 0:
      return dt.INFINITY

    return s.capacity[r] / u.req[r]

  min_n = dt.min_in_list(dt.ALL_RESOURCE_TYPES, _n)
  assert min_n != dt.INFINITY
  assert min_n != 0
  return min_n


# s_{n,i}: virtual dominant share, eq (8)
def _vds(u: User, s: Server) -> float:
  return float(u.total_n_tasks()) / float(_n_tasks_assuming_idle_server(u, s))


def _xxx(
    s: Server,
    schedulable_users: set[User],
    saturated_resources: set[dt.ResourceID],
) -> set[User]:
  max_vds = 0.0
  max_u = set[User]()

  for u in schedulable_users:
    for r in saturated_resources:
      if u.per_server_usage(s)[r] * u.req[r] > 0:
        vds = _vds(u, s) / u.weight
        if vds == max_vds:
          max_u.add(u)
        elif vds > max_vds:
          max_vds = vds
          max_u = {u}
  return max_u


def _saturated_resources(
    s: Server, min_users: Iterable[User]
) -> set[dt.ResourceID]:
  saturated_resources = set[dt.ResourceID]()
  server_free = s.free()
  for u in min_users:
    user_usage = u.per_server_usage(s)
    for r in dt.ALL_RESOURCE_TYPES:
      if u.req[r] > 0 and server_free[r] <= dt.INFINITISIMAL:
        saturated_resources.add(r)
  logging.info(f"saturated {saturated_resources}")
  return saturated_resources


class Scheduler:

  def __init__(self, servers: list[Server], users: list[User]) -> None:
    self._servers = servers
    self._usage = dt.ResourceVec([0, 0])
    self._users = users

  # # S^*_i: min VDS across all users. eq (16)
  # def _min_vds(self, s: Server) -> float:
  #   return dt.min_in_list(self._users, lambda u: _vds(u, s) / u.weight)

  # def _min_vdsXX(self, u: dt.User) -> float:
  #   v = 999999999
  #   for s in self._servers:
  #     v = min(v, self._vds(u, s))
  #   return v

  # N_i: set of users for which γ_{n,i} > 0.
  def _schedulable_users(self, s: Server) -> set[User]:
    return {
        u
        for u in self._users
        if (
            self._user_can_run_on_server(u, s)
            and _n_tasks_assuming_idle_server(u, s) > 0
        )
    }

  # δn,i: 1 if user n can run on server i.
  def _user_can_run_on_server(self, u: User, s: Server) -> bool:
    return True

  def _update_allocation(
      self,
      s: Server,
      min_vds,
      schedulable_users: set[User],
      saturated_resources: set[dt.ResourceID],
  ) -> None:
    logging.info(
        "updateallocated:"
        f" {s.id} min_vds={min_vds} schedulable={schedulable_users} saturated={saturated_resources}"
    )
    # Identify f_i
    free: dt.ResourceVec = s.free()

    def _nr(r: dt.ResourceID) -> User:
      return dt.argmax(
          self._users,
          lambda u: _vds(u, s) if u.n_allocated(s) * u.req[r] > 0 else -1,
      )

    def pick_beta(min_vds: float, z_star: float) -> float:
      def _beta(beta: float) -> float:
        lhs = min_vds + beta * z_star
        for r in saturated_resources:
          nr = _nr(r)
          rhs = (nr.total_n_tasks() - beta * nr.n_allocated(s)) / (
              nr.weight * _n_tasks_assuming_idle_server(nr, s)
          )

          if lhs > rhs:
            return False
        return True

      for beta10 in range(1, 10):
        beta = beta10 / 10.0
        if _beta(beta):
          return beta
      assert False

    for r in saturated_resources:
      nr = _nr(r)
      free = free + nr.req * nr.n_allocated(s)

    d_i: dt.ResourceVec = dt.sum(
        schedulable_users,
        lambda u: u.req * u.weight * _n_tasks_assuming_idle_server(u, s),
        zero=dt.ResourceVec.zeros(),
    )
    z_star: float = dt.min_in_list(
        dt.ALL_RESOURCE_TYPES, lambda r: free[r] / d_i[r]
    )

    beta = pick_beta(min_vds, z_star)

    for u in schedulable_users:
      u.add_n_allocated(
          s, beta * u.weight * _n_tasks_assuming_idle_server(u, s) * z_star
      )

    for r in saturated_resources:
      nr = _nr(r)
      nr.set_n_allocated(s, (1.0 - beta) * nr.n_allocated(s))

  def schedule(self) -> bool:
    updated = False
    for s in self._servers:
      # N_i (schedulable_users)
      schedulable_users = self._schedulable_users(s)
      while schedulable_users:
        logging.info(f"scheulable_users={schedulable_users}")
        # Compute S^*_i (min_vds) and N^*_i (min_user)
        min_vds = dt.INFINITY
        min_users = set[User]()
        for u in schedulable_users:
          v = _vds(u, s)
          if v == min_vds:
            min_users.add(u)
          elif v < min_vds:
            min_vds = v
            min_users = {u}
        assert min_users

        # Compute R^*_i (saturated_resources)
        saturated_resources = _saturated_resources(s, min_users)
        #     set[dt.ResourceID]()
        # for u in min_users:
        #   user_usage = u.per_server_usage(s)
        #   server_free = s.free()
        #   for r in dt.ALL_RESOURCE_TYPES:
        #     if u.req[r] > 0 and server_free[r] <= dt.INFINITISIMAL:
        #       saturated_resources.add(r)
        # logging.info(f"saturated={saturated_resources}")

        xxx = _xxx(s, schedulable_users, saturated_resources)
        print(xxx)
        if schedulable_users == xxx:
          for u in schedulable_users:
            for r in saturated_resources:
              if u.req[r] > 0:
                schedulable_users.remove(u)
                break
        else:
          updated = True
          self._update_allocation(
              s, min_vds, schedulable_users, saturated_resources
          )
        break

    return updated
