import logging

import drf
import drftypes as dt
import psdsf


def drf_main() -> None:
  capacity = dt.ResourceVec([20, 30])
  users = [
      drf.User(dt.UserID("u0"), dt.ResourceVec([2.0, 1.0]), 0),
      drf.User(dt.UserID("u1"), dt.ResourceVec([1.0, 2.0]), 0),
  ]

  s = drf.Scheduler(capacity, users)
  for i in range(10):
    if not s.schedule():
      break
  for u in users:
    print(
        f"User {u.user_id} share {u.dominant_share(capacity)} used"
        f" {u.allocated()}"
    )
  print(f"Total unused: {s.unused()}")


def psdsf_main() -> None:
  servers = [
      psdsf.Server(dt.ServerID("s0"), capacity=dt.ResourceVec([20, 30])),
      psdsf.Server(dt.ServerID("s1"), capacity=dt.ResourceVec([30, 20])),
  ]
  users = [
      psdsf.User(dt.UserID("u0"), dt.ResourceVec([2.0, 1.0]), 0),
      psdsf.User(dt.UserID("u1"), dt.ResourceVec([1.0, 2.0]), 0),
  ]
  s = psdsf.Scheduler(servers, users)
  s.schedule()


def main() -> None:
  logging.basicConfig(
      level=logging.INFO,
      format=(
          "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
      ),
  )

  psdsf_main()
  # drf_main()


main()
