import logging

import drf
import drftypes as dt


def main() -> None:
  logging.basicConfig(level=logging.INFO)
  capacity = [20, 30]
  users = [dt.User("u0", [2, 1], 0), dt.User("u1", [1, 2], 0)]

  s = drf.Scheduler(capacity, users)
  for i in range(10):
    if not s.schedule():
      break
  for u in users:
    print(
        f"User {u.user_id} share {u.dominant_share(capacity)} used"
        f" {u.used_resources()}"
    )
  print(f"Total unused: {s.unused()}")


main()
