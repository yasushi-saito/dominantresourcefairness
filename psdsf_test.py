import unittest
import drftypes as dt
import psdsf


class TestPSDSF(unittest.TestCase):

  def test_user(self):
    u0 = psdsf.User(dt.UserID("u0"), dt.ResourceVec([1, 2]))
    u1 = psdsf.User(dt.UserID("u1"), dt.ResourceVec([1, 2]))
    self.assertFalse(u0 == u1)
    self.assertFalse(u0 is u1)
    self.assertTrue(u0 != u1)

    s0 = psdsf.Server(dt.ServerID("s0"), dt.ResourceVec([20, 30]))
    s1 = psdsf.Server(dt.ServerID("s0"), dt.ResourceVec([30, 20]))
    self.assertFalse(s0 == s1)
    self.assertFalse(s0 is s1)
    self.assertTrue(s0 != s1)

    u0.set_n_allocated(s0, 1.0)
    u0.set_n_allocated(s1, 2.0)
    self.assertEqual(u0._n_allocated, {s0: 1.0, s1: 2.0})
    self.assertEqual(s0.allocated, {u0})
    self.assertEqual(s1.allocated, {u0})
    print(f"U0={s1}")

    u1.set_n_allocated(s0, 2.0)
    print(f"U0={s1}")
    self.assertEqual(s0.allocated, {u0,u1})
    print(f"U0={s1}")
    self.assertEqual(s1.allocated, {u0})

  def test_n_tasks_assuming_idle_server(self):
    user_id = dt.UserID("u0")
    server = psdsf.Server(dt.ServerID("s0"), dt.ResourceVec([20, 30]))
    self.assertEqual(
        psdsf._n_tasks_assuming_idle_server(
            psdsf.User(user_id, dt.ResourceVec([1, 2])), server
        ),
        15.0,
    )
    self.assertEqual(
        psdsf._n_tasks_assuming_idle_server(
            psdsf.User(user_id, dt.ResourceVec([1, 3])), server
        ),
        10.0,
    )
    self.assertEqual(
        psdsf._n_tasks_assuming_idle_server(
            psdsf.User(user_id, dt.ResourceVec([1, 1])), server
        ),
        20.0,
    )


if __name__ == "__main__":
  unittest.main()
