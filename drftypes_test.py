import unittest
import drftypes as dt


class TestDRFTypes(unittest.TestCase):

  def test_sum(self):
    self.assertEqual(dt.sum([1, 10], lambda v: v * 10, 5), 115)

  def test_max_in_list(self):
    self.assertEqual(dt.max_in_list([1, 10], lambda v: v), 10)

  def test_min_in_list(self):
    self.assertEqual(dt.min_in_list([1, 10], lambda v: v), 1)

  def test_argmax(self):
    self.assertEqual(dt.argmax([1, 10], lambda v: -(v * 10)), 1)
    self.assertEqual(dt.argmax([1, 10], lambda v: v * 10), 10)

  def test_resourcevec_equal(self):
    self.assertEqual(dt.ResourceVec([1, 2]), dt.ResourceVec([1, 2]))
    self.assertFalse(dt.ResourceVec([1, 2]) == dt.ResourceVec([2, 1]))

  def test_resourcevec_zeros(self):
    self.assertEqual(dt.ResourceVec.zeros(), dt.ResourceVec([0, 0]))


if __name__ == '__main__':
  unittest.main()
