import unittest
from ..utils import diff, diff_apply


class DiffTestCase(unittest.TestCase):
    def test_trivial_incorrect(self):
        a = [5,6]
        b = [1,2,3]
        self.assertIsNone(diff(a,b))

    def test_diff_empty(self):
        a = [1,2,3]
        b = [1,2,3]
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff(a, b), ())

    def test_diff_intersection(self):
        a = [1,2,3]
        b = [1,2,4,5]
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff(a, b), ((4,3), (5, 4)))

    def test_diff_sparse_intersection(self):
        a = [1,5,7]
        b = [1,2,4,5]
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff(a, b), ((2, 1), (4, 2)))

    def test_diff_sparse_intersection2(self):
        a = [1,5,7]
        b = [5,7,2]
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff(a, b), ((2, 1),))

    def test_diff_no_intersection(self):
        a = [1,5,7]
        b = [8,9,10]
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff(a, b), ((8, 7), (9,8), (10,9)))

class DiffApplyTestCase(unittest.TestCase):
    def test_diff_apply_empty(self):
        a = [1,2,3]
        b = [1,2,3]
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff_apply(a, diff(a, b)), (1,2,3))

    def test_diff_apply_intersection(self):
        a = [1,2,3]
        b = [1,2,4,5]
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff_apply(a, diff(a, b)), (1,2,3,4,5))

    def test_diff_apply_sparse_intersection(self):
        a = [1,5,7]
        b = [1,2,4,5]
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff_apply(a, diff(a, b)), (1,2,4,5,7))

    def test_diff_apply_sparse_intersection2(self):
        a = [1,5,7]
        b = [5,7,2]
        self.assertEqual(diff_apply(a, diff(a, b)), (1,2,5,7))

    def test_diff_apply_no_intersection(self):
        a = [1,5,7]
        b = [8,9,10]
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff_apply(a, diff(a, b)), (1,5,7,8,9,10))


class DiffStringsTestCase(unittest.TestCase):
    def test_diff_empty(self):
        a = ['1','2','3']
        b = ['1','2','3']
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff(a, b), ())

    def test_diff_intersection(self):
        a = ['1','2','3']
        b = ['1','2','4','5']
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff(a, b), (('4','3'), ('5', '4')))

    def test_diff_sparse_intersection2(self):
        a = ['1','5','7']
        b = ['5','7','2']
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff(a, b), (('2', '1'),))


class DiffTuplesTestCase(unittest.TestCase):
    def test_diff_empty(self):
        a = [('1', 'a'), ('2', 'b'), ('3', 'foo')]
        b = [('1', 'a'), ('2', 'b'), ('3', 'foo')]
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff(a, b), ())

    def test_diff_tuples_differ(self):
        a = [('1', 'a'), ('2', 'b'), ('3', 'foo')]
        b = [('1', 'b'), ('2', 'b'), ('3', 'foo')]
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff(a, b), ((('1', 'b'), ('1', 'a')),))

    def test_diff_intersection(self):
        a = [('1', 'a'), ('2', 'b'), ('3', 'foo')]
        b = [('1', 'a'), ('2', 'b'), ('4', 'bar'), ('5', 'baz')]
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff(a, b), ((('4', 'bar'), ('3', 'foo')), (('5', 'baz'), ('4', 'bar'))))


class DiffApplyTuplesTestCase(unittest.TestCase):
    def test_diff_empty(self):
        a = [('1', 'a'), ('2', 'b'), ('3', 'foo')]
        b = [('1', 'a'), ('2', 'b'), ('3', 'foo')]
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff_apply(a, diff(a, b)), tuple(a))

    def test_diff_tuples_differ(self):
        a = [('1', 'a'), ('2', 'b'), ('3', 'foo')]
        b = [('1', 'b'), ('2', 'b'), ('3', 'foo')]
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff_apply(a, diff(a, b)), (('1', 'a'), ('1', 'b'), ('2', 'b'), ('3', 'foo')))

    def test_diff_intersection(self):
        a = [('1', 'a'), ('2', 'b'), ('3', 'foo')]
        b = [('1', 'a'), ('2', 'b'), ('4', 'bar'), ('5', 'baz')]
        self.assertIsNotNone(diff(a,b))
        self.assertEqual(diff_apply(a, diff(a, b)), (('1', 'a'), ('2', 'b'), ('3', 'foo'), ('4', 'bar'), ('5', 'baz')))
