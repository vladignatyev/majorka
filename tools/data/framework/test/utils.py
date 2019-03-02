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


class DiffApplyCustomObjectTestCase(unittest.TestCase):
    class ComparableColumn(object):
        def __init__(self, name, type):
            self.name = name
            self.type = type

        def __eq__(self, other):
            return (other is not None) and (self.name == other.name) # and type(self.type) is type(other.type)

        def __cmp__(self, other):
            if other is None: return 1
            if self.name < other.name: return -1
            if self.name > other.name: return 1
            if self.name == other.name: return 0

        def __repr__(self):
            return "{name} : {type}".format(name=self.name,type=type(self.type))

    def test(self):
        c1 = self.ComparableColumn('id', str)
        c2 = self.ComparableColumn('date_added', str)

        c3 = self.ComparableColumn('campaign', str)
        c31 = self.ComparableColumn('campaign', str)

        def custom_sorted(columns):
            return sorted(columns, key=lambda i: i.name, reverse=True)

        self.assertEqual(diff([c1, c2], [c1, c2, c3], custom_sorted=custom_sorted), ((c3, c2),))
