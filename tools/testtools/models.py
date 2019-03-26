import random
from testtools.simulator import flat_items


# class OfferModel(object):
#     def __init__(self, dimensions):
#         """
#         `dimensions` is a list of property names to use as dimensions for prob. model
#
#         NOTE:
#         `data_dict`, `conversion_dict`, `hit_dict` should contain the same set of keys
#         """
#         self.dimensions = dimensions
#         self.md = MultiDimensionDistribution()
#
#     def _new_vec(self):
#         return [None] * len(self.dimensions)
#
#     def _vec_with_data(self, data_dict):
#         vec = self._new_vec()
#
#         for i, d in enumerate(self.dimensions):
#             vec[i] = conversion_dict.get(d, None)
#
#         return vec
#
#     def learn_conversion(self, conversion_dict):
#         self.md.eat(self._vec_with_data(conversion_dict) + ['conv'])
#     def learn_hit(self, hit_dict):
#         self.md.eat(self._vec_with_data(conversion_dict) + ['noconv'])
#
#     def convert_or_not(self, hit_dict):
#         vec = self._vec_with_data(hit_dict + ['conv'])
#
#         p = self.md.probability(vec)
#         print vec
#         print p
#         return random.random() <= p
#


class MultiDimensionDistribution(object):
    def __init__(self):
        self.dis = {}
        self.counter = 0

    def eat(self, vec):
        """
        Eats provided `vec` sample, and update inner representation
        of the probability distribution

        >>> samples = [['a', 'b'], ['a', 'b'], ['a', 'a'], ['c', 'a'], ['c', 'b'], ['b', 'c'], ['b', 'a'], ['a', 'b'], ['a', 'b'], ['a', 'a'], ['a', 'a'], ['a', 'a']]
        >>> md = MultiDimensionDistribution()
        >>> for sample in samples:
        ...     md.eat(sample)
        >>> md.print_dist()
        p(a) = 8/12
        p(a|a) = 4/8
        p(b|a) = 4/8
        p(c) = 2/12
        p(a|c) = 1/2
        p(b|c) = 1/2
        p(b) = 2/12
        p(a|b) = 1/2
        p(c|b) = 1/2
        """
        self.counter += 1

        dim = vec[0]
        dims = vec[1:]

        if self.dis.get(dim, None) is None:
            dis = MultiDimensionDistribution()
            self.dis[dim] = (0, dis)

        c, dis = self.dis[dim]
        self.dis[dim] = (c + 1, dis)

        if dims != []:
            dis.eat(dims)

    def random_vec(self):
        """
        Generates random vector from given distribution

        >>> md = MultiDimensionDistribution()
        >>> samples = [['a', 'b'], ['a', 'b'], ['a', 'a'], ['c', 'a'], ['c', 'b'], ['b', 'c'], ['b', 'a'], ['a', 'b'], ['a', 'b'], ['a', 'a'], ['a', 'a'], ['a', 'a']]
        >>> for sample in samples:
        ...     md.eat(sample)
        >>> v1 = md.random_vec()
        >>> assert len(v1) == 2
        >>> assert v1[0] in ['a','b','c']
        >>> assert v1[1] in ['a','b','c']
        """
        if self.counter == 0:
            return []
        r = random.randint(0, self.counter - 1)
        acc = 0
        for k in self.dis.keys():
            if r >= acc and r < acc + self.dis[k][0]:
                return [k] + self.dis[k][1].random_vec()

            acc = acc + self.dis[k][0]

    def probability(self, vec):
        """
        Traverses tree and returns the absolute probability of the `vec` (applies
        calculations to get absolute instead of conditional probability).

        In case the `vec` is not completely contained in the space, the probability
        of the lenghtest equal path in tree returned.

        >>> samples = [['a', 'b'], ['a', 'b'], ['a', 'a'], ['c', 'a'], ['c', 'b'], ['b', 'c'], ['b', 'a'], ['a', 'b'], ['a', 'b'], ['a', 'a'], ['a', 'a'], ['a', 'a']]
        >>> md = MultiDimensionDistribution()
        >>> for sample in samples:
        ...     md.eat(sample)
        >>> assert md.probability(['a']) == 8.0 / 12.0
        >>> assert md.probability(['a', 'a']) == 4.0 / 8.0 * 8.0 / 12.0
        >>> assert md.probability(['a', 'b']) == 4.0 / 8.0 * 8.0 / 12.0
        >>> assert md.probability(['b', 'c']) == 1.0 / 2.0 * 2.0 / 12.0
        >>> assert md.probability(['e']) == 0
        >>> assert md.probability(['a', 'd']) == 8.0 / 12.0
        """
        prob = 0.0
        counter = self.counter
        dis = self.dis
        for d in range(len(vec)):
            if counter == 0:
                return prob

            dim = vec[d]
            if dis.get(dim, None) is None:
                return prob

            if prob == 0.0:
                prob = 1.0

            prob = prob * dis[dim][0] * 1.0 / counter
            counter = dis[dim][1].counter
            dis = dis[dim][1].dis

        return prob


    def print_dist(self, given=''):
        for k, v in self.dis.items():

            print "p({k}{g}) = {a}/{b}".format(k=k,
                                               a=v[0],
                                               b=self.counter,
                                               g='|' + given if given != '' else '')

            if given != '':
                v[1].print_dist(given=','.join([str(given), str(k)]))
            else:
                v[1].print_dist(given=str(k))

    def print_graph(self, out='./graph'):
        g = self._new_graph()

        self._draw_graph(g=g)

        g.render(out, format='svg')

    def _new_graph(self, engine='dot'):
        from graphviz import Digraph
        return Digraph(format='svg', engine=engine,
                    graph_attr={'overlap': 'false', 'pad': '0.1', 'nodesep': '0.5', 'ranksep': '1.0'},
                    edge_attr={'fontname':'HelveticaNeue'})

    def _draw_graph(self, g):
        node_idx = 0
        dis = self.dis
        counter = self.counter

        top_nodes = []

        for k, v in dis.items():
            new_idx, node = self._graph_node(g, idx=node_idx, name=k)
            # top_nodes += [node]
            new_idx = self._draw_sub_tree(g, idx=new_idx, parent=node, given=[k,], dis=v[1].dis, root_counter_val=v[0])
            node_idx = new_idx

    def _draw_sub_tree(self, g, idx, parent, given, root_counter_val, dis):
        _idx = idx
        for k, v in dis.items():
            _idx, node = self._graph_node(g, idx=_idx, name=k, given=given, parent=parent, prob=v[0] * 1.0 / root_counter_val)
            if k is None or k.strip() == '':
                k_ = '<empty>'
            else:
                k_ = k
            _idx = self._draw_sub_tree(g, parent=node, idx=_idx, given=given+[k_], dis=v[1].dis, root_counter_val=v[0])
        return _idx


    def _graph_node(self, g, idx, name, prob=0.0, given='', parent=None):
        _idx = idx + 1
        if name is None or name.strip() == '':
            name = '<empty>'

        node_name = "[{i}] {k}{g}".format(i=_idx,
                                         k=name,
                                         g=(' | ' + (', '.join(given))) if given != '' else '')
        g.node(node_name)
        if parent is not None:
            alpha = "{0:x}".format(int(round(prob * 250 + 5)))
            g.edge(parent, node_name,
                 label=" %.4f " % prob,
                 color='#ff0000%s' % alpha,
                 fontcolor='#f00000%s' % alpha)
        return _idx, node_name


if __name__ == '__main__':
    import doctest
    doctest.testmod()

    def example_plot():
        samples = [
          ['zone1',  'chrome',  'dsl', 'offer1'],
          ['zone1',  'chrome',  'dsl', 'offer2'],
          ['zone1',  'chrome',  'dsl', 'offer1'],
          ['zone1',  'chrome',  'dsl', 'offer2'],
          ['zone1', 'firefox',  'dsl', 'offer1'],
          ['zone1', 'firefox',   '3g', 'offer1'],
          ['zone2',  'chrome',  'dsl', 'offer1'],
          ['zone2',  'chrome',  'dsl', 'offer2'],
          ['zone2',  'chrome',  'dsl', 'offer1'],
          ['zone2',  'chrome',  'dsl', 'offer2'],
          ['zone2',  'chrome',  'dsl', 'offer1'],
          ['zone2',  'chrome',  'dsl', 'offer2'],
          ['zone2',  'chrome',  'dsl', 'offer1'],
          ['zone2',  'chrome',  'dsl', 'offer2'],
          ['zone2', 'firefox',  'dsl', 'offer1'],
          ['zone2', 'firefox',   '3g', 'offer1'],
          ['zone2', 'firefox',   '3g', 'offer2'],
          ['zone2', 'firefox',   '3g', 'offer1'],
          ['zone2', 'firefox',   '3g', 'offer2'],
          ['zone2', 'firefox',   '3g', 'offer2'],
          ['zone2', 'firefox',   '3g', 'offer2'],
          ['zone2', 'firefox',   '3g', 'offer2'],
          ['zone3', 'chrome',    '3g', 'offer1'],
          ['zone3', 'firefox',   '3g', 'offer1'],
          ['zone3', 'firefox',  'dsl', 'offer1'],
          ['zone3', 'chrome',    '3g', 'offer1'],
          ['zone3', 'chrome',    '3g', 'offer1'],
          ['zone3', 'firefox',  'dsl', 'offer1'],
          ['zone4', 'firefox',  '', ''],
          ['zone4', 'chrome',  '', ''],
          ['zone4', 'chrome',  '', ''],
          ['zone4', 'firefox',  '', ''],
        ]
        d = MultiDimensionDistribution()
        for sample in samples:
            d.eat(sample)

        print 'space content'
        d.print_dist()
        d.print_graph()

    def example():
        samples = [
          ['a','b'],
          ['a','b'],
          ['a','a'],
          ['c','a'],
          ['c','b'],
          ['b','c'],
          ['b','a'],
          ['a','b'],
          ['a','b'],
          ['a','a'],
          ['a','a'],
          ['a','a'],
        ]

        d = MultiDimensionDistribution()
        for sample in samples:
            d.eat(sample)

        print 'space content'
        d.print_dist()

        print 'test'
        counter = {}
        n = 1000000 # BIG NUMBER!
        for i in range(n):
            v = tuple(d.random_vec())
            if counter.get(v, None) is None:
                counter[v] = 0
            counter[v] = counter[v] + 1.0/float(n)

        for k, v in counter.items():
            print "{k} : {v}".format(k=k, v=v)

        assert abs(0.083333 - counter[('c','b')]) < 0.01
        assert abs(0.333333 - counter[('a','b')]) < 0.01



    # example()
    example_plot()
