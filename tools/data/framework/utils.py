

'''
source and complement are ordered in default order
'''
def diff(source, complement):
    # union (in terms of `sets theory`) of source and complement iterators
    union = set(source + complement)

    if len(union) == len(source) == len(complement):
        # diff is empty.
        return ()

    ordered = sorted(union)

    # we cannot represent/describe such diff
    if ordered[0] != source[0]:
        return None

    output = []

    previous = None
    for i, item in enumerate(ordered):
        s = source[i] if i < len(source) else None
        if ordered[i] != s:
            if ordered[i] not in source:
                output += [(ordered[i], previous)]
        previous = ordered[i]

    return tuple(output)

def diff_apply(source, diff):
    result = list(source)
    for operation in diff:
        item, after = operation
        index = result.index(after)
        result = result[0:index + 1] + [item] + result[index + 1:]
    return tuple(result)
