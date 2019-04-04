

class TabSeparatedError(Exception):
    def __init__(self, msg='', col=0, row=0, val=''):
        self.col = col
        self.row = row
        self.msg = msg
        self.val = val

        message = """

        Cannot convert into tab separated data.
        Error has occured at ({col}:{row}): {msg}!
        Error first occured in this sample: {val}
        """
        super(TabSeparatedError, self).__init__(message.format(col=self.col,
                                                                 row=self.row,
                                                                 val=self.val,
                                                                 msg=self.msg))

def _tab_separated_escape_vals(enumeration):
    col, value = enumeration
    # According to IANA TSV format definition,
    # TAB chars are disallowed within field values
    # https://www.iana.org/assignments/media-types/text/tab-separated-values
    val = unicode(value)

    try:
        val.index('\t')
        raise TabSeparatedError(msg='TAB char is disallowed within field values',
                                col=col)
    except ValueError:
        return val

def _tab_separated_row_func(dims):
    def row_func(enumeration):
        row, row_content = enumeration
        # check dimensions
        if len(row_content) != dims:
            raise TabSeparatedError(msg='dimensions for every row '
                                        'should match dimension of '
                                        'first row in data',
                                    row=row,
                                    val=row_content)
        try:
            row_tab_separated = u'\t'.join(map(_tab_separated_escape_vals, enumerate(row_content)))
        except Exception as e:
            raise TabSeparatedError(msg=e.msg, row=row, col=e.col, val=row_content)

        return row_tab_separated
    return row_func

class TabSeparated(object):
    def __init__(self, data):
        self.data = data
        assert type(data) == list or type(data) == tuple

    def generate(self):
        # trivial case
        if len(self.data) == 0:
            return u""

        output = u""
        dims = len(self.data[0])
        print self.data
        return u'\n'.join(map(_tab_separated_row_func(dims), enumerate(self.data)))
