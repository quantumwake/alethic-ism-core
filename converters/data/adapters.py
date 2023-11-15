from io import StringIO

class OrganizedStateCSVStringIO(StringIO):

    def __init__(self, organized_state: dict):

        self.organized_state = organized_state
        self.row_index = 0
        self.row_text_char_index = 0
        self.row_text = None

    @property
    def count(self):
        return int(self.organized_state['row_count'])

    def seek(self, __cookie, __whence=...):
        self.row_index = __cookie

    def read(self, __size=...):

        buffer = []
        for index in range(__size):

            # check whether we need to fetch a new row given the position of the char index within this row
            if not self.row_text or self.row_text_char_index == len(self.row_text):
                self.row_text_char_index = 0
                self.row_text = self.readline()

            # for readability
            row_text = self.row_text

            # add the character to the array
            buffer.append(row_text[self.row_text_char_index])

            # increment the position of the row text column/char index
            self.row_text_char_index = self.row_text_char_index + 1

    def readline(self, __size=...):

        if __size:
            raise NotImplementedError(f'size input in readline not implemented of size {__size}')

        if self.row_index == self.count:
            raise Exception(f'no data found at row {self.row_index} of {self.count}')

        # read the column headers and fetch the relevant data fields at the correct index
        column_headers = self.organized_state['header']['columns']
        row_idx = self.row_index
        line = [self.organized_state[column][row_idx]
                for column, column_header
                in column_headers]

        # join them to be a CSV delimited by | pipe
        self.row_text = "|".join(line)
        self.row_index = self.row_index + 1
        return self.row_text
