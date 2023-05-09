# Constants

ALL_MESSAGES = -1

#

class Local:
    def __init__(self):
        self.textIOWrapper = None

    def open(self, file, mode):
        file_str = file
        mode_str = mode
        #
        textIOWrapper = open(file_str, mode_str)
        #
        return textIOWrapper

    def read(self, message_separator="\n", n=ALL_MESSAGES, bufsize=4096, break_function=lambda _: False):
        message_separator_str = message_separator
        num_lines_int = n
        bufsize_int = bufsize
        #
        while True:
            newbuf_str = self.textIOWrapper.read(bufsize_int)
            if not newbuf_str:
                if buf_str:
                    last_line_str = buf_str
                    line_counter_int += 1
                    if self.verbose_int > 0 and line_counter_int % self.progress_num_lines_int == 0:
                        print(f"Read: {line_counter_int}")
                    #
                    key_str_value_str_tuple = split(last_line_str)
                    #
                    if break_function(key_str_value_str_tuple):
                        break_bool = True
                        break
                    #
                    acc = foldl_function(acc, key_str_value_str_tuple)
                break
            buf_str += newbuf_str
            line_str_list = buf_str.split(message_separator_str)
            for line_str in line_str_list[:-1]:
                line_counter_int += 1
                if self.verbose_int > 0 and line_counter_int % progress_num_lines_int == 0:
                    print(f"Read: {line_counter_int}")
                #
                key_str_value_str_tuple = split(line_str)
                #
                if break_function(key_str_value_str_tuple):
                    break_bool = True
                    break
                #
                acc = foldl_function(acc, key_str_value_str_tuple)
                #
                if num_lines_int != ALL_MESSAGES:
                    if line_counter_int >= num_lines_int:
                        break
            #
            if break_bool:
                break
            #
            buf_str = line_str_list[-1]

#

def split(line_str):
    key_str = None
    value_str = None
    #
    if line_str:
        if key_value_separator_str is not None:
            split_str_list = line_str.split(key_value_separator_str)
            if len(split_str_list) == 2:
                key_str = split_str_list[0]
                value_str = split_str_list[1]
            else:
                value_str = line_str
        else:
            value_str = line_str
    #
    return key_str, value_str 
