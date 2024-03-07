import illumio_pylo as pylo


def find_chars(text: str, start: int):
    end = len(text)
    return {"ending_parenthesis": text.find(")", start),
            "opening_parenthesis": text.find(")", start),
            "opening_squote": text.find("'", start),
            }


def find_first_punctuation(text: str, start: int):
    cursor = start
    while cursor < len(text):
        char = text[cursor]
        if char == ')':
            return {'notfound': False, 'position': cursor, 'character': ')'}
        if char == '(':
            return {'notfound': False, 'position': cursor, 'character': '('}
        if char == "'":
            return {'notfound': False, 'position': cursor, 'character': "'"}

        cursor += 1

    return {'notfound': True}


class get_block_response:
    def __init__(self, length=None, operator=None, error=None):
        self.length = length
        self.operator = operator
        self.error = error


def get_block_until_binary_ops_quotes_enabled(data: str):

    detected_quote = None

    for pos in range(len(data)):

        cur_2let = data[pos:pos+2].lower()
        cur_3let = data[pos:pos+3].lower()

        # print("DEBUG {}  {}".format(cur_2let, cur_3let))

        if cur_2let == 'or':
            if detected_quote is None:
                return get_block_response(length=pos, operator='or')
        elif cur_3let == 'and':
            if detected_quote is None:
                return get_block_response(length=pos, operator='and')

        cur_char = data[pos]

        if cur_char == "'":
            if detected_quote is None:
                detected_quote = cur_char
            elif detected_quote == '"':
                continue
            else:
                detected_quote = None

        elif cur_char == '"':
            if detected_quote is None:
                detected_quote = cur_char
            elif detected_quote == "'":
                continue
            else:
                detected_quote = None

    if detected_quote is None:
        return get_block_response(length=len(data))

    return get_block_response(error="some quotes {} were not closed in expression: {}".format(detected_quote, data))


class Query:
    def __init__(self, level=0):
        self.level = level
        self.subQueries = []  # type: list[pylo.Query]
        self.raw_value = None

    def parse(self, data: str):
        padding = ''.rjust(self.level*3)
        data_len = len(data)
        self.raw_value = data

        cursor = 0
        current_block_start = 0
        parenthesis_opened = []
        blocks = []
        reached_end = False

        print(padding + 'Level {} parsing string "{}"'.format(self.level , data))

        while cursor < len(data):
            find_punctuation = find_first_punctuation(data, cursor)
            if find_punctuation['notfound']:
                if len(parenthesis_opened) > 0:
                    raise pylo.PyloEx("Reached the end of string before closing parenthesis in block: {}".format(data[current_block_start-1:]))
                blocks.append({'type': 'text', 'text': data[current_block_start:]})
                reached_end = True
                print(padding + "{}-{} REACHED END OF STRING".format(self.level, len(blocks)))
                break

            found_character = find_punctuation['character']
            found_character_position = find_punctuation['position']

            if found_character == "'":
                find_next_quote = data.find("'", found_character_position+1)
                if find_next_quote == -1:
                    raise pylo.PyloEx("Cannot find a matching closing quote from position {} in text: {}".format(found_character_position, data[current_block_start:]))
                cursor = find_next_quote+1
                if cursor >= len(data):
                    blocks.append({'type': 'text', 'text': data[current_block_start:]})
                continue
            elif found_character == ")":
                if len(parenthesis_opened) < 1:
                    raise pylo.PyloEx("Cannot find a matching opening parenthesis at position #{} in text: {}".format(found_character_position, data[current_block_start:]))
                elif len(parenthesis_opened) == 1:
                    parenthesis_opened.pop()
                    blocks.append({'type': 'sub', 'text': data[current_block_start:found_character_position]})
                    current_block_start = found_character_position + 1
                    cursor = current_block_start
                    print(padding + "{}-{} REACHED BLOCK-END C PARENTHESIS".format(self.level, len(blocks)))
                    continue
                else:
                    parenthesis_opened.pop()
                    cursor = found_character_position + 1
                    print(padding + "{}-{} REACHED MID-BLOCK C PARENTHESIS".format(self.level, len(blocks)))
                    continue
            elif found_character == "(":
                parenthesis_opened.append(found_character_position)
                print(padding + "{}-{} FOUND OPENING P".format(self.level, len(blocks)))

                if (found_character_position == 0 or found_character_position > current_block_start) and len(parenthesis_opened) == 1:
                    if found_character_position != 0:
                        blocks.append({'type': 'text', 'text': data[current_block_start:found_character_position]})
                    current_block_start = found_character_position+1
                    cursor = current_block_start
                    print(padding + "{}-{} OPENING P WAS FIRST".format(self.level, len(blocks)))
                    continue
                else:
                    print(padding + "{}-{} OPENING P WAS NOT FIRST".format(self.level, len(blocks)))

                cursor = found_character_position + 1
                continue

            cursor += 1

        if len(parenthesis_opened) > 0:
            raise pylo.PyloEx("Reached the end of string before closing parenthesis in block: {}".format(
                data[current_block_start-1:]))

        print(padding + "* Query Level {} blocks:".format(self.level))
        for block in blocks:
            print(padding + "- {}: |{}|".format(block['type'], block['text']))

        # clear empty blocks, they're just noise
        cleared_blocks = []
        for block in blocks:
            if block['type'] == 'text':
                block['text'] = block['text'].strip()
                if len(block['text']) < 1:
                    continue
            cleared_blocks.append(block)

        # now building operator blocks
        operator_blocks = []

        for block_number in range(len(blocks)):
            block = blocks[block_number]
            if block['type'] == 'sub':
                new_sub_query = Query(self.level+1)
                new_sub_query.parse(block['text'])
                self.subQueries.append(new_sub_query)
                new_block = {'type': 'query', 'query': new_sub_query}
                operator_blocks.append(new_block)
                continue

            text = block['text']
            found_filter_name = False
            found_filter_operator = False

            filter_name = None
            filter_operator = None
            find_filter_in_collection = None

            while True:
                text_len = len(text)
                if text_len < 1:
                    break
                print(padding + "* Handling of text block '||{}||'".format(text))
                first_word_end = text.find(' ')

                if first_word_end < 0:
                    first_word = text
                    first_word_end = text_len
                else:
                    first_word = text[0:first_word_end]
                first_word_lower = first_word.lower()
                print(padding + "  - First word '{}'".format(first_word))

                if first_word_lower == 'or' or first_word_lower == 'and':
                    if found_filter_name:
                        if not found_filter_operator:
                            raise pylo.PyloEx("Found binary operator '{}' while filter '{}' was found but no operator provided in expression '{}'".format(first_word, filter_name, block['text']))
                        if find_filter_in_collection.arguments is not None:
                            raise pylo.PyloEx(
                                "Found binary operator '{}' while filter '{}' with operator '{}' requires arguments in expression '{}'".format(
                                    first_word, filter_name, filter_operator, block['text']))
                        new_block = {'type': 'filter', 'filter': find_filter_in_collection, 'raw_arguments': None}
                        operator_blocks.append(new_block)
                        found_filter_name = False
                        found_filter_operator = False

                    new_block = {'type': 'binary_op', 'value': first_word_lower}
                    operator_blocks.append(new_block)
                elif found_filter_name and found_filter_operator:
                    block_info = get_block_until_binary_ops_quotes_enabled(text)
                    if block_info.error is not None:
                        raise pylo.PyloEx(block_info.error)

                    if block_info.length == 0:
                        new_block = {'type': 'filter', 'filter': find_filter_in_collection, 'raw_arguments': None}
                        operator_blocks.append(new_block)
                        print(padding+"   - Found no argument (or empty)")
                        raise pylo.PyloEx("This should never happen")
                    else:
                        new_block = {'type': 'filter', 'filter': find_filter_in_collection, 'raw_arguments': text[0:block_info.length].strip()}
                        operator_blocks.append(new_block)
                        print(padding + "   - Found argument ||{}|| stopped by {}".format(new_block['raw_arguments'], block_info.operator))

                    found_filter_name = False
                    found_filter_operator = False

                    first_word_end = block_info.length - 1

                elif not found_filter_name and not found_filter_operator:
                    filter_name = first_word
                    found_filter_name = True
                    find_filter_in_collection = FilterCollections.workload_filters.get(filter_name)
                    if find_filter_in_collection is None:
                        raise pylo.PyloEx("Cannot find a filter named '{}' in expression '{}'".format(filter_name, block['text']))
                elif found_filter_name and not found_filter_operator:
                    filter_operator = first_word
                    found_filter_operator = True
                    find_filter_in_collection = find_filter_in_collection.get(filter_operator)
                    if find_filter_in_collection is None:
                        raise pylo.PyloEx("Cannot find a filter operator '{}' for filter named '{}' in expression '{}'".format(filter_operator, filter_name, block['text']))

                if first_word_end >= text_len:
                    break
                text = text[first_word_end + 1:].strip()

        # showing blocks to check how well we did
        for block in operator_blocks:
            print(block)

        # todo: optimize/handle inverters

    def execute_on_single_object(self, object):
        if len(self.subQueries) == 1:
            pass

        return False


class Filter:
    def __init__(self, name: str, func, arguments=None):
        self.name = name
        self.function = func
        self.arguments = arguments


class WorkloadFilter(Filter):
    def __init__(self, name: str, func, arguments = None):
        Filter.__init__(self, name, func, arguments)

    def do_filter(self, workload: pylo.Workload):
        return self.function(workload)


class FilterContext:
    def __init__(self, argument, math_op=None):
        self.math_op = math_op
        self.argument = argument


class FilterCollections:
    workload_filters = {}  # type: dict[str,WorkloadFilter]

    @staticmethod
    def add_workload_filter(name: str, operator: str, func, arguments=None):
        new_filter = WorkloadFilter(name, func)
        if FilterCollections.workload_filters.get(name) is None:
            FilterCollections.workload_filters[name.lower()] = {}

        if FilterCollections.workload_filters[name.lower()].get(operator) is not None:
            raise pylo.PyloEx("Filter named '{}' with operator '{}' is already defined".format(name, operator))
        FilterCollections.workload_filters[name.lower()][operator.lower()] = new_filter


def tmp_func(wkl: pylo.Workload, context: FilterContext):
    return wkl.get_name() == context.argument


FilterCollections.add_workload_filter('name', 'matches', tmp_func, 'string')


def tmp_func(wkl: pylo.Workload, context: FilterContext):
    if context.math_op == '>':
        return wkl.count_references() > context.argument


FilterCollections.add_workload_filter('reference.count', '<>=!', tmp_func, 'int')


def tmp_func(wkl: pylo.Workload, context: FilterContext):
    return context.argument in wkl.description


FilterCollections.add_workload_filter('description', 'contains', tmp_func, 'string')








