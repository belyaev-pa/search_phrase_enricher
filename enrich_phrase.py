from sets import *



def remove_trash_chars(string, trash_chars=TRASH_CHARS, make_lower=True):
    if make_lower:
        string = string.lower()
    for i in range(2):
        for rem, repl in trash_chars.items():
            string = string.replace(rem, repl)
    return string


def append_to_csv(row_as_list, csv_path, delimiter=";"):
    try:
        row_as_list = [str(cell) for cell in row_as_list]
        row = delimiter.join(row_as_list)
        row = remove_trash_chars(row, trash_chars=TRASH_CHARS_FOR_OUTPUT, make_lower=False)
        with codecs.open(csv_path, "a", encoding="utf-8") as f:
            f.write(row + "\n")
    except Exception as exc:
        print("was unable to write to", csv_path, exc)


def get_word_tag(word):
    word = word.lower()
    morph = MORPH.parse(word)[0]
    raw_tag = morph.tag.__str__().split(",")[0].split(" ")[0]
    return raw_tag


def worker_sp(worker_id):
    while True:
        try:
            row = next(SearchPhrase.sp_rows_generator)
            if len(row) > 0 and row[0].internal_value:
                SearchPhrase(row[0].internal_value)
            time.sleep(random.random() / 10)
        except StopIteration:
            print("worker_sp #{} has finished its task. Closing...".format(str(worker_id)))
            return None
        except ValueError as exc:
            print(exc)
            time.sleep(random.random())


def load_input_xlsx(input_xlsx):
    wb = px.load_workbook(input_xlsx)
    used_strings = set()

    # Get SearchPhrases
    # ws = wb.get_sheet_by_name("new")
    ws = wb.active
    SearchPhrase.sp_rows_generator = ws.iter_rows()
    next(SearchPhrase.sp_rows_generator)
    pool = Pool(THREADS_NUM)
    pool.map(worker_sp, list(range(THREADS_NUM)))


class SearchPhrase(object):
    all = list()
    headers = list()
    sp_rows_generator = None

    def __init__(self, sp):
        self.row = list()
        self.sp = sp.lower()
        self.words_set = set(self.sp.split(" "))
        SearchPhrase.all.append(self)
        if any(com_word.lower() in self.sp for com_word in COMMERCE_WORDS):
            self.if_commerce = '#commerce'
        else:
            self.if_commerce = ''

        common_names = self.words_set & fios_all_set
        if len(common_names) > 0:
            for name in common_names:
                for nt in NAME_TYPES:
                    if fios_all[name] == nt:
                        self.__dict__[nt] = name
                    else:
                        self.__dict__[nt] = ""
        else:
            check_fio = list()
            for word in self.words_set:
                check_fio.append(MORPH.parse(word)[0].normal_form)
            common_names = set(check_fio) & fios_all_set
            if len(common_names) > 0:
                for name in common_names:
                    for nt in NAME_TYPES:
                        if fios_all[name] == nt:
                            self.__dict__[nt] = name
                        else:
                            self.__dict__[nt] = ""
            else:
                for nt in NAME_TYPES:
                    self.__dict__[nt] = ""
        self.is_geo = ""
        self.addresses = ""
        self.get_addresses()
        if self.addresses:
            self.is_geo = "#geo"
        self.output_row()

    def get_addresses(self):
        addresses = list()
        for word in self.words_set:
            word = word.lower()
            raw_tag = get_word_tag(word)
            word_address = ""
            if raw_tag in TRASH_TAGS:
                continue
            if word in true_adr_words:
                word_address = true_adr_words[word]
            else:
                word = MORPH.parse(word)[0].normal_form
                if word in true_adr_words:
                    word_address = true_adr_words[word]
            if word_address:
                addresses.append(word_address)
        self.addresses = " | ".join(addresses)

    def output_row(self):
        for col in SEARCH_PHRASE_OUTPUT_COLUMNS:
            self.row.append(self.__dict__[col])
        append_to_csv(self.row, OUTPUT_SP_CSV)
        # print(self.row)



if __name__ == '__main__':
    load_input_xlsx(INPUT_FILE)