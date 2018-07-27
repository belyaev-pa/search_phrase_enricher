import pymorphy2
import re
from sys import argv


INPUT_FILE = argv[1] # csv
morph = pymorphy2.MorphAnalyzer()
strings = open(INPUT_FILE, 'r')
with open(f'{INPUT_FILE.split(".")[0]}_normalized.csv', 'w') as f:
	for string in strings:
		string = string.split(';')
		string1 = string[0].strip()
		string2 = ';'.join(string[1:])
		string2 = string2.strip()
		new_string = list()
		string_ = re.sub('[\+\-\*=)(!/&?\n\t]', ' ', string1).lower().split()
		for word in string_:
			if word:
				word = morph.parse(word)[0].normal_form
				new_string.append(word)
		f.write(f"{string1};{' '.join(new_string)};{string2}\n")