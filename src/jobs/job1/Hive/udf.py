import re
import string
import sys

CLEANR = re.compile("<.*?>")

for line in sys.stdin:
    text = re.sub(CLEANR, " ", line)
    text = text.replace(".", " ")
    text = text.translate(str.maketrans("", "", string.punctuation))
    text = re.sub(" +", " ", text)
    text = text.strip()
    print(text.lower())
