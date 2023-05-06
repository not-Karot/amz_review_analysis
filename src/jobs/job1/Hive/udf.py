import re
import string

CLEANR = re.compile('<.*?>')

def cleanhtml(raw_html):
    return re.sub(CLEANR, ' ', raw_html)

def clean_text(text):
    text = cleanhtml(text)
    text = text.replace(".", " ")
    text = text.translate(str.maketrans('', '', string.punctuation))
    text = re.sub(' +', ' ', text)
    text = text.strip()
    return text.lower()
