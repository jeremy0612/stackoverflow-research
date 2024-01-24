import re

def extract_languages(string):
        lang_regex = r"Java|Python|C\+\+|C\#|Go|Ruby|Javascript|PHP|HTML|CSS|SQL"
        if string is not None:
            return re.findall(lang_regex, string)
        
print(extract_languages("Hello Python Python Go and Ruby!"))