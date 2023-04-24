import requests

resp = requests.get("https://raw.githubusercontent.com/stopwords-iso/stopwords-iso/master/stopwords-iso.json")
resp.raise_for_status()
data = resp.json()

with requests.Session() as sess, open("stopwords.rs", "w") as mod:
    mod.write("/*\n")
    mod.write(
        "These stop word lists are from the stopwords-iso project (https://github.com/stopwords-iso/stopwords-iso/) "
        "which carries the MIT license."
    )
    mod.write("\n*/\n\n")

    for lang, data in data.items():

        mod.write(f"pub(crate) const {lang.upper()}: &[&str] = &[\n")

        for word in data:
            mod.write(f'    r#"{word}"#,\n')

        mod.write("];\n\n")
        print(f'"{lang}" => stopwords::{lang.upper()},')
