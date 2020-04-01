import pandas as pd

# read the latest dataset
df  = pd.read_csv("https://covid19.communitydata.science/datasets/keywords/csv/latest.csv")

# find translations of "coronavirus"
coronavirus_itemids = df.loc[df.label.str.lower() == "coronavirus"]

# there are actually 5 item ids. The one referring to the family of virus is Q57751738
coronavirus_translations = df.loc[df.itemid == "http://www.wikidata.org/entity/Q57751738"]

# let's only look at unique, non-aliases
print(coronavirus_translations.loc[df.is_alt == False,['label','langcode']])
