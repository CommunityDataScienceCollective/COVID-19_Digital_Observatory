## example reading latest file straight from the server
df <- read.csv("https://covid19.communitydata.science/datasets/keywords/csv/latest.csv")

## make the data more R-friendly
df$is.alt <- df$is_alt == "True"
df$is_alt <- NULL

## find all translations for coronavirus
coronavirus.itemids <- df[ (tolower(df$label) == "coronavirus") &
                         (df$langcode == 'en')
                       ,"itemid"]

## there are actually 5 item ids. The one referring to the family of virus is Q57751738
coronavirus.translations <- df[df$itemid == "http://www.wikidata.org/entity/Q57751738",]

## let's only look at non-aliases
print(coronavirus.translations[c(coronavirus.translations$is.alt == FALSE), c("label","langcode")])
