### COVID-19 Digital Observatory
### 2020-03-28
### 
### Minimal example analysis file using trending search data

library(tidyverse)

### Import and cleanup data


related.searches.top = read_csv("https://github.com/CommunityDataScienceCollective/COVID-19_Digital_Observatory/raw/master/keywords/output/intermediate/related_searches_top.csv")


## Plot how often the top 10 queries appear in the top 10 suggested list each day

plot <- related.searches.top %>% 
  group_by(term, date) %>% # Group by term and date
  arrange(-value) %>% # Sort by value (this should already be done anyway)
  top_n(10) %>% # Get the top 10 queries for each term-day pair
  group_by(query) %>% # Group by again, this time for each query
  summarize(appearances = n()) %>% # Count how often this query appears in the top 10 (which is how many Google displays)
  arrange(-appearances) %>% # Sort by appearances
  top_n(10) %>% # And get the top 10 queries
  ggplot(aes(x=reorder(query, appearances), y=appearances)) + # Plot the number of appearances, ordered by appearances
  geom_bar(stat = 'identity') + # Tell R that we want to use the values of `appearances` as the counts
  coord_flip() + # Flip the plot
  xlab("Query") + 
  ylab("Number of appearances in top 10 suggested queries") +
  theme_minimal() # And make it minimal

ggsave('./output/top_queries_plot.png', plot)