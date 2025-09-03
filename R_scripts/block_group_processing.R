install.packages("tidycensus")
install.packages("tidyverse")
install.packages("sf")
install.packages("arrow")

library(tidycensus)
library(tidyverse)
library(sf)
library(arrow)

state_abb_list <- state.abb
state_abb_list <- c(state_abb_list, "DC")


for (state_abb in state_abb_list){
  print(str_c("Processing for ", state_abb))
  
  block_groups <- get_acs(
    geography = "block group",
    variables = c(
      total_pop = "B01003_001E",
      unemployed = "B23025_005E",
      civilian_labor = "B23025_003E",
      income_less_10k = "B19001_002E",
      income_10k_15k = "B19001_003E",
      income_15k_20k = "B19001_004E",
      income_20k_25k = "B19001_005E",
      income_25k_30k = "B19001_006E",
      income_30k_35k = "B19001_007E",
      income_35k_40k = "B19001_008E",
      income_40k_45k = "B19001_009E",
      income_45k_50k = "B19001_010E",
      income_50k_60k = "B19001_011E",
      income_60k_75k = "B19001_012E",
      income_75k_100k = "B19001_013E",
      income_100k_125k = "B19001_014E",
      income_125k_150k = "B19001_015E",
      income_150k_200k = "B19001_016E",
      income_more_200k = "B19001_017E",
      bachelors = "B15003_022E",
      masters = "B15003_023E",
      doctorate = "B15003_025E",
      pop_above_25 = "B15003_001E"
    ),
    state = state_abb,
    year = 2023,
    survey = "acs5",
    geometry = F,
    output = "wide"
  )
  
  block_groups <- block_groups %>%
    select(-ends_with("m")) 
  
  write_parquet(block_groups, file.path("data", "outputs", "block_groups", 
                                 str_c(state_abb, "_BGs.parquet")))
}


