#Chapter 3 - analysis with spark
#Thomas Zwagerman
#06/08

#Libraries----
#Read in the library
library(sparklyr)
library(dplyr)

#connect to spark
sc <- spark_connect(master = "local", version = "2.3")

#import cars into spark
cars <- copy_to(sc, mtcars)
#Note: When using real clusters, you should use copy_to() to transfer only small tables from R; 
#large data transfers should be performed with specialized data transfer tools.

#Wrangle----
summarize_all(cars, mean) %>% 
  show_query()

cars %>%
  mutate(transmission = ifelse(am == 0, "automatic", "manual")) %>%
  group_by(transmission) %>%
  summarise_all(mean)

#Built-in functions
#functions that aren't in dplyr can still be passed on ie percentile() in this example
summarise(cars, mpg_percentile = percentile(mpg, 0.25)) %>% 
  show_query()

#another hive function is array()
summarise(cars, mpg_percentile = percentile(mpg, array(0.25, 0.5, 0.75)))

#can use the explode() function to seperate Spark's array values into their own record
summarise(cars, mpg_percentile = percentile(mpg, array(0.25, 0.5, 0.75))) %>%
  mutate(mpg_percentile = explode(mpg_percentile))

