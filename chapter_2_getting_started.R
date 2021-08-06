#Getting started with Spark
#Thomas Zwagerman
#06/08

#Read in the library
library(sparklyr)

#Connect to the local cluster
sc <- spark_connect(master = "local", version = "2.3")

#copying a dataset into Spark
cars <- copy_to(sc,mtcars)

#now I can simply type cars and that works
cars

#Spark web interface----
spark_web(sc)

#Analysis----
#you can use SQL or dplyr with spark
#SQL
library(DBI)
dbGetQuery(sc, "SELECT count(*) FROM mtcars")

#dplyr
library(dplyr)
count(cars)
select(cars, hp, mpg) %>%
  sample_n(100) %>%
  collect() %>%
  plot()

#Modelling-----
#relationship between fuel efficiency and hp
model <- ml_linear_regression(cars, mpg ~ hp)
model

#we can add entries for cars with horsepower beyond 250 and also visualize the predicted values
model %>%
  ml_predict(copy_to(sc, data.frame(hp = 250 + 10 * 1:10))) %>%
  transmute(hp = hp, mpg = prediction) %>%
  full_join(select(cars, hp, mpg)) %>%
  collect() %>%
  plot()

#data is not usually read into spark but comes from another source ie a csv
spark_write_csv(cars, "cars.csv")
#or read in from a distributed storage system, or from local disk:
cars <- spark_read_csv(sc, "cars.csv")

#Extensions----
#sparkly nested
install.packages("sparkly.nested")
library()

#Streaming----
dir.create("input")
write.csv(mtcars, "input/cars_1.csv", row.names = F)
#Then, we define a stream that processes incoming data from the input/ folder, 
#performs a custom transformation in R, and pushes the output into an output/ folder:
stream <- stream_read_csv(sc,"input/") %>% 
  select(mpg,cyl,disp) %>% 
  stream_write_csv("output/")
#check it's there
dir("output", pattern = ".csv")
# Write more data into the stream source
write.csv(mtcars, "input/cars_2.csv", row.names = F)
# Check the contents of the stream destination
dir("output", pattern = ".csv")
#stop the stream
stream_stop(stream)

#Logging----
spark_log(sc)
spark_log(sc, filter = "sparklyr")

#Disconnecting----
spark_disconnect(sc)
#for multiple instances
spark_disconnect_all()

