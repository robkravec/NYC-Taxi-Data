## This script imports locally saved versions of yellow and green taxi data from
## Feb 2015 into Spark and performs some data wrangling that will be helpful
## for constructing heat maps

# Citation for rounding to custom amounts (didn't end up using but served 
# as inspiration)
# https://www.rdocumentation.org/packages/DescTools/versions
# /0.99.38/topics/RoundTo

# Citation for boundaries of New York City Borough
#https://www1.nyc.gov/assets/planning/download/pdf/data-maps/open-data/
#nybb_metadata.pdf?ver=18c

# Citation for changing color scale on geom_tile
# https://stackoverflow.com/questions/5069220/plotting-heatmaps-with-ggplot2-
# logscale-color-and-modify-color-legend

# Load libraries
library(sparklyr)
library(tidyverse)
library(patchwork)


# Connect to spark
conf <- list(
  sparklyr.cores.local = 4,
  `sparklyr.shell.driver-memory` = "16G",
  spark.memory.fraction = 0.5
)
sc <- spark_connect(master = "local", version = "3.0", config = conf)

# Load data into spark
yellow_tbl <- spark_read_csv(sc, name = "yellow_feb_15", 
                             path = "data/yellow.csv")
green_tbl <- spark_read_csv(sc, name = "green_feb_15", path = "data/green.csv")

### Yellow taxi data wrangling

# Select relevant columns from yellow taxi data
yellow_tbl_mod <- yellow_tbl %>%
  filter(pickup_latitude != 0 & pickup_longitude != 0) %>% 
  transmute(pickup_datetime = tpep_pickup_datetime,
            dropoff_datetime = tpep_dropoff_datetime,
            pickup_long = round(pickup_longitude, 3),
            pickup_lat = round(pickup_latitude, 3),
            dropoff_long = round(dropoff_longitude, 3),
            dropoff_lat = round(dropoff_latitude, 3),
            rush_hour = (between(hour(pickup_datetime), 7, 10) |
                           between(hour(pickup_datetime), 16, 19))
            )

# Aggregate yellow taxi pickup table (collected back to R)
yellow_tbl_mod_pickup <- yellow_tbl_mod %>% 
  count(pickup_long, pickup_lat) %>% 
  arrange(desc(n)) %>% 
  collect()

# Aggregate yellow taxi dropoff table (collected back to R)
yellow_tbl_mod_dropoff <- yellow_tbl_mod %>% 
  count(dropoff_long, dropoff_lat) %>% 
  arrange(desc(n)) %>% 
  collect()

# Rush hour calculations for Task 2
# Aggregate yellow taxi pickup table (collected back to R)
yellow_rush_hr_pickup <- yellow_tbl_mod %>% 
  count(pickup_long, pickup_lat, rush_hour) %>% 
  arrange(desc(n)) %>% 
  collect()

# Aggregate yellow taxi dropoff table (collected back to R)
yellow_rush_hr_dropoff <- yellow_tbl_mod %>% 
  count(dropoff_long, dropoff_lat, rush_hour) %>% 
  arrange(desc(n)) %>% 
  collect()

### Green taxi data wrangling

# Select relevant columns from green taxi data
green_tbl_mod <- green_tbl %>%
  filter(Pickup_latitude != 0 & Pickup_longitude != 0) %>% 
  transmute(pickup_datetime = lpep_pickup_datetime,
            dropoff_datetime = Lpep_dropoff_datetime,
            pickup_long = round(Pickup_longitude, 3),
            pickup_lat = round(Pickup_latitude, 3),
            dropoff_long = round(Dropoff_longitude, 3),
            dropoff_lat = round(Dropoff_latitude, 3),
            rush_hour = (between(hour(pickup_datetime), 7, 10) |
                           between(hour(pickup_datetime), 16, 19))
  )


# Aggregate green taxi pickup table (collected back to R)
green_tbl_mod_pickup <- green_tbl_mod %>% 
  count(pickup_long, pickup_lat) %>% 
  arrange(desc(n)) %>% 
  collect()

# Aggregate green taxi dropoff table (collected back to R)
green_tbl_mod_dropoff <- green_tbl_mod %>% 
  count(dropoff_long, dropoff_lat) %>% 
  arrange(desc(n)) %>% 
  collect()

# Rush hour calculations for Task 2

# Aggregate green taxi rush hour pickup table (collected back to R)
green_rush_hr_pickup <- green_tbl_mod %>% 
  count(pickup_long, pickup_lat, rush_hour) %>% 
  arrange(desc(n)) %>% 
  collect()

# Aggregate green taxi rush hour dropoff table (collected back to R)
green_rush_hr_dropoff <- green_tbl_mod %>% 
  count(dropoff_long, dropoff_lat, rush_hour) %>% 
  arrange(desc(n)) %>% 
  collect()

# Disconnect from spark
spark_disconnect_all()

# Create heatmap of yellow taxi pickup
yellow_pick <- ggplot(data = yellow_tbl_mod_pickup, 
       mapping = aes(x = pickup_long, y = pickup_lat, fill = n)) +
  geom_tile() +
  # Legend breaks chosen based on values naturally displayed with log transform
  scale_fill_gradient(trans = 'log',
                      breaks = c(1, 20, 400, 8000)
                      ) +
  labs(x = "Pickup longitude", y = "Pickup latitude",
       title = "Feb 2015 Yellow Taxi Pickup",
       fill = "Count") +
  theme_bw() +
  theme(plot.title = element_text(hjust = 0.5)) +
  lims(x = c(-74.3, -73.6), y = c(40.5, 41)) 
ggsave(filename = "plots/yellow_pickup.png", plot = yellow_pick) # Save plot

# Create heatmap of yellow taxi dropoff
yellow_drop <- ggplot(data = yellow_tbl_mod_dropoff, 
       mapping = aes(x = dropoff_long, y = dropoff_lat, fill = n)) +
  geom_tile() +
  # Legend breaks chosen based on values naturally displayed with log transform
  scale_fill_gradient(trans = 'log',
                      breaks = c(1, 20, 400, 8000)
                      ) +
  labs(x = "Dropoff longitude", y = "Dropoff latitude",
       title = "Feb 2015 Yellow Taxi Dropoff",
       fill = "Count") +
  theme_bw() +
  theme(plot.title = element_text(hjust = 0.5)) +
  lims(x = c(-74.3, -73.6), y = c(40.5, 41)) 
ggsave(filename = "plots/yellow_dropoff.png", plot = yellow_drop) # Save plot

# Create heatmap of green taxi pickup
green_pick <- ggplot(data = green_tbl_mod_pickup, 
       mapping = aes(x = pickup_long, y = pickup_lat, fill = n)) +
  geom_tile() +
  # Legend breaks chosen based on values naturally displayed with log transform
  scale_fill_gradient(trans = 'log',
                      breaks = c(1, 20, 400, 8000)
                      ) +
  labs(x = "Pickup longitude", y = "Pickup latitude",
       title = "Feb 2015 Green Taxi Pickup",
       fill = "Count") +
  theme_bw() +
  theme(plot.title = element_text(hjust = 0.5)) +
  lims(x = c(-74.3, -73.6), y = c(40.5, 41)) 
ggsave(filename = "plots/green_pickup.png", plot = green_pick) # Save plot

# Create heatmap of green taxi dropoff
green_drop <- ggplot(data = green_tbl_mod_dropoff, 
       mapping = aes(x = dropoff_long, y = dropoff_lat, fill = n)) +
  geom_tile() +
  # Legend breaks chosen based on values naturally displayed with log transform
  scale_fill_gradient(trans = 'log', 
                      breaks = c(1, 20, 400)
  ) +
  labs(x = "Dropoff longitude", y = "Dropoff latitude",
       title = "Feb 2015 Green Taxi Dropoff",
       fill = "Count") +
  theme_bw() +
  theme(plot.title = element_text(hjust = 0.5)) +
  lims(x = c(-74.3, -73.6), y = c(40.5, 41)) 
ggsave(filename = "plots/green_dropoff.png", plot = green_drop) # Save plot

# Create a single plot using patchwork
combo_plot <- (yellow_pick + yellow_drop) / (green_pick + green_drop)
ggsave(filename = "plots/combo_plot.png", plot = combo_plot)


# Task 2 heat maps 

facet_names_rh <- c(
  `FALSE` = "",
  `TRUE` = ""
)

# Create heatmap of yellow taxi pickup including rush_hour
yellow_pick_rh <- ggplot(data = yellow_rush_hr_pickup, 
                      mapping = aes(x = pickup_long, 
                                    y = pickup_lat)) +
  geom_point(aes(alpha = n,
                 col = rush_hour),
             size = 0.01) +
  # Legend breaks chosen based on values naturally displayed with log transform
  scale_alpha_continuous(range = c(.2, 1),
                         trans = "log",
                         breaks = c(1, 20, 400, 8000),
                         name = "Count") +
  scale_color_manual(name = "Rush Hour",
                     values = c("#74add1", "#d73027")) +
  labs(x = "Pickup longitude", y = "Pickup latitude",
       title = "Feb 2015 Yellow Taxi Pickups",
       fill = "Count") +
  guides(color = guide_legend(override.aes = list(size=3)),
         alpha = guide_legend(override.aes = list(size=2))) +
  lims(x = c(-74.1, -73.7), y = c(40.5, 40.92)) +
  facet_wrap(rush_hour ~ ., 
             scales = "fixed", 
             labeller = as_labeller(facet_names_rh),
             strip.position =  "top") +
  theme_bw() +
  theme(plot.title = element_text(hjust = 0.5)) 
ggsave(filename = "plots/yellow_rush_pickup.png", plot = yellow_pick_rh)


# Create heatmap of green taxi pickup
green_pick_rh <- ggplot(data = green_rush_hr_pickup, 
                        mapping = aes(x = pickup_long, 
                                      y = pickup_lat)) +
  geom_point(aes(alpha = n,
                 col = rush_hour),
             size = 0.01) +
  # Legend breaks chosen based on values naturally displayed with log transform
  scale_alpha_continuous(range = c(.2, 1),
                         trans = "log",
                         breaks = c(1, 20, 400, 8000),
                         name = "Count") +
  scale_color_manual(name = "Rush Hour",
                     values = c("#74add1", "#d73027")) +
  labs(x = "Pickup longitude", y = "Pickup latitude",
       title = "Feb 2015 Green Taxi Pickups",
       fill = "Count") +
  guides(color = guide_legend(override.aes = list(size=3)),
         alpha = guide_legend(override.aes = list(size=2))) +
  lims(x = c(-74.1, -73.7), y = c(40.5, 40.92)) +
  facet_wrap(rush_hour ~ ., 
             scales = "fixed", 
             labeller = as_labeller(facet_names_rh),
             strip.position =  "top") +
  theme_bw() +
  theme(plot.title = element_text(hjust = 0.5)) 
ggsave(filename = "plots/green_rush_pickup.png", plot = green_pick_rh)

# Create dataframe for 4-way face plot

yellow_rush_hr_pickup$taxi = "Yellow Taxi"
green_rush_hr_pickup$taxi = "Green Taxi"

facet_names <- c(
  `Green Taxi` = "Green Taxi",
  `Yellow Taxi` = "Yellow Taxi",
  `FALSE` = "Not Rush Hour",
  `TRUE` = "Rush Hour"
)
taxi_rush_hr <- rbind(yellow_rush_hr_pickup, 
                      green_rush_hr_pickup)
taxi_rush_hr <- taxi_rush_hr %>% 
  mutate(avg_pickup_hr = if_else(rush_hour, n /6, n/18))
# Create heatmap of green taxi pickup
combo_plot_rh <- ggplot(data = taxi_rush_hr, 
                        mapping = aes(x = pickup_long, 
                                      y = pickup_lat)) +
  geom_point(aes(alpha = avg_pickup_hr,
                 col = rush_hour),
             size = 0.01) +
  # Legend breaks chosen based on values naturally displayed with log transform
  scale_alpha_continuous(range = c(.1, 1),
                         trans = "log", 
                         limits = range(taxi_rush_hr$avg_pickup_hr),
                         name = "Avg Pickup / Hr") +
  scale_color_manual(name = "Rush Hour",
                    values = c("#74add1", "#d73027")) +
  labs(x = "Pickup longitude", y = "Pickup latitude",
       title = "Feb 2015 Taxi Pickups",
       fill = "Pickups / Hr") +
  guides(color = guide_legend(override.aes = list(size=3)),
         alpha = guide_legend(override.aes = list(size=2))) +
  lims(x = c(-74.1, -73.7), y = c(40.5, 40.92)) +
  facet_wrap(rush_hour ~ taxi, 
             scales = "fixed", 
             labeller = as_labeller(facet_names),
             strip.position =  "top") +
  theme_bw() +
  theme(plot.title = element_text(hjust = 0.5)) 
ggsave(filename = "plots/combo_rush_plot.png", plot = combo_plot_rh)