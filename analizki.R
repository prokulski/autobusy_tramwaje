library(tidyverse)
library(lubridate)

# początek zbierania danych
start_time <- make_datetime(year(today()), month(today()), day(today()), 11, 27)

db <- src_sqlite("autobusy_i_tramwaje.sqlite")

# liczba danych zebranych wg typu
total_data <- bind_rows(db %>%
                          tbl("buses") %>%
                          distinct() %>%
                          collect() %>%
                          mutate(time = ymd_hms(time)) %>%
                          filter(between(.$time, start_time,  ceiling_date(now(), "hour") + hours(1))) %>%
                          mutate(f_time = floor_date(time, unit = "minute")) %>%
                          distinct() %>%
                          mutate(type = "bus"),
                        db %>%
                          tbl("trams") %>%
                          distinct() %>%
                          collect() %>%
                          mutate(time = ymd_hms(time)) %>%
                          filter(between(.$time, start_time, ceiling_date(now(), "hour") + hours(1))) %>%
                          mutate(f_time = floor_date(time, unit = "minute")) %>%
                          distinct() %>%
                          mutate(type = "tram"))

total_data %>%
  count(f_time, type) %>%
  ggplot() +
  geom_point(aes(f_time, n, color = type))


# liczba unikalnych linii jeżdżących po mieście
total_data %>%
  distinct(type, f_time, line) %>%
  count(f_time, type) %>%
  ggplot() +
  geom_point(aes(f_time, n, color = type))





line_types <- total_data %>% distinct(line, type)


library(geosphere)

predkosc_chwilowa <- total_data %>%
  group_by(line, brigade) %>%
  arrange(time) %>%
  mutate(prev_lat = lag(lat),
         prev_long = lag(long),
         prev_time = lag(time)) %>%
  drop_na() %>%
  ungroup() %>%
  mutate(d_time = as.numeric(time - prev_time, "hours")) %>%
  filter(d_time > 0) %>%
  rowwise() %>%
  mutate(distance = distHaversine(c(prev_long, prev_lat), c(long, lat), r = 6378.137)) %>%
  ungroup() %>%
  mutate(v = distance/d_time) %>%
  filter(v <= 120) %>%
  select(line, brigade, time, v)


# średnia prędkość wg pory dnia
predkosc_chwilowa %>%
  filter(v <= 120) %>%
  mutate(f_time = floor_date(time, unit = "15 minutes")) %>%
  left_join(line_types, by = "line") %>%
  group_by(f_time, type) %>%
  summarise(v_mean = mean(v)) %>%
  ungroup() %>%
  ggplot() +
  geom_col(aes(f_time, v_mean, fill = type), position = position_dodge())


# średnia prędkość wg linii
predkosc_chwilowa %>%
  filter(v <= 120) %>%
  group_by(line) %>%
  summarise(v_mean = mean(v)) %>%
  ungroup() %>%
  left_join(line_types, by = "line") %>%
  ggplot() +
  geom_point(aes(line, v_mean, fill = type))
