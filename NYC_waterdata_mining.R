---
title: "Water_data_script"
author: "Youfei Zhang"
---
  
###############################################
## special thanks to Prudence Katze 
## who helped me organized codes into functions 
###############################################


## load packages 
require(RODBC)
library(stringr)
library(dplyr)
library(tidyr)
library(data.table)
library(XML)
require(sqldf)   
library(lubridate)

## setups
options(scipen = 999)
tdate <- strftime(Sys.Date(), "_%Y-%m-%d")


##########################################
## Part I: Data Cleaning: clean duplicates

water_data <- read.csv(file = "water_data.csv", header=TRUE, sep=",")

## find out the duplicates 
dup <- water_data[, c('BBL','DATE_READ')] 
dup_readings <- water_data[duplicated(dup) | duplicated(dup, fromLast = TRUE),]
dup_readings <-  dup_readings[order(dup_readings$BBL),]

length(unique(dup_readings$BBL))
unique(dup_readings$TYPE_ID)

dup_CCF <- dup_readings[dup_readings$TYPE_ID == "CCF",]
dup_PBP_MNL <- dup_readings[dup_readings$TYPE_ID != "CCF",]

## for CCFs, combine rows 
dup_CCF <- dup_CCF[rev(order(as.Date(dup_CCF$DATE_READ, format = '%Y-%m-%d'))),]
dup_CCF_sum <- dup_CCF %>%
  group_by(BBL, DATE_READ, NUM_OF_DAYS) %>% 
  summarise(NUM_OF_UNITS = sum(NUM_OF_UNITS))

## clean the residual CCF duplicates
dup_meter <- dup_CCF_sum[,c('BBL','DATE_READ')] 
dup_readings_meter <- dup_CCF_sum[duplicated(dup_meter) | duplicated(dup_meter, fromLast = TRUE),]
DEP_CCFsum_ndupmeter <- anti_join(dup_CCF_sum, dup_readings_meter) # the CCF exclude different meters

## delete duplicates from orginal dataset 
unique_readings <- anti_join(water_data, dup_readings)

## keep only the key columns
unique_readings_simple <- subset(unique_readings, select = c("BBL", "DATE_READ", "NUM_OF_DAYS", "NUM_OF_UNITS"))

## merge cleaned data back
water_clean <- rbind(setDT(unique_readings_simple), setDT(DEP_CCFsum_ndupmeter))


###########################################
## Part II: Data Manipulation - aggregation 

## slice DEP dataframe by the min and max number of days condition

lastread_loop = function(x, min, max) {
  message("starting script")
  slice_number = 1
  
  ## The 1st last reading
  ## the last DATE_READ of all the BBLs
  lastread_BBL <-
    x %>%
    group_by(BBL) %>%
    arrange(desc(as.Date(x$DATE_READ, format = "%m/%d/%y"))) %>%
    slice(slice_number)
  
  ## rename the cols for the convenience to merge tables later
  colnames(lastread_BBL)[names(lastread_BBL) == "DATE_READ"] <-
    "DATE_READ1"
  colnames(lastread_BBL)[names(lastread_BBL) == "NUM_OF_DAYS"] <-
    "NUM_OF_DAYS1"
  colnames(lastread_BBL)[names(lastread_BBL) == "NUM_OF_UNITS"] <-
    "NUM_OF_UNITS1"
  
  message("..first done!..")
  
  lastread_BBL$total_numdays = lastread_BBL$NUM_OF_DAYS1
  
  ## second parts
  
  ## if the condition is met, then subset by BBL
  ## go back to universe, subset the next most recent reading
  
  repeat {
    slice_number = slice_number + 1
    message("starting ", slice_number, " loop")
    
    ## extract rows that match min and max number of days
    nextread_BBL_list <-
      lastread_BBL[lastread_BBL$total_numdays > min &
                     lastread_BBL$total_numdays < max, ]
    
    ## extract the readings that match with the original dataframe
    nextread_BBL_match <-
      water_clean[water_clean$BBL %in% nextread_BBL_list$BBL,]
    
    ## slice the next most recent readings by slice number
    nextread_BBL <-
      nextread_BBL_match %>%
      group_by(BBL) %>%
      arrange(desc(
        as.Date(nextread_BBL_match$DATE_READ, format = "%m/%d/%y")
      )) %>%
      slice(slice_number)
    
    nextread_BBL_length = length(nextread_BBL$BBL)
    
    ## STOP loop if there are no more rows
    if (nextread_BBL_length == 0)
      break
    
    ## get the number of rows for the most recent slice
    ## check to see there are BBLs that still meet the conditions in the next loop
    
    message("there are ", nextread_BBL_length, " rows in this slice")
    
    ## in case of occuring df group
    nextread_BBL = as.data.frame(nextread_BBL)
    
    ## update the old df with new df as cols
    lastread_BBL = merge(lastread_BBL,
                         nextread_BBL,
                         by = "BBL",
                         all.x = TRUE)
    
    lastread_BBL = as.data.frame(lastread_BBL)
    
    ## update the total number of days
    lastread_BBL$total_numdays1 = lastread_BBL$total_numdays
    lastread_BBL$total_numdays <-
      rowSums(lastread_BBL[, c("total_numdays1", "NUM_OF_DAYS")], na.rm = TRUE)
    
    ## rename the cols for the convenience of merging tables later
    colnames(lastread_BBL)[names(lastread_BBL) == "DATE_READ"] <-
      paste0("DATE_READ", slice_number)
    colnames(lastread_BBL)[names(lastread_BBL) == "NUM_OF_DAYS"] <-
      paste0("NUM_OF_DAYS", slice_number)
    colnames(lastread_BBL)[names(lastread_BBL) == "NUM_OF_UNITS"] <-
      paste0("NUM_OF_UNITS", slice_number)
    
    message(slice_number, " done!...")
    
  }
  ## only return the df
  return(lastread_BBL)
  
}

loop_test90days = lastread_loop(water_clean, 0, 90) 
loop_test180days = lastread_loop(water_clean, 90, 180)
loop_test270days = lastread_loop(water_clean, 180, 270)
loop_test360days = lastread_loop(water_clean, 270, 360)


############################################
## Part III: Data Manipulation - weighed-sum

## loop for getting weighed sum of 90, 180, 270, 360 days

## for days over 90, 180, 270, 360 

daysover = function(x, thre, slices) {
  daysoverlist = list()
  
  message("starting script")
  
  # first sum the weights for days over threshold
  daysover_n <- filter(x, total_numdays > thre) 
  
  # create a df that NUM_OF-DAYS1 > threehold
  daysover_ND1 <- filter(daysover_n, NUM_OF_DAYS1 > thre) 
  
  # create columns to match with other data frames later
  daysover_ND1$sumN_1days <- 0  # the sum of n-1 days
  daysover_ND1$Ndays <- thre  # 90 - sumN_1days 
  daysover_ND1$unitsN <- daysover_ND1$NUM_OF_UNITS1 * (daysover_ND1$Ndays/daysover_ND1$NUM_OF_DAYS1)  
  daysover_ND1$weightedsum <- daysover_ND1$unitsN # weighed sum of all period for 90 days
  
  # next step: 
  daysover_ND2 <- filter(daysover_n, NUM_OF_DAYS1 < thre) 
  
  # add first slice to list
  daysoverlist[[1]] = daysover_ND1
  
  # create first part of the loop 
  pastnumdays_1 = paste0("NUM_OF_DAYS1")
  pastnumdays_2 = paste0("")
  secondpart_weightedsum = paste0(thre)
  
  ##############
  message("...part one finished!...")
  ##############
  
  for(i in 2:slices) {
    
    ##############
    message("starting loop ",i)
    ##############
    
    j = i - 1 #for the previous day
    days_colname = paste0("NUM_OF_DAYS",i)
    units_colname = paste0("NUM_OF_UNITS",i)
    
    past_days_colname = paste0("NUM_OF_DAYS",j)
    past_units_colname = paste0("NUM_OF_UNITS",j)
    
    # daysover_ND2_x <- filter(daysover_ND2, NUM_OF_DAYS1 + NUM_OF_DAYS2 > thre)
    daysover_ND2_x <- sqldf(paste0("SELECT * FROM daysover_ND2 WHERE ",pastnumdays_1, " + ", days_colname, " > ",thre, pastnumdays_2))
    
    # mutate(sumN_1days = NUM_OF_DAYS1)
    sumN_1days <- sqldf(paste0("select ", pastnumdays_1, " as sumN_1days, BBL from daysover_ND2_x"))
    daysover_ND2_x <- sqldf(paste0("select * from daysover_ND2_x left join sumN_1days using (BBL)"))
    
    daysover_ND2_x = daysover_ND2_x%>%  
      mutate(Ndays = thre - sumN_1days) %>%
      # mutate(unitsN = NUM_OF_UNITS2 * Ndays/NUM_OF_DAYS2)
      mutate(unitsN = !!sym(units_colname) * Ndays/!!sym(days_colname))
    
    #  mutate(weightedsum = unitsN + NUM_OF_UNITS1 * NUM_OF_DAYS1/thre)
    weightedsum <- sqldf(paste0("select ( unitsN + NUM_OF_UNITS1 * NUM_OF_DAYS1/",
                                secondpart_weightedsum ,") as weightedsum, BBL from daysover_ND2_x"))
    
    daysover_ND2_x <- sqldf(paste0("select * from daysover_ND2_x left join weightedsum using (BBL)"))
    
    # dynamically assign a new name to the dataframe
    # while adding the new df to list
    daysoverlist[[i]] = assign(paste0("daysover_ND2_", j), daysover_ND2_x)
    
    # update past
    pastnumdays_1 = paste0( pastnumdays_1," + ",days_colname)
    pastnumdays_2 = paste0(" AND ",pastnumdays_1, " < ",thre)
    secondpart_weightedsum = paste0(secondpart_weightedsum," + ",units_colname,"*",days_colname,"/",thre)
    
    ##############
    message("loop ",i, " finished!")
    ##############
    
  }
  
  ## only return the df 
  daysover_ws <- do.call("rbind", daysoverlist) 
  
  ## delete intermediate cols in order to rbind with equal & less than threshold later
  daysover_ws <- subset(daysover_ws, select = - c(sumN_1days, Ndays, unitsN))
  
  ## add a cols to distinguish whether the sum days that meet the threshold
  daysover_ws$weight_type = paste("over ", thre)
  return(daysover_ws)
  message("...done!...")
  
}


daysover90 = daysover(loop_test90days, thre = 90, slices = 7)
daysover180 = daysover(loop_test180days, thre = 180, slices = 7) 
daysover270 = daysover(loop_test270days, thre = 270, slices = 5) 
daysover360 = daysover(loop_test360days, thre = 360, slices = 2) 


## for days equal to 90, 180, 270, 360 

daysequal = function(x, thre) {
  
  ## select the df according to the threshold
  dayseuqal_n <- filter(x, total_numdays == thre)
  
  if (thre == 90 || thre == 180){
    dayseuqal_n <- dayseuqal_n  %>%
      mutate(weightedsum =  NUM_OF_DAYS1/total_numdays * NUM_OF_UNITS1 + NUM_OF_DAYS2/total_numdays * NUM_OF_UNITS2 + NUM_OF_DAYS3/total_numdays * NUM_OF_UNITS3 + 
               NUM_OF_DAYS4/total_numdays * NUM_OF_UNITS4 + NUM_OF_DAYS5/total_numdays * NUM_OF_UNITS5 +NUM_OF_DAYS6/total_numdays * NUM_OF_UNITS6 + NUM_OF_DAYS7/total_numdays * NUM_OF_UNITS7)
  }
  
  else if (thre == 270){
    dayseuqal_n <- dayseuqal_n %>%
      mutate(weightedsum =  NUM_OF_DAYS1/total_numdays * NUM_OF_UNITS1 + NUM_OF_DAYS2/total_numdays * NUM_OF_UNITS2 + NUM_OF_DAYS3/total_numdays * NUM_OF_UNITS3 + 
               NUM_OF_DAYS4/total_numdays * NUM_OF_UNITS4 + NUM_OF_DAYS5/total_numdays * NUM_OF_UNITS5)
  }
  
  else if (thre == 360){
    dayseuqal_n <- dayseuqal_n %>%
      mutate(weightedsum =  NUM_OF_DAYS1/total_numdays * NUM_OF_UNITS1 + NUM_OF_DAYS2/total_numdays * NUM_OF_UNITS2)
  }
  
  dayseuqal_n$weight_type = paste("equal to ", thre)
  return(dayseuqal_n)
  
}

dayseuqal90 = daysequal(loop_test90days, thre = 90)
dayseuqal180 = daysequal(loop_test180days, thre = 180)
dayseuqal270 = daysequal(loop_test270days, thre = 270)
dayseuqal360 = daysequal(loop_test360days, thre = 360)


## for days less than 90, 180, 270, 360 
## add a column indicating that the sum_days < threshold: Yes, No

daysless = function(x, thre) {
  
  ## select the df according to the threshold
  daysless_n <- filter(x, total_numdays < thre)
  
  if (thre == 90 || thre == 180){
    
    daysless_n <- daysless_n  %>%
      mutate(weightedsum =  NUM_OF_DAYS1/total_numdays * NUM_OF_UNITS1 + NUM_OF_DAYS2/total_numdays * NUM_OF_UNITS2 + NUM_OF_DAYS3/total_numdays * NUM_OF_UNITS3 + 
               NUM_OF_DAYS4/total_numdays * NUM_OF_UNITS4 + NUM_OF_DAYS5/total_numdays * NUM_OF_UNITS5 +NUM_OF_DAYS6/total_numdays * NUM_OF_UNITS6 + NUM_OF_DAYS7/total_numdays * NUM_OF_UNITS7)
  }
  
  else if (thre == 270){
    daysless_n <- daysless_n %>%
      mutate(weightedsum =  NUM_OF_DAYS1/total_numdays * NUM_OF_UNITS1 + NUM_OF_DAYS2/total_numdays * NUM_OF_UNITS2 + NUM_OF_DAYS3/total_numdays * NUM_OF_UNITS3 + 
               NUM_OF_DAYS4/total_numdays * NUM_OF_UNITS4 + NUM_OF_DAYS5/total_numdays * NUM_OF_UNITS5)
  }
  
  else if (thre == 360){
    daysless_n <- daysless_n %>%
      mutate(weightedsum =  NUM_OF_DAYS1/total_numdays * NUM_OF_UNITS1 + NUM_OF_DAYS2/total_numdays * NUM_OF_UNITS2)
    
  }
  
  daysless_n$weight_type = paste("less than ", thre)
  return(daysless_n)
  
}

daysless90 = daysless(x = loop_test90days, thre = 90)
daysless180 = daysless(x = loop_test180days, thre = 180)
daysless270 = daysless(x = loop_test270days, thre = 270)
daysless360 = daysless(x = loop_test360days, thre = 360)


## combine the tables for 90, 180, 270, 360 seperatetly

water90days <- rbind(daysover90, dayseuqal90, daysless90)
water180days <- rbind(daysover180, dayseuqal180, daysless180)
water270days <- rbind(daysover270, dayseuqal270, daysless270)
water360days <- rbind(daysover360, dayseuqal360, daysless360)


## aggregate 90, 180, 270, 360 into one table to create thresholds

weighedsum90 <- subset(water90days, select = c(BBL, weightedsum, weight_type))
colnames(weighedsum90)[2] <- "water90days"
colnames(weighedsum90)[3] <- "weight_type90"

weighedsum180 <- subset(water180days, select = c(BBL, weightedsum, weight_type))
colnames(weighedsum180)[2] <- "water180days"
colnames(weighedsum180)[3] <- "weight_type180"

weighedsum270 <- subset(water270days, select = c(BBL, weightedsum, weight_type))
colnames(weighedsum270)[2] <- "water270days"
colnames(weighedsum270)[3] <- "weight_type270"

weighedsum360 <- subset(water360days, select = c(BBL, weightedsum, weight_type))
colnames(weighedsum360)[2] <- "water360days"
colnames(weighedsum360)[3] <- "weight_type360"

weighteddwater1 <- merge(weighedsum90, weighedsum180, by = "BBL")
weighteddwater2 <- merge(weighteddwater1, weighedsum270, by = "BBL")
weightedwater <- merge(weighteddwater2, weighedsum360, by = "BBL")

## save the final table 
write.csv(weightedwater, file = paste0( "weightedwater", tdate, ".csv"), row.names = FALSE)


