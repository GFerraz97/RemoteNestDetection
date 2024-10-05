library(lookup)
library(dplyr)
library(sp)
library(raster)
library(suntools)

setwd("")

# 1. DATA PREPARATION

  #This script allows to organize different databases from different data repositories,
  #so variables are similarly named and future scripts are cleaner. 
  #This script will also allow to remove outliers and prepare both GPS and ODBA 
  #files for the nest detection script


# All work files must be saved within a folder per individual (i.e. the wd should have folders for each individual)
# The directory should not have other files in the same format


# 1.1. Type of File ----------------------------------------------------------

  # this loop determines the device/type of data (ornitela, druid or movebank) 
  # and format variable names to be similar.Many files can be analyzed within 
  # the same folder. Run this part within each individual folder.

  #If you have files from other repositories, you can adapt this part of the script by changing column names.

  #Main output is a file starting by “prep_” that is the result 
  # of the formatted raw file.

listfiles <-
  list.files(pattern = "*.csv", recursive = TRUE) # CHANGE IF NEEDED

for (i in listfiles) {
  tryCatch({
    db <- read.csv(i)
    
    #Ornitela files generally have one column called "device_id". So, if the file has this column we can assume that the data was retrieved from Ornitrack.
    
    if ("device_id" %in% names(db)) {
      db$logger_type <- "ornitela"
      db$date <- db$UTC_date
      db$time_gmt0 <-
        db$UTC_time #New column indicates the timezone of the data
      db$timestamp_gmt0 <-
        as.POSIXct(db$UTC_datetime, format = "%Y-%m-%d %H:%M:%S") #Format timestamp
      db$X_4326 <-
        db$Longitude #X coordinate. Name of the column indicate the projection system.
      db$Y_4326 <-
        db$Latitude  #Y coordinate. Name of the column indicate the projection system.
      db$sat_count <- db$satcount
      db$speed <- db$speed_km_h
      db$altitude <- db$Altitude_m
      db <-
        db[!names(db) %in% c(
          "Longitude",
          "Latitude",
          "UTC_datetime",
          "UTC_date",
          "UTC_time",
          "satcount",
          "speed_km_h",
          "Altitude_m"
        )] #exclude from the database the columns with these names. Only remain the ones with the new names.
      write.csv(db, paste("prep_", unique(db$device_id), i, sep = ""))
      
    } else{
      #Druid files generally have one column called "UUID". So, if the file has this column we can assume that the data was retrieved from Druid.
      
      if ("UUID" %in% names(db)) {
        db$logger_type <- "druid"
        db$date <-
          substr(db$Collecting.time, 1, 10) #Date. Since the timestamp format has a "T" and a "Z" in the middle, this command will only retrieve the date portion.
        db$time_gmt0 <-
          substr(db$Collecting.time, 12, 19) #Time. Since the timestamp format has a "T" and a "Z" in the middle, this command will only retrieve the time portion.
        db$timestamp_gmt0 <-
          as.POSIXct(db$Collecting.time, format = "%Y-%m-%dT%H:%M:%SZ")
        db$X_4326 <- db$Longitude
        db$Y_4326 <- db$Latitude
        db$hdop <- db$HDOP
        db$sat_count <- db$Satellite.used
        db$speed <- db$Speed
        db$altitude <- db$Altitude
        db$device_id <-
          db$UUID #Transform the UUID field into device_id, to have compatibility between files.
        
        db <-
          db[!names(db) %in% c(
            "Longitude",
            "Latitude",
            "Collecting.time",
            "Satellite.used",
            "HDOP",
            "Speed",
            "Altitude",
            "UUID"
          )]
        write.csv(db, paste("prep_", unique(db$device_id), i, sep = ""))
        
      } else{
        #Movebank files generally have one column called "tag.local.identifier" and are different from the ones above. So, if the file does not match the other two types, we can consider that it might have come from movebank.
        
        db$logger_type <- "movebank"
        db$date <- substr(db$timestamp, 1, 10)
        db$time_gmt0 <- substr(db$timestamp, 12, 16)
        db$timestamp_gmt0 <-
          as.POSIXct(db$timestamp, format = "%Y-%m-%d %H:%M:%S")
        db$X_4326 <- db$location.long
        db$Y_4326 <- db$location.lat
        db$hdop <- db$gps.hdop
        db$sat_count <- db$gps.satellite.count
        db$speed <- db$ground.speed
        db$altitude <- db$height.above.msl
        db$device_id <-
          db$tag.local.identifier #Transform the tag.local.identifier field into device_id, to have compatibility between files.
        
        db <-
          db[!names(db) %in% c(
            "location.long",
            "location.lat",
            "gps.hdop",
            "gps.satellite.count",
            "ground.speed",
            "height.above.msl",
            "tag.local.identifier"
          )]
        write.csv(db, paste("prep_", unique(db$device_id), i, sep = ""))
        
      }
    }
    
    
  }, error = function(e) {
    cat("ERROR :", conditionMessage(e), "\n")
  })
}


# 2. Match ODBA data with GPS data --------------------------------------------

  # This loop matches ODBA and GPS observations to the closest date timestamp.
  # Steps ready for ODBA estimates only (not raw ACC data)

  # ODBA and GPS files must be included in the same folder
  # (i.e. folder "21099" includes GPS21099 and ODBA21099 files)

  # Place these folders and ONLY these folders in the wd.

  # Long time running script.

  # As a result, matches are done for both files.
  # Mean coordinates for each date are included in the ODBA file.
  # Also the difference in time between observation and closest observation in
  # datetime from the other database is included.

listfolders <-
  list.dirs(path = getwd(),
            full.names = TRUE,
            recursive = TRUE)
listfolders<-listfolders[-1]

for (i in listfolders) {
  tryCatch({
    listfiles <- list.files(path=i,pattern = "prep_*")
    
    for (j in listfiles) {
      tryCatch({
        
        db<-read.table(file=paste(i,"/",j, sep=""),header = TRUE,sep=",",colClasses = 'character')    
        
        if ("ODBA" %in% names(db)) {
          #creates the "ODBA" database if there's "ODBA" in the name of the file
          
          ODBA <- db
          
        } else{
          gps <-
            db  #If the name of the file does not have "ODBA", it generates a GPS database
        }
      }, error = function(e) {
        cat("ERROR :", conditionMessage(e), "\n")
      })
    }
    
    ODBAdatetimes <-
      ODBA$timestamp_gmt0 #creates a database with the timestamps of the ODBA file
    ODBA$date <-
      substr(ODBA$timestamp_gmt0, 1, 10) #creates date field in the ODBA database
    gps$ODBA <- NA #creates new column called ODBA in the GPS database
    gps$difftime_ODBAGPS <-
      NA #creates new column for the time difference between the ODBA and GPS records in the GPS database
    
    
    for (k in 1:nrow(gps)) {
      #this loop considers the GPS file
      tryCatch({
        minODBAGPStimerow <-
          which.min(abs(as.numeric(
            as.POSIXct(gps$timestamp_gmt0[k], format = "%Y-%m-%d %H:%M:%S")
          ) - as.numeric(
            as.POSIXct(ODBAdatetimes, format = "%Y-%m-%d %H:%M:%S")
          ))) #Find the row index (minODBAGPStimerow) in the gps data frame where the timestamp has the minimum absolute difference compared to the ODBAdatetimes database.
        gps$ODBA[k] <-
          as.character(ODBA[minODBAGPStimerow, 3]) #includes in the ODBA column the ODBA value of the closest timestamp
        gps$difftime_ODBAGPS[k] <-
          as.character(abs(as.numeric(
            as.POSIXct(gps$timestamp_gmt0[k], format = "%Y-%m-%d %H:%M:%S")
          ) - as.numeric(
            as.POSIXct(ODBA$timestamp_gmt0[minODBAGPStimerow], format = "%Y-%m-%d %H:%M:%S")
          ))) #the command calculates the absolute difference in time between the "timestamp_gmt0" value in the gps data frame and the "timestamp_gmt0" value in the "ODBA" data frame at the row specified by minODBAGPStimerow. The resulting absolute difference is then converted to a character type and assigned to the corresponding cell in the "difftime_ODBAGPS" column of the gps data frame
        
      }, error = function(e) {
        cat("ERROR :", conditionMessage(e), "\n")
      })
    }
    
    gps$ODBA <- as.numeric(gps$ODBA)
    gps$difftime_ODBAGPS <- as.numeric(gps$difftime_ODBAGPS)
    
    write.csv(gps, paste(i,"/gps_", unique(gps$device_id), ".csv", sep = ""))
    
    meangps <-
      aggregate(list(as.numeric(gps$X_4326), as.numeric(gps$Y_4326)), by = list(gps$date), mean) #creates database with the average GPS position per date
    names(meangps) <- c("date", "X_4326", "Y_4326")
    
    ODBA$X_4326 <-
      lookup(ODBA$date, meangps$date, meangps$X_4326) #for each date, it inserts the average coordinate in the ODBA database
    ODBA$Y_4326 <-
      lookup(ODBA$date, meangps$date, meangps$Y_4326) #for each date, it inserts the average coordinate in the ODBA database
    
    
    write.csv(ODBA, paste(i,"/ODBA_", unique(gps$device_id), ".csv", sep =""))
    
  }, error = function(e) {
    cat("ERROR :", conditionMessage(e), "\n")
  })
}

# 3. ADDING INFORMATION ---------------------------------------------------

  # information from dictionary is added. Initial files come from previous step
  # it is considered that the unique ID for each individual (indiv_id) is set 
  # outside this script.

  # It is very important that dictionary is reviewed: that individual and device
  # ids are correct and meaningful
  # pay attention to birds with re-used loggers

  # The local time period (summer or winter) is included and so the local time is calculated. 
  # For this only two countries are considered: Portugal and Spain.

  #FOR ODBA AND GPS FILES

dicc <-
  read.csv("Dictionary.csv", sep = ";") #Change WD of the dictionary

listfolders <-
  list.dirs(path = getwd(),
            full.names = TRUE,
            recursive = TRUE)
listfolders<-listfolders[-1]


# Create database with the dates of hour change to daylight saving period and to winter time. Should be adjusted according to the country policy.

daylight_saving <-
  data.frame(
    c("2019",      "2020",      "2021",      "2022",      "2023",       "2024"),
    c(
      "2019-03-31",
      "2020-03-29",
      "2021-03-28",
      "2022-03-27",
      "2023-03-26",
      "2024-03-31"
    ),
    c(
      "2019-10-27",
      "2020-10-25",
      "2021-10-31",
      "2022-10-30",
      "2023-10-29",
      "2024-10-27"
    )
  )
names(daylight_saving) <- c("year", "spring_date", "autumn_date")

for(i in listfolders){
  tryCatch({
  
    listfiles <-
      list.files(path=i,pattern =paste0(c("ODBA_","gps_"), collapse="|") , recursive = TRUE) # CHANGE IF NEEDED
    
for(k in listfiles) {
  tryCatch({
    db <-
      read.csv(paste(i,k, sep="/"), sep = ",", dec = ".") #Now we will add to each database information from the dictionary. CHANGE sep and dec according to your file
    
    db$sex <-
      lookup(db$device_id,  dicc$UUID,  dicc$Sex)    # ! CHANGE device_id to indiv_id if considered!!!
    db$species <- lookup(db$device_id,  dicc$UUID,  dicc$Species)
    db$country <- lookup(db$device_id,  dicc$UUID,  dicc$Country)
    db$year <- substr(db$date, 1, 4)
    
    db3 <- NULL
    
    #You can jump this step if your timestamps are in the desired timezone!!
    
    #Here we consider data that is at UTC + 0 or GMT +0 and we intend to change according to the country timezone and the daylight saving period
    
    for (j in unique(db$year)) {
      tryCatch({
        db2 <- db[db$year == j, ]
        daylight_saving2 <- daylight_saving[daylight_saving$year == j, ]
        
        db2 <- db2 %>%
          mutate(
            period_time = case_when(
              as.POSIXct(db2$date, format = "%Y-%m-%d") >= daylight_saving2$spring_date &
                as.POSIXct(db2$date, format = "%Y-%m-%d") < daylight_saving2$autumn_date ~ "summer",
              #In this command we will compare the dates of the observations with the dates of change to daylight savind time and we will assign the name "summer" to all the dates with times that correspond to the daylight saving time, and "winter" to all the dates with winter times, accounting for year variation
              as.POSIXct(db2$date, format = "%Y-%m-%d") >= daylight_saving2$autumn_date |
                as.POSIXct(db2$date, format = "%Y-%m-%d") < daylight_saving2$spring_date ~ "winter",
            )
          )
        
        db3 <- rbind(db3, db2)
        
        
      }, error = function(e) {
        cat("ERROR :", conditionMessage(e), "\n")
      })
    }
    
    #Adjust the time to the correct time, according to the country and the period.
  
    db3 <- db3 %>%
      mutate(
        local_time = case_when(
          period_time == "summer" &
            country == "Portugal" ~ as.character(
              as.POSIXct(timestamp_gmt0, format = "%Y-%m-%d %H:%M:%S") + 3600
            ),
          #In summer, Portugal time will be in GMT + 1, so we need to add 1 hour (3600 seconds)
          period_time == "summer" &
            country == "Spain"    ~ as.character(
              as.POSIXct(timestamp_gmt0, format = "%Y-%m-%d %H:%M:%S") + 7200
            ),
          #In summer, Spain time will be in GMT + 2, so we need to add 2 hours (7200 seconds)
          period_time == "winter" &
            country == "Portugal" ~ as.character(as.POSIXct(timestamp_gmt0, format = "%Y-%m-%d %H:%M:%S")),
          #In winter, Portugal time will be in GMT + 0, so we need to maintain the time
          period_time == "winter" &
            country == "Spain"    ~ as.character(
              as.POSIXct(timestamp_gmt0, format = "%Y-%m-%d %H:%M:%S") + 3600
            ),
          #In winter, Spain time will be in GMT + 1, so we need to add 1 hour (3600 seconds)
        )
      )

    write.csv(db3, paste(i,"/dic_", k, sep = ""))
    
  }, error = function(e) {
    cat("ERROR :", conditionMessage(e), "\n")
  })
}
}, error = function(e) {
  cat("ERROR :", conditionMessage(e), "\n")
})
}



# 4. Filter OUTLIERS -------------------------------------------------------------
  
  # Filter outliers and positions linked to large movements
  # Outliers are not discarded, it must be always done on further steps.

  #ONLY FOR GPS DATA!!

listfolders <-
  list.dirs(path = getwd(),
            full.names = TRUE,
            recursive = TRUE)
listfolders<-listfolders[-1]


#When having multiple fixes per burst, only keep the last fix. The following code was designed for Ornitela bursts, that usually have 6 fixes.
#It can also be applied to Druid data and it will remove records that are seconds apart and not according to the settled frequency of data collection

for(i in listfolders){
  tryCatch({
    
    listfiles <-
      list.files(path=i, pattern = "dic_gps*", recursive = TRUE)
    
for (k in listfiles) {
  tryCatch({
    x <- read.csv(paste(i,k, sep="/"))
    x <-
      x[order(x$device_id, x$local_time),]  #order columns according to timestamp
    x$local_time_2 <-
      as.POSIXct(x$local_time, format = "%Y-%m-%d %H:%M:%S") #format timestamp
    x$lag1 <-
      rbind(NA, as.matrix(diff(x$local_time_2))) #estimate the temporal difference for the last entry of the table
    x$lag5 <-
      rbind(NA, NA, NA, NA, NA, as.matrix(diff(x$local_time_2, lag = 5))) #estimate the time difference for the entry 5 lines bellow
    x <- x %>%
      filter(!(lag1 == 1 &
                 (!lag5 == 5))) #eliminate the first 5 wake ups
    
    x$IDorder <- order(x$local_time)
    

    # Remove entries with invalid coordinates
    
    x <- x %>%
      filter(
        !is.na(x$X_4326),
        x$X_4326  >= -90,
        x$X_4326  <= 90,
        !is.na(x$Y_4326),
        x$Y_4326 >= -180,
        x$Y_4326 <= 180
      )
    # Filter outliers considering velocity from previous point
    
    coordinates(x) <- cbind(x$X_4326, x$Y_4326)
    proj4string(x) = CRS("+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0")
    x <-
      spTransform(x, CRS("+init=epsg:25830")) # transforming to 25830 or other UTM system
    x$X_25830 <- coordinates(x)[, 1]
    x$Y_25830 <- coordinates(x)[, 2]
    x <- as.data.frame(x)
    xvel <- tail(x, nrow(x) - 1)
    xvel$distancefrom <-
      sqrt((diff(x$X_25830)) ^ 2 + (diff(x$Y_25830)) ^ 2)
    xvel$difftimefrom <-
      as.numeric(diff(as.numeric(
        as.POSIXct(x$local_time, format = "%Y-%m-%d %H:%M:%S")
      ))) + 1
    xvel$velfrom <- as.numeric(xvel$distancefrom / xvel$difftimefrom)
    x$vel <- lookup(x$IDorder, xvel$IDorder, xvel$velfrom)
    
    xvel$outlier <-
      ifelse(xvel$velfrom > 25 | xvel$X_4326 > 180 |
               xvel$Y_4326 > 90,
             "YES",
             "NO")
    
    x$outliers <- lookup(x$IDorder, xvel$IDorder, xvel$outlier)
    
    #Filter by Altitude and Speed
    #Altitude values should be changed according to the study area. Values in metres.
    
    x <- subset(x, x$altitude > 0)
    
    x <- subset(x, x$altitude < 800)
    
    x <- subset(x, x$speed <= 1)
    
    
    write.csv(x, paste(i,"/filtered_", k, sep = ""))
    
    
  }, error = function(e) {
    cat("ERROR :", conditionMessage(e), "\n")
  })
}
    
  }, error = function(e) {
    cat("ERROR :", conditionMessage(e), "\n")
  })
}

# 5. DAILY PERIODS --------------------------------------------------------

  # This loop firstly consider daily period by calculating sunrise and sunset for each day and location.
  # Then some fixed lags for each period are considered and depending on the sex 
  # of the individual (i.e. 30min before and 90min after sunrise for males).
  # Midday is the middle time between sunrise and sunset.
  # So, each local time is classified into these periods and depending on sex. If
  # within these periods, that time is considered to be during the incubation shift.

  #change lag_secs (window limits) according to species and sex

lag_secs_male_before_sunrise  <- 1800
lag_secs_male_after_sunrise   <- 5400
lag_secs_female_before_midday <- 3600
lag_secs_female_after_midday  <- 3600

  
  #Only for GPS DATA AFTER FILTERING

listfolders <-
  list.dirs(path = getwd(),
            full.names = TRUE,
            recursive = TRUE)
listfolders<-listfolders[-1]

for(i in listfolders){
  tryCatch({
    
listfiles <-
  list.files(path=i,pattern = "filtered_*", recursive = TRUE) 

for (k in listfiles) {
  tryCatch({
    x <- read.csv(paste(i,k, sep="/"))
    x <- x[x$outliers == "NO", ]
    
    x <-
      x[complete.cases(x$X_4326),] #remove ODBA fixes without coordinates
    x <-
      x[complete.cases(x$Y_4326),] #remove ODBA fixes without coordinates
    DB <- NULL
    
    for (j in unique(x$date)) {
      tryCatch({
        x2 <- x[x$date == j, ]
        x2$sunrise <-
          sunriset(
            as.matrix(data.frame(x2$X_4326, x2$Y_4326)),
            as.POSIXct(x2$local_time, format = "%Y-%m-%d %H:%M:%S"), 
            direction = "sunrise",
            POSIXct.out = T
          )$time
        x2$sunset <-
          sunriset(
            as.matrix(data.frame(x2$X_4326, x2$Y_4326)),
            as.POSIXct(x2$local_time, format = "%Y-%m-%d %H:%M:%S"), 
            direction = "sunset",
            POSIXct.out = T
          )$time
        
        #Adjust sunrise time according to country. Time estimated in the IP address timezone. To define windows is best to have this step instead of argument "tz" above.
          #SKIP THIS STEP IF WORKING IN THE SAME COUNTRY
        x2$sunrise <- x2$sunrise + 3600 #IP address in Portugal so we need to add 1 hour for birds in spain
        
        
        #Adjust sunrise time according to country. Time estimated in the IP address timezone. To define windows is best to have this step instead of argument "tz" above.
          #SKIP THIS STEP IF WORKING IN THE SAME COUNTRY
        x2$sunset <- x2$sunset + 3600 #IP address in Portugal so we need to add 1 hour for birds in spain
         
        
        x2$numtime <-
          as.numeric(as.POSIXct(
            as.POSIXct(x2$local_time, format = "%Y-%m-%d %H:%M:%S"),
            format = "%H:%M"
          ))
        x2$num_sunrise <-
          as.numeric(as.POSIXct(x2$sunrise, format = "%H:%M"))
        x2$num_sunset <-
          as.numeric(as.POSIXct(x2$sunset, format = "%H:%M"))
        
        
        x2$num_midday <-
          x2$num_sunrise + (x2$num_sunset - x2$num_sunrise) / 2
        x2$midday <- x2$sunrise + (x2$sunset - x2$sunrise) / 2
        
        x2 <- x2 %>%
          mutate(
            incub_shift = case_when(
              sex == "Male" &
                numtime > num_sunrise - lag_secs_male_before_sunrise &
                numtime < num_sunrise + lag_secs_male_after_sunrise ~ "YES",
              sex == "Female" &
                numtime > num_midday - lag_secs_female_before_midday &
                numtime < num_midday + lag_secs_female_after_midday ~ "YES",
            )
          )
        
        x2$incub_shift <-
          ifelse(is.na(x2$incub_shift), "NO", x2$incub_shift)
        
        DB <- rbind(DB, x2)
        
        
      }, error = function(e) {
        cat("ERROR :", conditionMessage(e), "\n")
      })
    }
    
    write.csv(DB, paste(i,"/shift_", k, sep = ""))
    
  }, error = function(e) {
    cat("ERROR :", conditionMessage(e), "\n")
  })
}

  }, error = function(e) {
    cat("ERROR :", conditionMessage(e), "\n")
  })
}


  #ONLY FOR ODBA FILES
listfolders <-
  list.dirs(path = getwd(),
            full.names = TRUE,
            recursive = TRUE)
listfolders<-listfolders[-1]

for(i in listfolders){
  tryCatch({
    
    listfiles <-
      list.files(path=i,pattern = "dic_ODBA*", recursive = TRUE) 
   
for (k in listfiles) {
  tryCatch({
    x <- read.csv(paste(i,k, sep="/"))
    
    x <-
      x[complete.cases(x$X_4326),] #remove ODBA fixes without coordinates
    x <-
      x[complete.cases(x$Y_4326),] #remove ODBA fixes without coordinates
    DB <- NULL
    
    for (j in unique(x$date)) {
      tryCatch({
        x2 <- x[x$date == j, ]
        x2$sunrise <-
          sunriset(
            as.matrix(data.frame(x2$X_4326, x2$Y_4326)),
            as.POSIXct(x2$local_time, format = "%Y-%m-%d %H:%M:%S"), 
            direction = "sunrise",
            POSIXct.out = T
          )$time
        x2$sunset <-
          sunriset(
            as.matrix(data.frame(x2$X_4326, x2$Y_4326)),
            as.POSIXct(x2$local_time, format = "%Y-%m-%d %H:%M:%S"), 
            direction = "sunset",
            POSIXct.out = T
          )$time
        
        #Adjust sunrise time according to country. Time estimated in the IP address timezone. To define windows is best to have this step instead of argument "tz" above.
        #SKIP THIS STEP IF WORKING IN THE SAME COUNTRY
        x2$sunrise <- x2$sunrise + 3600 #IP address in Portugal so we need to add 1 hour for birds in spain
        
        
        #Adjust sunrise time according to country. Time estimated in the IP address timezone. To define windows is best to have this step instead of argument "tz" above.
        #SKIP THIS STEP IF WORKING IN THE SAME COUNTRY
        x2$sunset <- x2$sunset + 3600 #IP address in Portugal so we need to add 1 hour for birds in spain
        
        
        x2$numtime <-
          as.numeric(as.POSIXct(
            as.POSIXct(x2$local_time, format = "%Y-%m-%d %H:%M:%S"),
            format = "%H:%M"
          ))
        x2$num_sunrise <-
          as.numeric(as.POSIXct(x2$sunrise, format = "%H:%M"))
        x2$num_sunset <-
          as.numeric(as.POSIXct(x2$sunset, format = "%H:%M"))
        
        
        x2$num_midday <-
          x2$num_sunrise + (x2$num_sunset - x2$num_sunrise) / 2
        x2$midday <- x2$sunrise + (x2$sunset - x2$sunrise) / 2
        
        x2 <- x2 %>%
          mutate(
            incub_shift = case_when(
              sex == "Male" &
                numtime > num_sunrise - lag_secs_male_before_sunrise &
                numtime < num_sunrise + lag_secs_male_after_sunrise ~ "YES",
              sex == "Female" &
                numtime > num_midday - lag_secs_female_before_midday &
                numtime < num_midday + lag_secs_female_after_midday ~ "YES",
            )
          )
        
        x2$incub_shift <-
          ifelse(is.na(x2$incub_shift), "NO", x2$incub_shift)
        
        DB <- rbind(DB, x2)
        
        
      }, error = function(e) {
        cat("ERROR :", conditionMessage(e), "\n")
      })
    }
    
    write.csv(DB, paste(i,"/shift_", k, sep = ""))
    

  },
  error = function(e) {
    cat("ERROR :", conditionMessage(e), "\n")
  })
}
    
  },
error = function(e) {
  cat("ERROR :", conditionMessage(e), "\n")
})
}

# 6. ODBA COMPARISON ------------------------------------------------------

  # For each date, mean ODBA for the incubation shift (AvODBA) is calculated as well as
  # mean ODBA for the non incubation shift

  # An ODBA threshold is set and if AvODBA is under this value the date 
  #is considered as possible incubation.

  # If after n consecutive days (object "days") AvODBA is under 
  # the threshold value, then that date is considered as probable incubation

  # In the final results, the ODBA threshold is included and the days used to 
  # calculate probable incubation. As well as the number of points for each date and
  # for each incubation shift within same date.

  #output is a file starting by "meanODBA_"

listfolders <-
  list.dirs(path = getwd(),
            full.names = TRUE,
            recursive = TRUE)
listfolders<-listfolders[-1]

days <- 2 # Optimal periods are 2 or 3 days. Change if necessary

for(i in listfolders){
  tryCatch({
    
    listfiles <-
      list.files(path=i,pattern = "shift_dic*", recursive = TRUE)
    
    for (m in listfiles) {
      tryCatch({
        db1 <- read.csv(paste(i,m, sep="/"))
        
        db1 <- db1[db1$ODBA <= 2000, ]
        
        odba_threshold<-unique(ifelse(db1$sex=="Female",278,286)) #change threshold values according to species and sex
        
        db1$incub_shift <-
          ifelse(is.na(db1$incub_shift), "NO", db1$incub_shift)
        
        
        meanodba <-
          aggregate(list(db1$ODBA), by = list(db1$date, db1$incub_shift), mean)
        names(meanodba) <- c("date", "incub_shift", "mean_ODBA")
        
        meanodba2 <- meanodba[meanodba$incub_shift == "YES", ]
        meanodba2$possible_incub <-NA
        meanodba2$possible_incub <- ifelse(meanodba2$mean_ODBA < odba_threshold, "YES", "NO")
        meanodba2$juliandate <- as.numeric(as.Date(meanodba2$date, format = "%Y-%m-%d"))
        
        rownames(meanodba2) <- c(1:nrow(meanodba2))
        meanodba2$probable_nesting <- NA
        
        
        for (j in 1:nrow(meanodba2)) {
          tryCatch({
            meanodba3 <- meanodba2[c(as.character(seq(j, j + days - 1))), ]
            meanodba3$possible_incub_num <-
              ifelse(meanodba3$possible_incub == "YES", 1, 0)
            meanodba2$probable_nesting[j] <-
              ifelse(sum(meanodba3$possible_incub_num) == days, "YES", "NO")
            
          }, error = function(e) {
            cat("ERROR :", conditionMessage(e), "\n")
          })
        }
        
        meanodba$possible_incub <-
          ifelse(
            meanodba$incub_shift == "YES",
            lookup(meanodba$date, meanodba2$date, meanodba2$possible_incub),
            "not applicable"
          )
        meanodba$probable_nesting <-
          ifelse(
            meanodba$incub_shift == "YES",
            lookup(
              meanodba$date,
              meanodba2$date,
              meanodba2$probable_nesting
            ),
            "not applicable"
          )
        
        count_odba <-
          aggregate(list(db1$ODBA), by = list(db1$date, db1$incub_shift), length)
        names(count_odba) <- c("date", "incub_shift", "count")
        
        meanodba <- merge(meanodba, count_odba, by = 1:2, all.x = TRUE)
        colnames(meanodba)[6] <- "n_points_shift"
        
        count_odba <- aggregate(list(db1$ODBA), by = list(db1$date), length)
        names(count_odba) <- c("date", "count")
        
        meanodba <- merge(meanodba, count_odba, by = 1, all.x = TRUE)
        colnames(meanodba)[7] <- "n_points_date"
        
        meanodba$threshold <- odba_threshold
        meanodba$days_probable <- days
        
        write.csv(meanodba, paste(i,"/meanODBA_",m,sep = "")) 
        
      },
      error = function(e) {
        cat("ERROR :", conditionMessage(e), "\n")
      })
    }
    
  },
  error = function(e) {
    cat("ERROR :", conditionMessage(e), "\n")
  })
}

# 7. Movement COMPARISON ------------------------------------------------------

  # For each date, mean position for the incubation shift is calculated as well as
  # mean position for the non incubation shift

  #Distance the mean position of the certain day and the position of the day before is estimated
  #both for positions within (IWDD) and out the incubation shift

  # A distance threshold is set and if IWDD is under this value a date is considered as possible incubation
  # Distance threshold defined by the error of the device type.
  # If after n consecutive days (object "days") IWDD is under 
  # the threshold value, then that date is considered as probable incubation

  # In the final results the distance threshold is included and the days used to 
  # calculate probable incubation. As well as the number of points for each date and
  # for each incubation shift within same date.

  #output is a file starting by "mean_coords_"

distance_threshold <-
  31 #change according to device. 8 meters for Ornitela and 31 for Druid

days <- 2 # Optimal periods are 2 or 3 days. Change if necessary

listfolders <-
  list.dirs(path = getwd(),
            full.names = TRUE,
            recursive = TRUE)
listfolders<-listfolders[-1]


for(i in listfolders){
  tryCatch({

listfiles <-
  list.files(path=i,pattern = "shift_filtered*", recursive = TRUE) 

for (m in listfiles) {
  tryCatch({
    db1 <- read.csv(paste(i,m, sep="/"), sep = ",", dec = ".")

    db1$incub_shift <-
      ifelse(is.na(db1$incub_shift), "NO", db1$incub_shift)
    
    mean_coords <-
      aggregate(list(db1$coords.x1, db1$coords.x2),
                by = list(db1$date, db1$incub_shift),
                mean)
    
    names(mean_coords) <-
      c("date", "incub_shift", "mean_x_coord", "mean_y_coord")
    mean_coords2 <- mean_coords[mean_coords$incub_shift == "YES", ]
    mean_coords2 <- mean_coords2[order(mean_coords2$date), ]
    
    locations <- mean_coords2[, c("mean_x_coord", "mean_y_coord")]
    locations$prev_x_coord <-
      ave(
        mean_coords2$mean_x_coord,
        FUN = function(x)
          c(NA, head(x,-1))
      )
    locations$prev_y_coord <-
      ave(
        mean_coords2$mean_y_coord,
        FUN = function(x)
          c(NA, head(x,-1))
      )
    locations$distance <-
      sqrt((locations$mean_x_coord - locations$prev_x_coord) ^ 2 +
             (locations$mean_y_coord - locations$prev_y_coord) ^
             2
      )
    mean_coords2$distance <- locations$distance
    mean_coords2$possible_incub <-
      ifelse(mean_coords2$distance < distance_threshold, "YES", "NO")
    mean_coords2$juliandate <-
      as.numeric(as.Date(mean_coords2$date, format = "%Y-%m-%d"))
    
    rownames(mean_coords2) <- c(1:nrow(mean_coords2))
    mean_coords2$probable_nesting <- NA
    
    for (j in 1:nrow(mean_coords2)) {
      tryCatch({
        mean_coords3 <- mean_coords2[c(as.character(seq(j, j + days - 1))), ]
        mean_coords3$possible_incub_num <-
          ifelse(mean_coords3$possible_incub == "YES", 1, 0)
        mean_coords2$probable_nesting[j] <-
          ifelse(sum(mean_coords3$possible_incub_num) == days,
                 "YES",
                 "NO")
        
      }, error = function(e) {
        cat("ERROR :", conditionMessage(e), "\n")
      })
    }
    
    
    mean_coords$possible_incub <-
      ifelse(
        mean_coords$incub_shift == "YES",
        lookup(
          mean_coords$date,
          mean_coords2$date,
          mean_coords2$possible_incub
        ),
        "not applicable"
      )
    mean_coords$probable_nesting <-
      ifelse(
        mean_coords$incub_shift == "YES",
        lookup(
          mean_coords$date,
          mean_coords2$date,
          mean_coords2$probable_nesting
        ),
        "not applicable"
      )
    mean_coords$distance <-
      ifelse(
        mean_coords$incub_shift == "YES",
        lookup(
          mean_coords$date,
          mean_coords2$date,
          mean_coords2$distance
        ),
        "not applicable"
      )
    
    count_GPS <-
      aggregate(list(db1$X_25830), by = list(db1$date, db1$incub_shift), length)
    names(count_GPS) <- c("date", "incub_shift", "count")
    
    mean_coords <-
      merge(mean_coords, count_GPS, by = 1:2, all.x = TRUE)
    colnames(mean_coords)[8] <- "n_points_shift"
    
    count_GPS <-
      aggregate(list(db1$X_25830), by = list(db1$date), length)
    names(count_GPS) <- c("date", "count")
    
    mean_coords <- merge(mean_coords, count_GPS, by = 1, all.x = TRUE)
    colnames(mean_coords)[9] <- "n_points_date"
    
    mean_coords$threshold <- distance_threshold
    mean_coords$days_probable <- days

    write.csv(
      mean_coords,
      paste(i,
        "/mean_coords_",
        m,
        sep = ""
      )
    )
    
  },
  error = function(e) {
    cat("ERROR :", conditionMessage(e), "\n")
  })
}

  },
error = function(e) {
  cat("ERROR :", conditionMessage(e), "\n")
})
}


# 8. Merge Data; Detection based on ODBA and GPS ------------------------------------------------------

  # For each date, it considers movement and ODBA data  
  #A certain day will be considered as a yes (possible incubation) if movement and ODBA data have an Yes. 
  #A probable nest will be consider if this pattern is consistent for more than x consecutive days

  #output is a file starting by "Merge_ODBA_GPS_"

days <- 2 # Optimal periods are 2 or 3 days. Change if necessary

listfolders <-
  list.dirs(path = getwd(),
            full.names = TRUE,
            recursive = TRUE)
listfolders<-listfolders[-1]


for(i in listfolders){
  tryCatch({
    
    gpsfile<-list.files(path=i,pattern = "mean_coords*", recursive = TRUE) 
    odbafile<-list.files(path=i,pattern = "meanODBA*", recursive = TRUE)
    
    File_GPS <- read.csv(paste(i,gpsfile, sep="/"), sep = ",", dec = ".")
    File_ODBA <- read.csv(paste(i,odbafile, sep="/"), sep = ",", dec = ".")
    File_GPS <- File_GPS[File_GPS$incub_shift == "YES", ]
    File_ODBA <- File_ODBA[File_ODBA$incub_shift == "YES", ]
    
    merged_data <- merge(File_GPS, File_ODBA, by = "date", all = TRUE)
    
    #Create a new dataframe with the columns from GPS and ODBA data
    
    File_ALL <- data.frame(
      date = merged_data$date,
      incub_shift.x = merged_data$incub_shift.x,
      distance = merged_data$distance,
      mean_ODBA = merged_data$mean_ODBA,
      n_points_shift_GPS = merged_data$n_points_shift.x,
      n_points_shift_ODBA = merged_data$n_points_shift.y,
      n_points_date_GPS = merged_data$n_points_date.x,
      n_points_date_ODB = merged_data$n_points_date.y,
      threshold_GPS = merged_data$threshold.x,
      threshold_ODBA = merged_data$threshold.y)
    
    File_ALL$possible_incub_GPS <-
      ifelse( File_ALL$incub_shift == "YES",
              lookup(File_ALL$date, File_GPS$date, File_GPS$possible_incub),
              "not applicable")
    
    File_ALL$possible_incub_ODBA <-
      ifelse(
        File_ALL$incub_shift == "YES",
        lookup(File_ALL$date, File_ODBA$date, File_ODBA$possible_incub),
        "not applicable")
    
    #create new column with new possible day based on the values of the ODBA and GPS
    
    File_All2 <- File_ALL %>%
      mutate(
        possible_incub_total = ifelse(
          File_ALL$possible_incub_ODBA == "YES" &
            File_ALL$possible_incub_GPS == "YES",
          "YES",
          "NO"))
    
    #Infer probable nesting
    
    File_All2$juliandate <-
      as.numeric(as.Date(File_All2$date, format = "%Y-%m-%d"))
    
    rownames(File_All2) <- c(1:nrow(File_All2))
    File_All2$probable_nesting <- NA
    
    for (j in 1:nrow(File_All2)) {
      tryCatch({
        File_All3 <- File_All2[c(as.character(seq(j, j + days - 1))), ]
        File_All3$possible_incub_num <-
          ifelse(File_All3$possible_incub_total == "YES", 1, 0)
        File_All2$probable_nesting[j] <-
          ifelse(sum(File_All3$possible_incub_num) == days, "YES", "NO")
        
      }, error = function(e) {
        cat("ERROR :", conditionMessage(e), "\n")
      })
    }
    
    File_All2$juliandate <- NULL
    File_All2$days_probable <- days
    
    write.csv(File_All2,paste(i,"/Merge_ODBA_GPS_", sub('.*/', '', i),".csv",sep = ""  ))
    
  },
  error = function(e) {
    cat("ERROR :", conditionMessage(e), "\n")
  })
}
