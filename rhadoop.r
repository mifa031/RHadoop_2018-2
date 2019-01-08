library(rhdfs) 
hdfs.init()     
library(rmr2) 
library(dplyr)
rmr.options(backend = "hadoop")

rm(list=ls())

#################################################################

#                           WEATHER                             # 

#################################################################

files <- hdfs.ls("/user/shcho/data/NYC_central_park_weather/central_park_weather.csv")$file
mr <- mapreduce(input = files[1], input.format = make.input.format(format = "csv", sep=",",
                                                                   stringsAsFactors=F))
res <- from.dfs(mr)
ress <- values(res)
colnames(ress) <- c("STATION","NAME","DATE","AWND","PRCP","SNOW","SNWD","TMAX","TMIN")
weather <- ress[-1,]
weather <- weather[weather$DATE>="2013-01-01" & weather$DATE<="2013-12-31",]
#summary(weather)
########## variables per weather ############
var_weather <- data.frame(weather$PRCP, weather$SNOW)
colnames(var_weather) <- c("PRCP","SNOW")
rain_snow <- data.frame(NULL)
for (i in 1:365){
  if (var_weather[i,1] == 0 &  var_weather[i,2] == 0){
    rain_snow <- rbind(rain_snow, 0)
  }else if (var_weather[i,1] > 0 & var_weather[i,2] == 0){
    rain_snow <- rbind(rain_snow, 1)
  }else{
    rain_snow <- rbind(rain_snow, 2)
  }
}
str(rain_snow)
tmp <- rain_snow %>% filter(X0 == 2)
tmp

str(rain_snow[rain_snow$X0==3,])
str(tmp$dropoff_datetime)
weather.dat <- cbind(as.Date(weather[,3]), rain_snow)
tmp <- weather.dat %>% filter(weather == 2);tmp
str(weather.dat)
colnames(weather.dat) <- c("pickup_date","weather")

#################################################################

#                           TAXI                                # 

#################################################################

files.taxi <- hdfs.ls("/user/shcho/data/taxi/combined")$file
files.taxi


tmp <- values(from.dfs(mapreduce(input = files.taxi[1], 
                                 input.format = make.input.format
                                 (format = "csv", sep=",", stringsAsFactors=F)))) #sampleeeeeeeeeeeeeeeeeeeee


colnames <- as.character(tmp[,1])[-1]
class <- as.character(tmp[,2])[-1]
class[c(6,8,9,10)] <- "numeric"
cbind(colnames, class)

input.format.taxi <- make.input.format( format = "csv", sep = ",",
                                        stringsAsFactors = F, 
                                        col.names = colnames, 
                                        colClasses = class)

files <- files.taxi[-1]

winter <- c(files[1],files[7],files[8],files[9])
winter
notWinter <-c(files[2],files[3],files[4],files[5],files[6],files[10],files[11],files[12])
notWinter


######################################################################

taxi.map <- function(k,v) {
  res<- v %>% filter(trip_distance > 0 & trip_time_in_secs > 0 & mta_tax >= 0 & tip_amount >= 0 & fare_amount >= 0
                     & (rate_code == 1 | rate_code == 2 |rate_code == 3 |rate_code == 4 |rate_code == 5 | rate_code == 6)
                     & passenger_count > 0 & pickup_longitude != 0 & pickup_latitude != 0 
                     & dropoff_latitude != 0 & dropoff_longitude != 0 & total_amount >= 2.5)
  res <- res %>% mutate( speed = trip_distance/(trip_time_in_secs/3600))
  res <- res %>% filter( speed >= 5 & speed <= 85 )
  res <- res %>% mutate( pickup_date = as.Date(pickup_datetime))
  pickup_time<-as.POSIXlt(res$pickup_datetime)$hour
  res<- cbind(pickup_time,res)
  res <- res %>% select(pickup_date, pickup_time, speed)
  res <- merge(res, weather.dat, by="pickup_date")
  res$weather <- as.factor(res$weather)
  peak.dat<- res %>% filter( (pickup_time >= 7 & pickup_time <= 9) 
                             | (pickup_time >= 17 & pickup_time <=19))
  non_peak.dat<- res %>% filter( (pickup_time < 7 | pickup_time > 9) 
                                 & (pickup_time < 17 | pickup_time > 19))
  fit.peak <- lm(formula = speed ~ weather , data = peak.dat)
  X.peak <- model.matrix(fit.peak)
  Y.peak<- as.matrix(peak.dat$speed)
  
  fit.non_peak <- lm(formula = speed ~ weather , data = non_peak.dat)
  X.non_peak <- model.matrix(fit.non_peak)
  Y.non_peak <- as.matrix(non_peak.dat$speed)
  
  peak.XtX<-crossprod(X.peak,X.peak)
  peak.XtY<-crossprod(X.peak,Y.peak)
  peak.YtY<-crossprod(Y.peak,Y.peak)
  
  non_peak.XtX<-crossprod(X.non_peak,X.non_peak)
  non_peak.XtY<-crossprod(X.non_peak,Y.non_peak)
  non_peak.YtY<-crossprod(Y.non_peak,Y.non_peak)
  
  J.peak<-matrix(1, nrow=peak.XtX[1,1], ncol=peak.XtX[1,1])
  J.non_peak<-matrix(1, nrow=non_peak.XtX[1,1], ncol=non_peak.XtX[1,1])
  
  YtJY.peak<-(1/peak.XtX[1,1])*(t(Y.peak)%*%J.peak%*%Y.peak)
  YtJY.non_peak<-(1/non_peak.XtX[1,1])*(t(Y.non_peak)%*%J.non_peak%*%Y.non_peak)
  
  
  keyval(1, 
         list(peak.XtX, peak.XtY, 
              peak.YtY, non_peak.XtX, 
              non_peak.XtY, non_peak.YtY,
              YtJY.peak,YtJY.non_peak))
  
}

reduce.fun <- function(k, v){
  
  peak.XtX = Reduce("+",v[seq_along(v) %% 8 == 1])
  peak.XtY = Reduce("+",v[seq_along(v) %% 8 == 2])
  peak.YtY = Reduce("+",v[seq_along(v) %% 8 == 3])
  
  non_peak.XtX = Reduce("+",v[seq_along(v) %% 8 == 4])
  non_peak.XtY = Reduce("+",v[seq_along(v) %% 8 == 5])
  non_Peak.YtY = Reduce("+",v[seq_along(v) %% 8 == 6])
  
  YtJY.peak = Reduce("+",v[seq_along(v) %% 8 == 7])
  YtJY.non_peak =Reduce("+",v[seq_along(v) %% 8 == 0])
  
  keyval(1,
         list(peak.XtX=peak.XtX, peak.XtY=peak.XtY, 
              peak.YtY=peak.YtY, non_peak.XtX=non_peak.XtX, 
              non_peak.XtY=non_peak.XtY, non_peak.YtY=non_Peak.YtY, 
              YtJY.peak=YtJY.peak , YtJY.non_peak=YtJY.non_peak))

}


#########################################################################

mr <- mapreduce( input = winter , input.format = input.format.taxi,
                 map = taxi.map, reduce=reduce.fun,
                 combine = TRUE)

v <- values(from.dfs(mr))

#########################################################################

peak.beta.hat = solve(v$peak.XtX,v$peak.XtY);peak.beta.hat
non_peak.beta.hat = solve(v$non_peak.XtX,v$non_peak.XtY);non_peak.beta.hat

peak.n = v$peak.XtX[1,1];peak.n
non_peak.n = v$non_peak.XtX[1,1];non_peak.n

SST.peak =v$peak.YtY - v$YtJY.peak;SST.peak
SST.non_peak=v$non_peak.YtY - v$YtJY.non_peak;SST.non_peak

peak.SSE<-v$peak.YtY-t(peak.beta.hat)%*%v$peak.XtY;peak.SSE
non_peak.SSE<-v$non_peak.YtY-t(non_peak.beta.hat)%*%v$non_peak.XtY;non_peak.SSE

peak.SSR<-SST.peak-peak.SSE ;peak.SSR
non_peak.SSR<-SST.non_peak-non_peak.SSE ;non_peak.SSR

peak.MSR<-peak.SSR/(3-1);peak.MSR
peak.MSE<-peak.SSE/(peak.n-3);peak.MSE
peak.F<-peak.MSR/peak.MSE;peak.F
non_peak.MSR<-peak.SSR/(3-1);non_peak.MSR
non_peak.MSE<-peak.SSE/(non_peak.n-3);non_peak.MSE
non_peak.F<-non_peak.MSR/non_peak.MSE;non_peak.F
Rsq_peak<-peak.SSR/SST.peak;Rsq_peak
Rsq_non_peak<-non_peak.SSR/SST.non_peak;Rsq_non_peak