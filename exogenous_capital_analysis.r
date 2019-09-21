library(mvtnorm)
library(matrixcalc) 
library(parallel)
library(doParallel)
library(shrink)
library(sandwich)
library(lmtest)
library(xts)
options(stringsAsFactors = FALSE)
#functions

normalize <- function(x)  {
  y<-sort(x)
  outliers<-floor(length(y)/40)
  if(outliers>0)
  {
    for(i in 1:outliers)
    {
      x[which(x==y[i])]<-y[outliers+1]
      x[which(x==y[length(y)-i+1])]<-y[length(y)-outliers]
    }
  }
  x<-scale(x)
  return(x)
}

removeOutliers <- function(x)  {
  y<-sort(x)
  outliers<-floor(length(y)/40)
  if(outliers>0)
  {
    for(i in 1:outliers)
    {
      x[which(x==y[i])]<-y[outliers+1]
      x[which(x==y[length(y)-i+1])]<-y[length(y)-outliers]
    }
  }
  return(x)
}

#types of exogenous capital that are in the dataset
types<-c("Common Stock", "Class A Common Stock", "Class B Common Stock", "Common Shares", "COMMON STOCK", 
         "Common Stock par value $0.01 per share", "Common", "common stock", "Ordinary Shares" , 
         "Common Stock $1.00 par value", "Common Units", "Common Stock par value $.01 per share", 
         "Common Stock $0.01 par value", "Class A Common", "Common Stock par value $0.001 per share", 
         "Common Stock $.01 par value", "Common Stock par value $0.01", "Common Stock par value $.01", 
         "Common Stock $.01 Par Value", "Class A common stock", "Restricted Stock", "Common Stock $.10 par value",
         "Common Stock $0.01 Par Value", "Common stock par value $0.01 per share", "Class B Common" ,
         "Common Stock without par value", "Common Stock no par value", "Common shares without par value", 
         "Common Stock $0.001 par value", "Common Stock par value $0.001", "Common Stock No par value", 
         "CBS Class B common stock", "Common Stock $0.10 par value" ,"Class C Capital Stock", "COMMON",
         "Series A Common Stock", "Class A Common Shares $.01 par value per share",
         "Common Stock par value $.10 per share", "Common Stock $0.0001 par value",
         "Common Stock $1 par value", "Common Stock $.01 par value per share", "Common Class A", 
         "Common Stock $0.01 par value per share", "Common Stock $.25 Par Value", 
         "Common Stock $1 23 Par Value", "$5 Par Common Stock")
#determine whether a type of capital is exogenous
is.exogenous<-function(x){
  found<-which(types==x)
  if(is.na(found)||length(found)==0) return(FALSE)
  else return(TRUE)
}


#import datasets from cvs files (downloaded from Quandl)
SF1<-read.csv("SF1.csv", stringsAsFactors = FALSE)  # Core US Fundamentals (to get debt/equity ratio and number of shares)
tickers<-read.csv("TICKERS.csv", stringsAsFactors = FALSE)  # Tickers and Metadata (to get the industy sector) 
DAILY<-read.csv("DAILY.csv", stringsAsFactors = FALSE)  #Daily Metrics (to get daily values for market capitalization and price-to-book ratio)
SEP<-read.csv("SEP.csv", stringsAsFactors = FALSE)  # Equity Prices


#Exract only the necessary data (only quarterly data)
data<-SF1[which(SF1[,2]=="MRQ"), c("ticker", "calendardate", "de", "shareswa")]
data<-as.data.frame(na.omit(data), stringsAsFactors = FALSE)
rm(SF1)
gc()

#get end dates of quarters, only those with more than 4000 tickers
time<-as.data.frame(table(data$calendardate), 
                    stringsAsFactors = FALSE)[which(as.data.frame(table(data$calendardate),
                                                                  stringsAsFactors = FALSE)[,2]>4000), 1]
n = length(time)

#split data in list, indexed by quarter number
cross<-vector("list", n)
for(j in 1:n)
{
  cross[[j]] <- data[which(data$calendardate==time[j]), ]
}  

#parse date to quarter number
quarter<- function(date){
  if(date<="2008-12-31" || date > time[n]) NA
  else if (date<=time[1]) 1
  else {
    for(i in 2:n)
    {
      if(date>time[i-1]&&date<=time[i]) return(i)
    }
  }  
}

#a function, identical to the above, but that accomodates the longer time range of SF2
time_extended <- c("2005-03-31", "2005-06-30", "2005-09-30", "2005-12-31",
                   "2006-03-31", "2006-06-30", "2006-09-30", "2006-12-31",
                   "2007-03-31", "2007-06-30", "2007-09-30", "2007-12-31",
                   "2008-03-31", "2008-06-30", "2008-09-30", "2008-12-31", time)
quarterExtended<- function(date){
  if(date<"2005-01-01" || date > time[n]) NA
  else if (date<=time_extended[1]) 1
  else {
    for(i in 2:length(time_extended))
    {
      if(date>time_extended[i-1]&&date<=time_extended[i]) return(i)
    }
  }  
}


#extract only necessary data and split it by quarter
day[, 1:4]<-na.omit(DAILY[,c("ticker", "date","marketcap", "pb")])
day<-na.omit(day)
y[,5]<-sapply(day$date, quarter)
factors<-lapply(1:n, function(i){
  return(day[which(day[,5]==i), 1:4])
})

rm(DAILY)
gc()


#Deal with extra values in SEP
companies<-as.data.frame(unique(data[,1]))
names(companies)<-"ticker"
SEP<-merge.data.frame(SEP, companies)

#split by quarter; take only opening prices
SEP[,5] <- as.numeric(sapply(SEP$date, quarter))
priceList<-vector("list", n)
for(i in 1:n)
{
  priceList[[i]]<-SEP[which(SEP[, 5]==i), 1:3]
}

#check if there are any NAs in SEP, by extention in priceList as well
stopifnot(!any(is.na(SEP$open)))
dates_all<-unique(SEP$date)
rm(SEP)
gc()

#get quarterly returns - for momentum
qreturns<-vector("list", n)
for(i in 1:n) 
{
  first <- min(priceList[[i]]$date)
  last <- max(priceList[[i]]$date)
  qreturns[[i]]<-merge.data.frame(priceList[[i]][which(priceList[[i]]$date==first), c(1, 3)],
                                  priceList[[i]][which(priceList[[i]]$date==last), c(1, 3)], by = "ticker")
  #calculate return and append it
  qreturns[[i]]<-cbind(qreturns[[i]], as.numeric(apply(qreturns[[i]][, c(2,3)], MARGIN = 1, 
                                                       function(x){
                                                         return((x[2]-x[1])/x[1])
                                                       })))
}


#calculate daily returns as (priceToday-priceYesterday)/priceYesterday
no_cores <- detectCores()

# Initiate cluster
cl <- makeCluster(no_cores)
clusterExport(cl, "priceList")

returnDaily<- parLapply(cl, 1:n, function(i) 
{
  priceList[[i]]<-priceList[[i]][order(priceList[[i]]$ticker, priceList[[i]]$date),]
  return <- as.numeric(sapply(1:length(priceList[[i]][,1]), function(j){
    
    if(priceList[[i]]$date[j]==priceList[[i]]$date[1])
    {
      if(i==1) return(0) #first day of the first quarter, the previous day does not exist in the database
      else
      {
        #yesterday's price is in the previous quarter
        old<-priceList[[i-1]][which(priceList[[i-1]]$ticker==priceList[[i]]$ticker[j]
                                    &priceList[[i-1]]$date==max(priceList[[i-1]]$date)), 3]
        
        if(length(old)==0||old==0) return(0)
        else  return((priceList[[i]]$open[j]-old)/old)
      }
    }
    else
    {
      return((priceList[[i]]$open[j]-priceList[[i]]$open[j-1])/priceList[[i]]$open[j-1])
    }
    
  }))
  return(return)
})

stopCluster(cl)

#append the daily returns to priceList
#also append the other daily metrics (marketcap and pb)
for (i in 1:n)
{
  priceList[[i]]<-priceList[[i]][, -c(5,6)]
  priceList[[i]][,4]<-returnDaily[[i]]
  names(priceList[[i]])[4]<- "return"
  priceList[[i]]<-merge.data.frame(priceList[[i]], factors[[i]])
  names(priceList[[i]])[c(5,6)]<-c("smb", "hml")
}
#end daily returns
rm(returnDaily)
gc()

#get industry returns from Kenneth R. French Data Library - 49 industry portfolios (Daily)
indret<-read.csv("indret.csv", stringsAsFactors = FALSE)

#parse industries from Quandl and french data library
ref<-c(31, 38, 46, 26, 49, 37,  6, 22, 35, 45,  5, 27,  4, 25, 
       34, 44, 23, 30,  2, 24, 14, 20, 32, 10, 48, 19, 40, 17, 
       33, 18, 36, 28, 41,  9, 16, 43, 13,  1,
       29, 42,  8, 21, 15, 47,  3, 12, 11,  7, 39)
parse<-vector("character", 49)
for(i in 1:49)
{
  parse[i]<-unique(tickers$famaindustry)[ref[i]]
}
fama<-cbind(parse, 1:49)



dates<-dates_all[order(dates_all)]
for(i in length(dates):1)
{
  if(is.na(quarter(dates[i]))==FALSE)
  {
    dates<-dates[-((i+1):length(dates))]
    break()
  }
}

#split by quarter
indret[,1]<-as.Date.character(dates)
indret<-cbind(indret, as.numeric(sapply(indret[,1], quarter)))
indreturns<-lapply(1:n, function(i){
  return(indret[which(indret[,51]==i), 1:50])
})

for(i in 1:n)
{
  names(indreturns[[i]])[1]<-"date"
  indreturns[[i]]$date<-as.character(indreturns[[i]]$date)
}


no_cores <- detectCores()

# Initiate cluster
cl <- makeCluster(no_cores)
clusterExport(cl, c("priceList", "fama", "indreturns", "shrink","tickers"))

#calculate industry betas to use in regression
indBetasList<-parLapply(cl, 1:n, function(i){
  priceList[[i]]<-priceList[[i]][order(priceList[[i]]$ticker, priceList[[i]]$date),]
  tick<-unique(priceList[[i]]$ticker)
  betas<-as.data.frame(sapply(tick, function(x)
  {
    df<-merge.data.frame(indreturns[[i]], priceList[[i]][which(priceList[[i]]$ticker==x),], by="date")
    df<-na.omit(df)
    industry<-tickers[which(tickers$ticker==df$ticker[1]),]
    if(is.na(industry$famaindustry[1])==FALSE&&length(df[,1])>20)
    {
      index<-as.numeric(fama[which(fama[,1]==industry$famaindustry[1]),2])
      reg<-as.data.frame(cbind(df$return, df[,index+1]))
      names(reg)<-c("ret", "indust")
      l<-lm(ret ~ indust, reg, x=TRUE, y=TRUE)
      b<-c(coef(shrink(l, type = "parameterwise", postfit = FALSE))[2], df$ticker[1]) #apply bayesian shrinkage
      return(as.character(b))
    }
    else return(c(NA, NA))
  }))
  if(ncol(betas)!=2)  {
    betas<-as.data.frame(t(betas), stringsAsFactors=FALSE)
  }
  
  names(betas)<-c("beta", "ticker")
  betas<-na.omit(betas)
  betas$beta<-as.numeric(as.character(betas$beta))
  return(betas)
}
)

stopCluster(cl)

#end industy betas


#load Core US Insiders
SF2<-read.csv("SF2.csv", stringsAsFactors = FALSE)

#start computation of exogenous factor using insider shares
SF2$transactionshares[which(is.na(SF2$transactionshares))] = 0
SF2<-merge.data.frame(SF2, companies)
SF2<-SF2[order(SF2$filingdate, SF2$ticker),]

time_frame<-as.data.frame(unique(SF2$filingdate), stringsAsFactors = FALSE)
time_frame[,2]<-sapply(time_frame[,1], quarterExtended)
time_frame<-na.omit(time_frame)
names(time_frame)<-c("filingdate", "quarter")
SF2<-merge.data.frame(SF2, time_frame)
SF2$securitytitle <- sapply(SF2$securitytitle, is.exogenous)
exo_by_quarter <- lapply(1:length(time_extended), function(i){
  return(SF2[which(SF2$quarter==i & SF2$securitytitle == TRUE), c("ticker", "transactionshares")])
})
rm(SF2)
gc()


#calculate the exogenous factor as ratio of new transactions for the quarter to the total number of shares
no_cores<-detectCores()
cl<-makeCluster(no_cores)
clusterExport(cl, c("exo_by_quarter", "cross"))

exoFactor <- parLapply(cl, 1:length(exo_by_quarter), function(i){
  insiders<-as.data.frame(unique(exo_by_quarter[[i]]$ticker), stringsAsFactors = FALSE)
  insiders[,2]<-as.numeric(vapply(insiders[,1],function(x){
    
    shares<-sum(exo_by_quarter[[i]][which(exo_by_quarter[[i]]$ticker == x), 2])
    if(i < 17)
    {
      if (length(which(cross[[1]]$ticker == x)) == 0)
        return(NA)
      shares <- shares / cross[[1]]$shareswa[which(cross[[1]]$ticker == x)]
    }   
    else
    {
      if (length(which(cross[[i-16]]$ticker == x)) == 0)
        return(NA)
      shares <- shares / cross[[i-16]]$shareswa[which(cross[[i-16]]$ticker == x)]
    }
    return(shares)
  }, numeric(1)))
  insiders<-na.omit(insiders)
  names(insiders)<-c("ticker", "exogenous")
  return(insiders)
})
stopCluster(cl)

#data for regressions
number_of_delays <-6
regression<-vector("list", number_of_delays)

for(k in 1:number_of_delays)
{
  regression[[k]]<-lapply(2:n, function(i){
    reg<-merge.data.frame(exoFactor[[i+16-k]], indBetasList[[i-1]])
    reg<-merge.data.frame(reg, cross[[i-1]][,c(1,3)])
    reg<-merge.data.frame(reg, qreturns[[i-1]][,c(1, 4)])
    for(j in 2:5)
    {
      reg[, j]<-normalize(reg[,j])
    }
    names(reg)<-c("ticker", "exo", "beta", "quality", "mom")
    return(reg)
  })
}




#regressions

#dates after first quarter
dates<-sort(dates_all)
dates<-dates[which(dates>=time[1] & dates<=time[n])]


exoReturn<-lapply(1:number_of_delays, function(x){
  time_series<-as.numeric(sapply(2:length(dates), function(date_index){
    i <- quarter(dates[date_index])
    prev_i<-quarter(dates[date_index-1])
    df<-merge.data.frame(priceList[[i]][which(priceList[[i]]$date==dates[date_index]), c("ticker", "return")],
                         regression[[x]][[i-1]])
    df<-merge.data.frame(df, 
                         priceList[[prev_i]][which(priceList[[prev_i]]$date==dates[date_index-1]), c("ticker", "smb", "hml")])
    df<-data.frame(df, row.names = "ticker")
    df<-na.omit(df)
    df$smb<-normalize(df$smb)
    df$hml<-normalize(df$hml)
    l<-lm(return~exo+beta+quality+smb+hml+mom, df)
    return(as.numeric(coeftest(l, vcovHC(l))[2,1]))
  }))
  time_series<-removeOutliers(time_series)
  exoret<-as.xts(cumprod(time_series+1), order.by = as.Date(dates[2:length(dates)]))
  return(exoret)
})

number_of_plots <- 4
exogenousReturn<-lapply(1:number_of_plots, function(i){
  return((exoReturn[[i]]-1)*100)
}
)
exogenousCompare<-merge.xts(exogenousReturn[[1]], exogenousReturn[[2]], all=FALSE)
if(number_of_plots>2)
{
  for(i in 3:number_of_plots)
  {
    exogenousCompare <- merge.xts(exogenousCompare, exogenousReturn[[i]], all=FALSE)
  }
}
plot.xts(exogenousCompare, main="Return of the factor-mimicking portfolios of the exogenous factor (in %)",
         col=c("#000000", "#bb1f1f", "#2E29A1","#217633" ), lwd=c(3,2,2,2))
par(font=2)
addLegend("topleft", legend.names = c( "1Q delay; 2.822625",
                                       "2Q delay; 3.38642",
                                       "3Q delay; 2.752286",
                                       "4Q delay; 2.703047"), 
          lty=c(1,1, 1,1), lwd=c(2, 2, 2, 2),
          col=c( "#000000", "#bb1f1f", "#2E29A1","#217633" ), y.intersp=0.5)


library(PerformanceAnalytics)
for (i in 1:number_of_plots) {
  print(SharpeRatio(exogenousReturn[[i]], 0, FUN="StdDev"))
}















