  library(Quandl)
  library(mvtnorm)
  library(matrixcalc) 
  library(parallel)
  library(foreach)
  library(doParallel)
  library(shrink)
  library(sandwich)
  library(lmtest)
  library(xts)
  options(stringsAsFactors = FALSE)
  #functions
  all_identical <- function(x)  {
    TrueFalse <- vapply(1:(length(x)-1),
                 function(n) identical(x[[n]], x[[n+1]]),
                 logical(1))
    if (all(TrueFalse)) TRUE 
    else FALSE
  }
  
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
    x
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
    x
  }
  
  removeOutliersMAX <- function(x)  {
    y<-sort(x)
    outliers<-floor(length(y)/20)
    if(outliers>0)
    {
      for(i in 1:outliers)
      {
        x[which(x==y[i])]<-y[outliers+1]
        x[which(x==y[length(y)-i+1])]<-y[length(y)-outliers]
      }
    }
    x
  }
  
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
  is.exogenous<-function(x){
    found<-which(types==x)
    if(is.na(found)||length(found)==0) FALSE
    else TRUE
  }
  
  
  
  #excel
  SF1<-read.csv("SF1.csv", stringsAsFactors = FALSE)
  #coverage<-read.csv("SHARADAR-SF1.csv", stringsAsFactors = FALSE)
  SF2<-read.csv("SF2.csv", stringsAsFactors = FALSE)
  #SF3<-read.csv("SF3.csv", stringsAsFactors = FALSE)
  #SF3A<-read.csv("SF3A.csv", stringsAsFactors = FALSE)
  tickers<-read.csv("TICKERS.csv", stringsAsFactors = FALSE)
  #DAILY<-read.csv("DAILY.csv", stringsAsFactors = FALSE)
  SEP<-read.csv("SEP.csv", stringsAsFactors = FALSE)


#quarterly data only
data<-SF1[which(SF1[,2]=="MRQ"), c("ticker", "calendardate", "de", "marketcap", "pb", "shareswa")]


#split data by quarter; cross-sectional data

#get end dates of quarters, only those with more than 4000 tickers
time<-as.data.frame(table(data$calendardate), 
                    stringsAsFactors = FALSE)[which(as.data.frame(table(data$calendardate),
                                                                  stringsAsFactors = FALSE)[,2]>4000), 1]
n = length(time)

#split data in list of the quarters
cross<-vector("list", n)
for(j in 1:n)
{
  cross[[j]] <- na.omit(data[which(data$calendardate==time[j]), ])
}  

#parse date to quarter number
quarter<- function(date){
  if(date<"2008-12-31" || date > time[n]) NA
  else if (date<=time[1]) 1
  else {
    for(i in 2:n)
    {
      if(date>time[i-1]&&date<=time[i]) return(i)
    }
  }  
}


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


#Deal with extra values in SEP
companies<-as.data.frame(unique(data[,1]))
names(companies)<-"ticker"
SEP<-merge.data.frame(SEP, companies)

SEP[,5] <- as.numeric(sapply(SEP$date, quarter))
priceList<-vector("list", n)
for(i in 1:n)
{
  priceList[[i]]<-SEP[which(SEP[, 5]==i), 1:3]
}

#check if there are any NAs in SEP, by extention in priceList as well
stopifnot(!any(is.na(SEP$open)))

#quarterly - for momentum
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


#calculate daily returns
no_cores <- detectCores()

# Initiate cluster
cl <- makeCluster(no_cores)
clusterExport(cl, "priceList")

returnParallel<- parLapply(cl, 1:n, function(i) 
  {
  priceList[[i]]<-priceList[[i]][order(priceList[[i]]$ticker, priceList[[i]]$date),]
  return <- as.numeric(sapply(1:length(priceList[[i]][,1]), function(j){

    if(priceList[[i]]$date[j]==priceList[[i]]$date[1])
    {
      if(i==1) return(0)
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
for (i in 1:n)
{
  priceList[[i]][,4]<-returnParallel[[i]]
  names(priceList[[i]])[4]<- "return"
}
#end daily returns


#industry betas
indret<-read.csv("indret.csv", stringsAsFactors = FALSE)
ref<-c(31, 38, 46, 26, 49, 37,  6, 22, 35, 45,  5, 27,  4, 25, 
       34, 44, 23, 30,  2, 24, 14, 20, 32, 10, 48, 19, 40, 17, 
       33, 18, 36, 28, 41,  9, 16, 43, 13,  1,
        29, 42,  8, 21, 15, 47,  3, 12, 11,  7, 39)
#parse industries from Quandl and fama data library
parse<-vector("character", 49)
for(i in 1:49)
{
parse[i]<-unique(tickers$famaindustry)[ref[i]]
}
fama<-cbind(parse, 1:49)


dates<-unique(SEP$date)
dates<-dates[order(dates)]
#removes any dates that are after time[n]
for(i in length(dates):1)
{
  if(is.na(quarter(dates[i]))==FALSE)
  {
    dates<-dates[-((i+1):length(dates))]
    break()
  }
}

indret[,1]<-as.Date.character(dates)


indret<-cbind(indret, as.numeric(sapply(indret[,1], quarter))) #column 51
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
  b<-c(coef(shrink(l, type = "parameterwise", postfit = FALSE))[2], df$ticker[1])
  return(as.character(b))
  }
  else return(c(NA, NA))
}))
if(ncol(betas)!=2)   {
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

#start computation of exogenous factor using insider shares
SF2$transactionshares[which(is.na(SF2$transactionshares))] = 0
SF2<-merge.data.frame(SF2, companies)
SF2<-SF2[order(SF2$filingdate, SF2$ticker),]

time_frame<-as.data.frame(unique(SF2$filingdate), stringsAsFactors = FALSE)
time_frame[,2]<-sapply(time_frame[,1], quarterExtended)
names(time_frame)<-c("filingdate", "quarter")
SF2<-merge.data.frame(SF2, time_frame)

SF2$securitytitle <- sapply(SF2$securitytitle, is.exogenous)

exo_by_quarter <- lapply(1:length(time_extended), function(i){
  return(SF2[which(SF2$quarter==i & SF2$securitytitle == TRUE), c("ticker", "transactionshares")])
})

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
  regression<-lapply(2:n, function(i){
      reg<-merge.data.frame(exoFactor[[i+15]], indBetasList[[i]])
      reg<-merge.data.frame(reg, cross[[i]][,c(1,3,4,5)])
      reg<-merge.data.frame(reg, qreturns[[i-1]][,c(1, 4)])
      reg[,2]<-normalize(reg[,2])
      for(j in 3:7)
      {
        reg[, j]<-scale(reg[,j])
      }
      names(reg)<-c("ticker", "exo", "beta", "quality", "smb", "hml", "mom")
      return(reg)
  })

#regressions

#dates after first quarter
dates<-unique(SEP$date)
dates<-sort(dates)
dates<-dates[which(dates>time[1] & dates<=time[n])]

  time_series<-as.numeric(sapply(dates, function(date){
    i <- quarter(date)
    df<-merge.data.frame(priceList[[i]][which(priceList[[i]]$date==date), c("ticker", "return")],
                         regression[[i-1]])
    df<-data.frame(df, row.names = "ticker")
    df<-na.omit(df)
    df$return <- scale(df$return)
    l<-lm(return~exo+beta+quality+smb+hml+mom, df)
    #print(date)
    return(as.numeric(coeftest(l, vcovHC(l))[2,1]))
    #print(as.numeric(coeftest(l, vcovHC(l))[2,1]))
   # return(as.numeric(l[["coefficients"]][2]))
  }))
  time_series<-removeOutliersMAX(time_series)
  exoret<-as.xts(cumprod(time_series+1), order.by = as.Date(dates))
  
exogenousReturn<-(exoret-1)*100
plot.xts(exogenousReturn, main="Return of the factor-mimicking portfolio of the exogenous factor", lwd=3)

