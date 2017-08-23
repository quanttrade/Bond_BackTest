#事件性回测#
#输入的信号格式InnerCode,Date,LongShort (chr,Date,Num)

library(dplyr)
library(lubridate)
library(readr)
library(DBI)
library(rJava)
library(RJDBC)
library(zoo)
library(ggplot2)
library(plotrix)

#### set path ####
jdbc_path <- "C:/Users/shuorui.zhang/Documents/R_WORKING_DIR/jdbc driver/sqljdbc_6.0/enu/jre8/sqljdbc42.jar"
load_path <- "D:/DATA_NEW/"

#### DB connection ####
drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver", jdbc_path, identifier.quote = "`")    
conn <- dbConnect(drv,"jdbc:sqlserver://192.168.66.12",databaseName="ZSR","shuorui.zhang","Xyinv1704")

#### set parameters ####
# excluding basedate
Days_BeforeEvent <- 0 
Days_AfterEvent <- 30

load(paste0(load_path,"/QT_TradingDayNew_DB.RData"))
#构造任意日期对应的最近的交易日期
QT_TradingDayNew <- QT_TradingDayNew_DB %>% filter(SecuMarket==83,TradingDate>='2007-01-01') %>%
  mutate(TradingDate_=as.Date(ifelse(IfTradingDay==1,TradingDate,NA),origin='1970-01-01')) %>%
  select(TradingDate,IfTradingDay,TradingDate_) %>% arrange(desc(TradingDate)) %>% 
  mutate(LatestTradingDay=as.Date(na.locf(TradingDate_, na.rm = F),origin='1970-01-01')) %>%
  select(TradingDate,IfTradingDay,LatestTradingDay)

rm(QT_TradingDayNew_DB)

#读取事件信号
Signal_Event_Raw <- read_csv('Contrarian_1pct.csv') %>% data.frame() %>% mutate(InnerCode=as.character(InnerCode))
#Signal_Event_Raw <- Contrarian

#关联上最近交易日
Signal_Event_Base<- Signal_Event_Raw %>% inner_join(QT_TradingDayNew, by = c("Date" = "TradingDate")) %>% 
  group_by(InnerCode,LatestTradingDay) %>% filter(n()==1) %>% ungroup %>% #剔除两个事件调整交易日后同交易日的情况
  rename(EventBaseDay=LatestTradingDay) 

#生成事件前后交易日序列
Unique_EventDays <- Signal_Event_Base %>% distinct(EventBaseDay) %>% arrange(EventBaseDay)

TradingDay_RowNum <- QT_TradingDayNew %>% filter(IfTradingDay==1) %>% arrange(TradingDate) %>% 
  select(TradingDate) %>% mutate(rk=row_number())

ff <- function(x,y){x %>% filter(TradingDate==y) %>% .[,2]}

TradingDay_Base_Sequence <- NULL
for (cursor in 1:nrow(Unique_EventDays)){
  Curr_EventBaseDay <- Unique_EventDays$EventBaseDay[cursor]
  print(Curr_EventBaseDay)
  Curr_TradingDay_Sequence <- TradingDay_RowNum %>% mutate(T_Day=rk-ff(data.frame(TradingDate,rk),Curr_EventBaseDay)) %>% 
    filter(T_Day >= -Days_BeforeEvent, T_Day <=Days_AfterEvent) %>% select(-rk)
  if(nrow(Curr_TradingDay_Sequence)==Days_BeforeEvent+Days_AfterEvent+1){#前后序列数量不足则跳过
    Curr_TradingDay_Base_Sequence <- data.frame(EventBaseDay=Curr_EventBaseDay,Curr_TradingDay_Sequence)
    TradingDay_Base_Sequence <- rbind(TradingDay_Base_Sequence,Curr_TradingDay_Base_Sequence)
  } else {next}
}

#读取日行情
ReturnDaily_Origin <- BondQuote_Orig %>% group_by(InnerCode) %>% arrange(InnerCode,TradingDay) %>% 
  mutate(ReturnDaily=ClosePrice/PrevClosePrice-1) %>% select(-PrevClosePrice) %>%
  filter(!is.na(ReturnDaily))

Return_Bond_All <- Signal_Event_Base %>% inner_join(TradingDay_Base_Sequence, by = 'EventBaseDay') %>% 
  inner_join(ReturnDaily_Origin, by = c('InnerCode','TradingDate'='TradingDay')) %>% 
  mutate(Return_LongShort=LongShort*ReturnDaily) #正常的按前日收盘价计算每日收益
  

#事件后第一天(T+1日)以开盘价买入，之后每天的收益
if(TRUE){
ReturnDaily_Origin <- ReturnDaily_Origin %>% 
  mutate(OpenReturn = ifelse(OpenPrice==0, 0, ClosePrice/OpenPrice - 1)) %>% #计算当日以开盘价买入的日内收益，没量的openprice==0
  filter(!is.na(OpenReturn))
Return_Bond_All <- Return_Bond_All %>% 
  mutate(Return_LongShort=LongShort*ifelse(T_Day==1, OpenReturn, ReturnDaily)) #改写第一天的return
}

#longshort return
xy <- Return_Bond_All %>% 
  left_join(Redemption, by = c('InnerCode','TradingDate'='StartDate')) %>%
  #filter(is.na(RedemptionInd)) %>% #剔除提前兑付的几只券的异常值影响
  #filter(TurnoverValue>0) %>% 
  #filter(LongShort==1) %>%
  group_by(T_Day) %>%
  summarise(Return=mean(Return_LongShort)) %>% 
  arrange(T_Day) %>% filter(T_Day>0) %>% 
  mutate(CumReturn=cumprod(1+Return))

png("Cumulate Return After Event.png", width=600, height = 400)
#累计收益
twoord.plot(lx='T_Day', ly='Return', rx='T_Day', ry='CumReturn',
            data = xy,
            main = paste0('Cumulate Return after Event'),
            xlim=c(-(Days_BeforeEvent+1),(Days_AfterEvent+1)),
            #rylim=c(0.95,1.1),
            lcol=4,rcol=6,
            xlab='Date',rylab='Equal Weight Cumulate Return', ylab='Equal Weight Daily Return',
            type=c('h','l'),
            do.first="plot_bg();grid(col=\"white\",lty=2)")
dev.off()

#long only return
xy_longonly <- Return_Bond_All %>% 
  left_join(Redemption, by = c('InnerCode','TradingDate'='StartDate')) %>%
  #filter(TurnoverValue>0) %>% 
  filter(LongShort==1) %>%
  group_by(T_Day) %>%
  summarise(Return=mean(Return_LongShort)) %>% 
  arrange(T_Day) %>% filter(T_Day>0) %>% 
  mutate(CumReturn=cumprod(1+Return))

png("Cumulate Return After Event Long Only.png", width=600, height = 400)
#累计收益
twoord.plot(lx='T_Day', ly='Return', rx='T_Day', ry='CumReturn',
            data = xy_longonly,
            main = paste0('Cumulate Return after Event Long Only'),
            xlim=c(-(Days_BeforeEvent+1),(Days_AfterEvent+1)),
            #rylim=c(0.95,1.1),
            lcol=4,rcol=6,
            xlab='Date',rylab='Equal Weight Cumulate Return', ylab='Equal Weight Daily Return',
            type=c('h','l'),
            do.first="plot_bg();grid(col=\"white\",lty=2)")
dev.off()

#short only return
xy_shortonly <- Return_Bond_All %>% 
  left_join(Redemption, by = c('InnerCode','TradingDate'='StartDate')) %>%
  filter(TurnoverValue>0) %>% 
  filter(LongShort==-1) %>%
  group_by(T_Day) %>%
  summarise(Return=mean(Return_LongShort)) %>% 
  arrange(T_Day) %>% filter(T_Day>0) %>% 
  mutate(CumReturn=cumprod(1+Return))

png("Cumulate Return After Event Short Only.png", width=600, height = 400)
#累计收益
twoord.plot(lx='T_Day', ly='Return', rx='T_Day', ry='CumReturn',
            data = xy_shortonly,
            main = paste0('Cumulate Return after Event Short Only'),
            xlim=c(-(Days_BeforeEvent+1),(Days_AfterEvent+1)),
            #rylim=c(0.95,1.1),
            lcol=4,rcol=6,
            xlab='Date',rylab='Equal Weight Cumulate Return', ylab='Equal Weight Daily Return',
            type=c('h','l'),
            do.first="plot_bg();grid(col=\"white\",lty=2)")
dev.off()

#return为0的分布情况
Dist <- Return_Bond_All %>% mutate(Volume_Ind=as.factor(ifelse(TurnoverValue==0,1,0)))

png("Distribution of TurnoverValue.png", width=400, height = 300)
qplot(T_Day, data = Dist, geom='histogram', color = Volume_Ind, binwidth=1, main='Distribution of TurnoverValue')
dev.off()

WinRate_LongShort <- Dist %>% filter(T_Day>0) %>% group_by(LongShort,T_Day) %>% 
  summarise(n=n(), n_nonzero = sum(ifelse(TurnoverValue>0,1,0)),
            WinRate = sum(ifelse(Return_LongShort>0,1,0))/n_nonzero) %>% ungroup

WinRate_All <- Dist %>% filter(T_Day>0) %>% mutate(LongShort='All') %>% group_by(LongShort,T_Day) %>% 
  summarise(n=n(), n_nonzero = sum(ifelse(TurnoverValue>0,1,0)),
            WinRate = sum(ifelse(Return_LongShort>0,1,0))/n_nonzero) %>% ungroup

WinRate <- rbind(WinRate_LongShort,WinRate_All)

WinRate_Out <- WinRate %>% select(LongShort,T_Day,WinRate) %>% dcast(LongShort~T_Day) %>% select(-LongShort) %>% sapply(round,2) %>%
  cbind(data.frame(LongShort=c('-1','1','ALL'))) %>% select(LongShort,everything())

write.csv(WinRate_Out,'WinRate.csv',row.names = F)

png("WinRate.png", width=600, height = 300)
qplot(T_Day,WinRate,data=WinRate, geom='line',color=LongShort)
dev.off()

#qplot(T_Day, WinRate, data = WinRate, geom = 'line')











