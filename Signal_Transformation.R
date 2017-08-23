#### Signal Transformation ####
#将信号转换成回测格式#
#输入的信号格式InnerCode,Date,LongShort (chr,Date,Num)
#原信号仅为事件发生日期
#将其转化为每日的信号

library(dplyr)
library(lubridate)
library(readr)
library(DBI)
library(rJava)
library(RJDBC)
library(zoo)

#### set path ####
jdbc_path <- "C:/Users/shuorui.zhang/Documents/R_WORKING_DIR/jdbc driver/sqljdbc_6.0/enu/jre8/sqljdbc42.jar"
load_path <- "D:/DATA_NEW/"

#### DB connection ####
drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver", jdbc_path, identifier.quote = "`")    
conn <- dbConnect(drv,"jdbc:sqlserver://192.168.66.12",databaseName="ZSR","shuorui.zhang","Xyinv1704")

#### set parameters ####
# excluding basedate
Days_BeforeEvent <- 0 
Days_AfterEvent <- 14 #事件后持续作用事件，最多持有15天后卖出(包括调仓日)

load(paste0(load_path,"/QT_TradingDayNew_DB.RData"))
#构造任意日期对应的最近的交易日期
QT_TradingDayNew <- QT_TradingDayNew_DB %>% filter(SecuMarket==83,TradingDate>='2007-01-01') %>%
  mutate(TradingDate_=as.Date(ifelse(IfTradingDay==1,TradingDate,NA),origin='1970-01-01')) %>%
  select(TradingDate,IfTradingDay,TradingDate_) %>% arrange(desc(TradingDate)) %>% 
  mutate(LatestTradingDay=as.Date(na.locf(TradingDate_, na.rm = F),origin='1970-01-01')) %>%
  select(TradingDate,IfTradingDay,LatestTradingDay)

NextTradingDay <- QT_TradingDayNew %>% filter(IfTradingDay==1) %>% 
  select(TradingDate) %>% arrange(TradingDate) %>% mutate(NextTradeDay=dplyr::lead(TradingDate)) %>% na.omit() %>% data.frame()

rm(QT_TradingDayNew_DB) 

#读取事件信号
Signal <- read_csv('Contrarian_1pct.csv')
Signal_Event_Raw <- Signal %>% data.frame() %>% 
  filter(LongShort==1) #Long Only

#事件日后一日作为Rebalance Day
Signal_Event_Rebalance <- Signal_Event_Raw %>% left_join(NextTradingDay, by = c('Date'='TradingDate')) %>%
  select(InnerCode,Date=NextTradeDay)

#关联上最近交易日
Signal_Event_Base<- Signal_Event_Rebalance %>% inner_join(QT_TradingDayNew, by = c("Date" = "TradingDate")) %>% 
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
    if(cursor<nrow(Unique_EventDays)){
      #若不是最后一个调仓日，则持仓日需要小于下一个调仓日
      NextRebalanceDay <- Unique_EventDays$EventBaseDay[cursor+1]
      Curr_TradingDay_Base_Sequence <- Curr_TradingDay_Base_Sequence %>% filter(TradingDate<NextRebalanceDay)
      }
    TradingDay_Base_Sequence <- rbind(TradingDay_Base_Sequence,Curr_TradingDay_Base_Sequence)
    
  } else {next}
}

#生成持仓矩阵InnerCode,Date,Weight
Signal_Holdings <- Signal_Event_Base %>% inner_join(TradingDay_Base_Sequence, by = 'EventBaseDay') %>% 
  select(InnerCode, TradingDate) %>% rename(Date = TradingDate) %>% 
  group_by(Date) %>% mutate(Weight=1/n()) %>% ungroup() #赋予等权重
  #后面一段加上是否Rebalance
  # left_join(Signal_Event_Rebalance%>%distinct(Date)%>%mutate(ast=1), by = 'Date') %>%
  # mutate(IfRebalance=ifelse(is.na(ast),0,1)) %>% select(-ast) %>% mutate(InnerCode=as.character(InnerCode))

#输出持仓矩阵信号
write.csv(Signal_Holdings, 'Signal_Contrarian_1pct.csv', row.names = F)


