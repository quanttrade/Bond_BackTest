# Backtest, buy and hold
# Signal Format (InnerCode, Date, Weight) 
# 每天的目标持仓，实际上并不是每天的，中间可能会有空档
# 每天都要检查是否有信号，如果持仓权重有变化，则就是一个调仓日

##### 说明 #####
# 0、此回测是一个非常理想化的回测，即不考虑交易额的问题，总假设可以随意金额买入、调仓、卖出
# 1、对于PR债，前收盘价已经相应调整过，所以计算当日收益没有问题，但由于会提前兑付本金，造成相应债券金额下降，此回测对此不作考虑
# 2、假设没有最小购买份额，可以按需要的资金全部买入债券



#### load libraries   ####
library(dplyr)
library(lubridate)
library(readr)
library(DBI)
library(rJava)
library(RJDBC)
library(zoo)
library(ggplot2)

#### set path ####
jdbc_path <- "C:/Users/shuorui.zhang/Documents/R_WORKING_DIR/jdbc driver/sqljdbc_6.0/enu/jre8/sqljdbc42.jar"
load_path <- "D:/DATA_NEW/"
signal_path <- "D:/BackTest_Framework/SignalPool_Bonds/"

#### DB connection ####
drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver", jdbc_path, identifier.quote = "`")    
conn <- dbConnect(drv,"jdbc:sqlserver://192.168.66.12",databaseName="ZSR","shuorui.zhang","Xyinv1704")

#### set parameters ####
StartDate_Backtest <- ymd('2015-04-22')
EndDate_Backtest <- ymd('2017-05-19') 
Interval_Backtest <- 'day' #调仓周期，日调仓
TRCost <- 0 #交易成本设置为0，暂时不考虑交易成本
Cash_Initial <- 1000000 #初始资金设定
Trade_Ratio <- 0.95 #资金当中的可投资百分比，即每次调仓时，用于投资的资金比例

#### set signal info ####
Signal_Series <- 'Contrarian'
Signal_Name <- 'Contrarian_AvgDiff_NoVol_5pct'

#### load functions ####
source('functions.R', encoding = 'UTF-8')

#### load data ####
load(paste0(load_path,"/QT_TradingDayNew_DB.RData"))

#### main ####
#rebalance date#
RebalanceDate_Backtest <- QT_TradingDayNew_DB %>% filter(SecuMarket==83, IfTradingDay==1) %>% 
  filter(TradingDate >= StartDate_Backtest, TradingDate <= EndDate_Backtest) %>% 
  arrange(TradingDate) %>% mutate(BuyDate = dplyr::lag(TradingDate)) %>% na.omit() %>% 
  select(BuyDate, SellDate = TradingDate)

#load Signal
Signal_Raw <- read_csv(file = paste0(signal_path, Signal_Series, '/', Signal_Name, '.csv')) %>%
  data.frame() %>% mutate(InnerCode=as.character(InnerCode))

#Signal_Raw <- read_csv('Signal_Contrarian_5pct.csv') %>% data.frame() %>% mutate(InnerCode=as.character(InnerCode))

#添加辅助信号，使得每一期都至少有一个信号，每天都要检查是否有信号，如果持仓权重有变化，则就是一个调仓日
Signal_Assist <- RebalanceDate_Backtest %>% select(BuyDate) %>% mutate(InnerCode = 'AS0000', Weight =0) %>% 
  select(InnerCode, Date=BuyDate, Weight)

Signal_Total <- rbind(Signal_Raw, Signal_Assist)

#得到Rebalance序列


#读入行情数据#
BondQuote_Orig <- dbGetQuery(conn,paste0("SELECT t2.SecuCode,t.InnerCode,t2.SecuAbbr,t.TradingDay,t.PrevClosePrice,t.ClosePrice,t.OpenPrice
                             ,t.LowPrice,t.HighPrice,t.TurnoverValue,t.TurnoverVolume,t.TurnoverDeals,
                             t1.BondTypeLevel1Desc AS BondType, t1.BondNature, t4.IssuerNature, t3.ValueCleanPrice
                             FROM jydb.dbo.QT_DailyQuote t
                             LEFT JOIN jybond.dbo.Bond_Code t1
                             ON t.InnerCode=t1.InnerCode
                             LEFT JOIN jydb.dbo.SecuMain t2
                             ON t.InnerCode=t2.InnerCode
                             LEFT JOIN jybond.dbo.Bond_CSIValuation t3
                             ON t.InnerCode=t3.InnerCode AND t.TradingDay=t3.EndDate
                             LEFT JOIN jybond.dbo.Bond_BasicInfo t4
                             ON t.InnerCode=t4.InnerCode 
                             WHERE 
                             t2.SecuCategory IN ( 6, 7, 11, 28) 
                             /*6国债,7金融债,11企业债券,28地方政府债;9可转债,29可交换公司债*/
                             AND t2.SecuMarket IN ( 83, 90 )
                             AND t.TradingDay>='",StartDate_Backtest,"' AND t.TradingDay<='",EndDate_Backtest,"'")) %>% 
  mutate(TradingDay=as.Date(TradingDay),SecuCode=as.character(SecuCode),InnerCode=as.character(InnerCode))

#裁剪行情数据，仅保留有信号的债券的行情，加速回测
BondQuote_Cut_All <- BondQuote_Orig %>% filter(InnerCode %in% distinct(Signal_Total,InnerCode)$InnerCode) %>%
  mutate(ReturnDaily=ClosePrice/PrevClosePrice-1)

Cash <- NULL
Real_Position <- NULL
for(cursor in 1:nrow(RebalanceDate_Backtest)){ 
  print(cursor)
  TradeDate_ <- RebalanceDate_Backtest$BuyDate[cursor]
  BondQuote_Cut_ <- BondQuote_Cut_All %>% filter(TradingDay==TradeDate_) #裁剪行情数据，加速回测
  if(cursor ==1 ){#第一期单独处理
    Real_Position[[cursor]] <- Signal_Total %>% filter(Date==TradeDate_) %>% 
      left_join(BondQuote_Cut_, by = c('InnerCode','Date'='TradingDay')) %>% #关联上当日行情计算购买量
      #Asset为根据Weight分配的资金，Shares为根据收盘价买入后持有的股数
      mutate(Asset=Weight*Cash_Initial*Trade_Ratio,Shares=Asset/ClosePrice,Shares=ifelse(is.na(Shares),0,Shares)) %>% 
      select(InnerCode,Date,Weight,Asset,Shares,everything()) 
    Cash[cursor] <- Cash_Initial - sum(Real_Position[[cursor]]$Asset) #购买债券后的剩余资金
  } else {#第二期开始
    Prev_Position_ <- Real_Position[[cursor-1]] %>% arrange(InnerCode) 
    Target_Position_ <- Signal_Total %>% filter(Date==TradeDate_) %>% arrange(InnerCode)
    #前后两期持仓权重
    Position_Matrix <- rbind(distinct(Prev_Position_,InnerCode),distinct(Target_Position_,InnerCode)) %>% distinct(InnerCode) %>%
      left_join(Prev_Position_[c('InnerCode','Weight')], by = 'InnerCode') %>% #关联上前一期的持仓权重
      rename(Weight_LT=Weight) %>%
      left_join(Target_Position_[c('InnerCode','Weight')], by = 'InnerCode') %>%  #关联上当前一期的持仓权重
      mutate(Weight_LT=ifelse(is.na(Weight_LT),0,Weight_LT), Weight=ifelse(is.na(Weight),0,Weight)) #NA->0
    
    if(sum(Position_Matrix$Weight_LT!=Position_Matrix$Weight)==0){
      #如果后一期持仓权重和前一期相同，则不调仓，只对已有持仓的价值进行重新评估
      Real_Position[[cursor]] <- Signal_Total %>% filter(Date==TradeDate_) %>% 
        left_join(Prev_Position_[c('InnerCode','Asset','Shares')], by = c('InnerCode')) %>%
        left_join(BondQuote_Cut_, by = c('InnerCode','Date'='TradingDay')) %>% #关联上当日行情，对于PR债，前收盘价已经调整
        mutate(Asset=Asset*(1+ifelse(is.na(ReturnDaily),0,ReturnDaily))) %>% #计算当日资金收益
        select(InnerCode,Date,Weight,Asset,Shares,everything()) 
      Cash[cursor] <- Cash[cursor-1]
    } else{#如果后一期持仓权重和前一期不同，则进行调仓
      #先计算调仓时的总资产，然后按照总资金按权重进行分配，得到目标持仓
      #最后根据目标持仓和原来持仓的差额进行先卖后买
      Prev_Position_Today <- Prev_Position_[c('InnerCode','Weight','Asset','Shares')] %>% 
        mutate(Date=TradeDate_) %>% select(InnerCode,Date,everything()) %>% #日期更改为当期日期，用于关联当期行情
        left_join(BondQuote_Cut_, by = c('InnerCode','Date'='TradingDay')) %>% 
        mutate(Asset=Asset*(1+ifelse(is.na(ReturnDaily),0,ReturnDaily))) 
      
      Asset_Cap <- sum(Prev_Position_Today$Asset) + Cash[cursor-1] #调仓日调仓时的总资金(实际能使用的要再乘Trade_Ratio)
      
      Target_Position_ <- Target_Position_ %>% 
        left_join(BondQuote_Cut_, by = c('InnerCode','Date'='TradingDay')) %>% #关联上当日行情，计算购买量 
        #Asset为根据Weight分配的资金，Shares为根据收盘价买入后持有的股数
        mutate(Asset=Weight*Asset_Cap*Trade_Ratio,Shares=Asset/ClosePrice,Shares=ifelse(is.na(Shares),0,Shares)) %>% 
        select(InnerCode,Date,Weight,Asset,Shares,everything()) 
      
      Total_Securities <- rbind(distinct(Prev_Position_,InnerCode),distinct(Target_Position_,InnerCode)) %>% distinct(InnerCode)
      Real_Position[[cursor]] <- Total_Securities %>% 
        #此处关联上一期是为了方便计算手续费，以及计算净买入，当前不考虑手续费的情况下，引入上一期无用
        left_join(Prev_Position_[c('InnerCode','Weight','Shares')], by = 'InnerCode') %>%
        rename(Weight_LT = Weight, Shares_LT = Shares) %>% 
        left_join(Target_Position_[c('InnerCode','Date','Weight','Shares')], by = 'InnerCode') %>%
        left_join(BondQuote_Cut_, by = c('InnerCode', 'Date'='TradingDay')) %>%
        mutate(Asset=Shares*ClosePrice,Asset=ifelse(is.na(Asset),0,Asset)) %>%
        select(-Weight_LT,-Shares_LT) %>%
        select(InnerCode,Date,Weight,Asset,Shares,everything()) %>% 
        filter(!is.na(Date)) #剔除上期持有，本期没有的证券
      
      Cash[cursor] <- Asset_Cap - sum(Real_Position[[cursor]]$Asset) #调仓日剩余现金
      
    }
  }
}

#合并每期的持仓
Total_Positions <- bind_rows(Real_Position) 

#合并资金曲线,并计算最大回撤
Total_Asset <- Total_Positions %>%  group_by(Date) %>% summarise(Asset_Securities = sum(Asset)) %>% 
  select(Date,Asset_Securities) %>% ungroup %>% 
  arrange(Date) %>% cbind(.,Cash) %>% #关联上现金
  mutate(Asset_Total = Asset_Securities + Cash) %>% 
  mutate(MaxDD = maxDrawdown(Asset_Total)) %>% #计算最大回撤
  #计算当日收益
  mutate(prev_Asset = lag(Asset_Total), Term_Return = Asset_Total/prev_Asset -1, Term_Return=ifelse(is.na(Term_Return),0,Term_Return))%>%
  select(-prev_Asset)

##计算IR, benchmark=0
IR <- Total_Asset %>% arrange(Date) %>%
  summarise(Years = round(as.numeric(EndDate_Backtest - StartDate_Backtest, units='days')/365, 2),
            denominator = round(sqrt(250), 4),
            AnnualizedReturn = round((Asset_Total[n()]/Asset_Total[1])^(1/Years) - 1, 4),
            AnnualizedVol = round(sd(Term_Return)*denominator,4)
  ) %>%
  mutate(IR = round(AnnualizedReturn/AnnualizedVol, 2))

Graph_Total_Asset <- ggplot(data = Total_Asset)+
  geom_line(aes(x=Date, y= Asset_Total/Asset_Total[1]))+
  labs(title = '累计收益曲线',
       x = 'Time Line',
       y = 'Cumulative Return')

####  output results   ####
Path_BacktestResult <- paste0('BacktestResult/', Signal_Series, '/', Signal_Name, "/", Sys.Date(), "_", format(Sys.time(), "%H%M%S"))
dir.create(Path_BacktestResult, recursive = T)

BackTest_Info <- paste0("Signal Series: ", Signal_Series, "\n",
                        "Signal: ", Signal_Name, "\n",
                        "Report Generation Time: ", Sys.time(), "\n",
                        "Start Date: ", StartDate_Backtest, "\n",
                        "End Date: ", EndDate_Backtest, "\n",
                        "Rebalance Interval: ", Interval_Backtest, "\n"
                        )

write(BackTest_Info, file = paste0(Path_BacktestResult, paste0("/BackTest_Info_",Interval_Backtest,".txt")))

write_csv(IR, path = paste0(Path_BacktestResult, '/IR.csv'))

write_csv(Total_Asset, path = paste0(Path_BacktestResult, '/Total_Asset.csv'))

write_excel_csv(Total_Positions, path = paste0(Path_BacktestResult, '/Total_Positions.csv'))

#收益曲线
png(filename = paste0(Path_BacktestResult,
                      '/Graph_Total_Asset.png'),
    width = 700, height = 400)
print(Graph_Total_Asset)
dev.off()








