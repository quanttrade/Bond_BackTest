# Backtest, buy and hold
# Signal Format (InnerCode, Date, Weight) 
# 每天的目标持仓，实际上并不是每天的，中间可能会有空档
# 每天都要检查是否有信号，如果持仓权重有变化，则就是一个调仓日

##### 说明 #####
# 1、对于PR债，前收盘价已经相应调整过，所以计算当日收益没有问题，但由于会提前兑付本金，造成相应债券金额下降，此回测对此不作考虑
# 2、假设没有最小购买份额，可以按需要的资金全部买入债券

# 20170901新增说明
# 当期目标持仓和上一期发生变化才调仓，调仓时考虑买入卖出的交易额情况，决定是否能够成交以及实际成交的量，注意
# 仅在调仓日做调整，若调仓日无法卖出或买入，后续的时间不再进行补充的交易操作

# 20170907更新说明
# 方法二，相对方法一，在Total_Position的细节上的观察更方便


#### load libraries   ####
library(dplyr)
library(lubridate)
library(readr)
library(DBI)
library(rJava)
library(RJDBC)
library(zoo)
library(ggplot2)
library(reshape2)

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
Buy_Proportion <- 0.2 #买入时买入额占当天成交额的最大比例
Sell_Proportion <- 0.2 #卖出时卖出额占当天成交额的最大比例 

#### set signal info ####
Signal_Series <- 'Contrarian'
Signal_Name <- 'Contrarian_AvgDiff_3pct'

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

Signal_Total <- rbind(Signal_Raw, Signal_Assist) %>% arrange(Date)

#得到Rebalance序列


#读入行情数据#
# BondQuote_Orig <- dbGetQuery(conn,paste0("SELECT t2.SecuCode,t.InnerCode,t2.SecuAbbr,t.TradingDay,t.PrevClosePrice,t.ClosePrice,t.OpenPrice
#                                          ,t.LowPrice,t.HighPrice,t.TurnoverValue,t.TurnoverVolume,t.TurnoverDeals,
#                                          t1.BondTypeLevel1Desc AS BondType, t1.BondNature, t4.IssuerNature, t3.ValueCleanPrice
#                                          FROM jydb.dbo.QT_DailyQuote t
#                                          LEFT JOIN jybond.dbo.Bond_Code t1
#                                          ON t.InnerCode=t1.InnerCode
#                                          LEFT JOIN jydb.dbo.SecuMain t2
#                                          ON t.InnerCode=t2.InnerCode
#                                          LEFT JOIN jybond.dbo.Bond_CSIValuation t3
#                                          ON t.InnerCode=t3.InnerCode AND t.TradingDay=t3.EndDate
#                                          LEFT JOIN jybond.dbo.Bond_BasicInfo t4
#                                          ON t.InnerCode=t4.InnerCode
#                                          WHERE
#                                          t2.SecuCategory IN ( 6, 7, 11, 28)
#                                          /*6国债,7金融债,11企业债券,28地方政府债;9可转债,29可交换公司债*/
#                                          AND t2.SecuMarket IN ( 83, 90 )
#                                          AND t.TradingDay>='",StartDate_Backtest,"' AND t.TradingDay<='",EndDate_Backtest,"'")) %>%
#   mutate(TradingDay=as.Date(TradingDay),SecuCode=as.character(SecuCode),InnerCode=as.character(InnerCode))

#裁剪行情数据，仅保留有信号的债券的行情，加速回测
BondQuote_Cut_All <- BondQuote_Orig %>% filter(InnerCode %in% distinct(Signal_Total,InnerCode)$InnerCode) %>%
  mutate(ReturnDaily=ClosePrice/PrevClosePrice-1) %>% 
  select(-OpenPrice,-LowPrice,-HighPrice,-TurnoverVolume,-TurnoverDeals,-BondType,-BondNature,-IssuerNature) #减少字段，方便代码编写

Cash <- NULL
Real_Position <- NULL
for(cursor in 1:nrow(RebalanceDate_Backtest)){
  print(cursor)
  TradeDate_ <- RebalanceDate_Backtest$BuyDate[cursor]
  SellDate_ <- RebalanceDate_Backtest$SellDate[cursor]
  
  BondQuote_Cut_ <- BondQuote_Cut_All %>% filter(TradingDay %in% c(TradeDate_,SellDate_)) 
  #裁剪行情数据，加速回测,包含TradeDate、SellDate两天行情，关联时需要关联TradingDay
  
  if(cursor ==1 ){#第一期单独处理
    Real_Position[[cursor]] <- Signal_Total %>% filter(Date==TradeDate_) %>% 
      left_join(BondQuote_Cut_, by = c('InnerCode','Date'='TradingDay')) %>% #关联上当日行情计算购买量
      mutate(SellDate=SellDate_) %>% #关联上SellDate的行情，以观察持仓期表现
      left_join(BondQuote_Cut_[c('InnerCode','TradingDay','ReturnDaily')], by = c('InnerCode','SellDate'='TradingDay')) %>%
      rename(Return_Buy=ReturnDaily.x,Return_Sell=ReturnDaily.y) %>% 
      mutate(Target_Asset  = Weight*Cash_Initial*Trade_Ratio, #目标买入金额
             Asset_BfTrade = 0, #盘前资产
             Asset_AtTrade = 0, #换仓时资产
             Trade_Orders  = Target_Asset, #目标交易单
             TurnoverValue = ifelse(is.na(TurnoverValue),0,TurnoverValue),
             Buy_Ceiling   = TurnoverValue*Buy_Proportion, #买入金额上限
             Actual_Buying = ifelse(Buy_Ceiling>Target_Asset,Target_Asset,Buy_Ceiling), #实际能买入的金额
             Asset_AfTrade = Actual_Buying, #盘后资产
             Asset_HoldTmr = Asset_AfTrade*(1+Return_Sell)) %>% 
      select(Date,InnerCode,Weight,Target_Asset,Asset_BfTrade,Asset_AtTrade,Trade_Orders,
             Asset_AfTrade,Asset_HoldTmr,everything()) 
    
    Cash[cursor] <- Cash_Initial - sum(Real_Position[[cursor]]$Asset_AfTrade) #购买债券后的剩余资金
    
  } else {#第二期开始
    Prev_Position_ <- Real_Position[[cursor-1]] %>%
      filter(Asset_AfTrade>0 | InnerCode=='AS0000')  #去掉前一期中已经卖出的债券
    
    Target_Position_ <- Signal_Total %>% filter(Date==TradeDate_) 
    
    #前后两期持仓权重
    Position_Matrix <- rbind(distinct(Prev_Position_,InnerCode),distinct(Target_Position_,InnerCode)) %>% 
      distinct(InnerCode) %>% #前后两期所有债
      left_join(Prev_Position_[c('InnerCode','Weight')], by = 'InnerCode') %>% #关联上前一期的持仓权重
      rename(Weight_LT=Weight) %>%
      left_join(Target_Position_[c('InnerCode','Weight')], by = 'InnerCode') %>%  #关联上当前一期的持仓权重
      mutate(Weight_LT=ifelse(is.na(Weight_LT),0,Weight_LT), Weight=ifelse(is.na(Weight),0,Weight)) #NA->0
    
    if(sum(Position_Matrix$Weight_LT!=Position_Matrix$Weight)==0){
      #如果后一期持仓权重和前一期相同，则不调仓，只对已有持仓的价值进行重新评估
      Real_Position[[cursor]]<- Position_Matrix %>%
        mutate(Date=TradeDate_,SellDate=SellDate_) %>% 
        left_join(BondQuote_Cut_[c('InnerCode','TradingDay','ReturnDaily')], by = c('InnerCode','SellDate'='TradingDay')) %>%
        select(Date,SellDate,InnerCode,SecuCode,SecuAbbr,Weight,
               Asset_BfTrade = Asset_AfTrade,
               Asset_AtTrade = Asset_HoldTmr,
               Return_Buy    = Return_Sell,
               Return_Sell   = ReturnDaily) %>%
        mutate(Trade_Orders  = 0,
               Actual_Buying = 0,
               IfDelisted    = ifelse(is.na(SecuCode)&(InnerCode!='AS0000'),1,0), #是否退市，若当天关联不到，代表上一期退市
               Asset_AtTrade = ifelse(IfDelisted==0,Asset_AtTrade,Asset_BfTrade), #处理退市债券
               Asset_AfTrade = ifelse(IfDelisted==0,Asset_AtTrade,0),
               Asset_HoldTmr = Asset_AfTrade*(1+Return_Sell),
               Target_Asset  = (sum(Asset_AtTrade, na.rm=T)+Cash[cursor-1])*Trade_Ratio*Weight
               )
      
      if(sum(Real_Position[[cursor]]$IfDelisted)==0){
        Cash[cursor] <- Cash[cursor-1]
        } else{
          #若退市，则退市资金进入Cash
          Cash[cursor] <- Cash[cursor-1] + sum((Real_Position[[cursor]] %>% filter(IfDelisted==1))$Asset_AtTrade)
        }
      
      Real_Position[[cursor]] <- Real_Position[[cursor]] %>% select(-IfDelisted)
      
    } else{#如果后一期持仓权重和前一期不同，则进行调仓
      #先计算调仓时的总资产，然后按照总资金按权重进行分配，得到目标持仓
      #最后根据目标持仓和原来持仓的差额进行先卖后买
      
      Real_Position[[cursor]]<- Position_Matrix %>% 
        left_join(Prev_Position_[c('InnerCode','Asset_AfTrade','Asset_HoldTmr')], by = 'InnerCode') %>%
        rename(Asset_BfTrade = Asset_AfTrade, Asset_AtTrade = Asset_HoldTmr) %>% 
        mutate(Asset_BfTrade = ifelse(is.na(Asset_BfTrade),0,Asset_BfTrade),
               Asset_AtTrade = ifelse(is.na(Asset_AtTrade),0,Asset_AtTrade)) %>%
        mutate(Date=TradeDate_,SellDate=SellDate_) %>%
        left_join(BondQuote_Cut_, by = c('InnerCode','Date'='TradingDay')) %>% #关联调仓日行情
        mutate(IfDelisted    = ifelse(is.na(SecuCode)&(InnerCode!='AS0000'),1,0), #是否退市，若当天关联不到，代表上一期退市
             Asset_AtTrade = ifelse(IfDelisted==0,Asset_AtTrade,Asset_BfTrade)) %>% #处理退市债券
        mutate(Target_Asset = Weight*(sum(Asset_AtTrade, na.rm =T)+Cash[cursor-1])*Trade_Ratio) %>% #调仓时的目标持仓市值
        mutate(Trade_Orders = Target_Asset - Asset_AtTrade) %>% #实际需要交易的差额形成的交易单
        mutate(LongShort = ifelse(Trade_Orders>0,1,ifelse(Trade_Orders<0,-1,0))) %>% 
        mutate(TurnoverValue=ifelse(is.na(TurnoverValue),0,TurnoverValue),
               Sell_Ceiling = TurnoverValue*Sell_Proportion,
               Buy_Ceiling = TurnoverValue*Buy_Proportion) %>%
        mutate(Actual_Selling = ifelse(Sell_Ceiling>=abs(Trade_Orders),-Trade_Orders,Sell_Ceiling)*ifelse(LongShort==-1,1,0)) %>%#实际卖出额
        mutate(Actual_Selling = ifelse(IfDelisted==0,Actual_Selling,Asset_AtTrade)) %>% #处理已退市股票，退市T+1日全部卖出
        mutate(Cash_Remain = (sum(Asset_AtTrade) + Cash[cursor-1])*(1-Trade_Ratio),
               Cash_Usable = Cash[cursor-1] + sum(Actual_Selling) - Cash_Remain) %>% #卖出后可用的资金总额
        mutate(Cash_Usable = ifelse(Cash_Usable>=0,Cash_Usable,0)) %>%
        mutate(AnyLong = sum(LongShort>0),
               Buy_Weight = ifelse(AnyLong>0,ifelse(Trade_Orders>0,Trade_Orders,0)/sum(Trade_Orders[Trade_Orders>0]),0),
               Cash_AlloToBuy = Cash_Usable * Buy_Weight) %>%
        mutate(Actual_Buying = ifelse(Buy_Ceiling>Cash_AlloToBuy, Cash_AlloToBuy, Buy_Ceiling)) %>%
        mutate(Asset_AfTrade=Asset_AtTrade-Actual_Selling+Actual_Buying) %>%
        mutate(SellDate=SellDate_) %>%
        left_join(BondQuote_Cut_[c('InnerCode','TradingDay','ReturnDaily')], by = c('InnerCode','SellDate'='TradingDay')) %>%
        rename(Return_Buy=ReturnDaily.x,Return_Sell=ReturnDaily.y) %>%
        mutate(Asset_HoldTmr = Asset_AfTrade*(1+Return_Sell)) 
      
      Cash[cursor] <- sum(Real_Position[[cursor]]$Asset_AtTrade, na.rm =T) + Cash[cursor-1] - 
        sum(Real_Position[[cursor]]$Asset_AfTrade) #调仓日剩余现金
      
    }
  }
}


#合并每期的持仓
Total_Positions <- bind_rows(Real_Position) %>%
  select(Date,SellDate,InnerCode,SecuCode,SecuAbbr,Weight_LT,Weight,Asset_BfTrade,Asset_AtTrade,Target_Asset,Trade_Orders,Actual_Selling,
         Cash_AlloToBuy,Actual_Buying,Asset_AfTrade,Asset_HoldTmr,Return_Buy,Return_Sell,LongShort,Sell_Ceiling,Buy_Ceiling,
         everything()) %>%
  mutate(IfHold = ifelse(Asset_AfTrade>0 | InnerCode=='AS0000', 1, 0)) #是否是当期持仓的标志，若为0，则当期已全部卖出


#合并资金曲线,并计算最大回撤
Total_Asset <- Total_Positions %>%  group_by(Date) %>% summarise(Asset_Securities = sum(Asset_AfTrade,na.rm=T)) %>% 
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
            AnnualizedVol = round(sd(Term_Return)*denominator,4)) %>%
  mutate(IR = round(AnnualizedReturn/AnnualizedVol, 2))

##绘图
Graph_Total_Asset <- ggplot(data = Total_Asset)+
  geom_line(aes(x=Date, y= Asset_Total/Asset_Total[1]))+
  labs(title = '累计收益曲线',
       x = 'Time Line',
       y = 'Cumulative Return')

##目标持仓和真实持仓的偏差
Total_Miss <-Total_Positions %>% filter(InnerCode != 'AS0000') %>% 
  mutate(IfTarget=ifelse(ceiling(Target_Asset/100)>0,1,0)) %>% #是否是目标持仓的标志,1是0否
  mutate(Diff=Asset_AfTrade-Target_Asset) %>% #偏差=真实-目标
  group_by(Date,SellDate,IfTarget) %>%
  summarise(Total_Diff=sum(abs(Diff),na.rm=T)) %>%
  left_join(Total_Asset, by = 'Date') %>%
  mutate(MissRate=Total_Diff/Asset_Total) %>% #偏差绝对值占当期总资产的比重
  select(Date,SellDate,IfTarget,Total_Diff,Asset_Securities,Cash,Asset_Total,MissRate) %>% ungroup

Miss_InSide <- Total_Miss %>% filter(IfTarget==1) %>% 
  select(Date,IfTarget,Total_Diff,MissRate) %>%
  right_join(Total_Asset, by = 'Date') %>%
  mutate(Total_Diff = ifelse(is.na(Total_Diff),0,Total_Diff),
         MissRate = ifelse(is.na(MissRate),0,MissRate),
         IfTarget = ifelse(is.na(IfTarget),1,IfTarget)
         )

Miss_OutSide <- Total_Miss %>% filter(IfTarget==0) %>% 
  select(Date,IfTarget,Total_Diff,MissRate) %>%
  right_join(Total_Asset, by = 'Date') %>%
  mutate(Total_Diff = ifelse(is.na(Total_Diff),0,Total_Diff),
         MissRate = ifelse(is.na(MissRate),0,MissRate),
         IfTarget = ifelse(is.na(IfTarget),0,IfTarget)
         )

#绘图
Graph_Position_Miss <- ggplot(data = Miss_InSide) +
  geom_line(aes(x=Date, y = MissRate,colour="In"), show.legend = TRUE) +
  geom_line(data = Miss_OutSide, aes(x=Date,y=MissRate, colour="Out"), show.legend = TRUE) +
  ylim(0, 1) +
  labs(title = 'Holdings Positions Bias(Real vs Target)',
       x = 'TimeLine',
       y = 'Holding Bias Rate')+
  theme(legend.position = 'right')+
  scale_colour_manual(name = 'Side' , values=c('In'='blue','Out'='red'))

##每期目标持仓数量与实际持仓数量（债券种类）的差别
Total_Positions %>% filter(Date=='2017-05-16',Asset_AfTrade>0) %>% group_by()

Real_Species<- Total_Positions %>% filter(IfHold==1) %>% group_by(Date) %>% summarise(N_Real=n()-1)
Target_Species <- Signal_Total %>% group_by(Date) %>% summarise(N_Tar=n()-1)
Species_Diff<- Real_Species %>% inner_join(Target_Species, by ='Date') %>% mutate(N_Diff = N_Real - N_Tar)

#绘图
Graph_Species_Diff <- ggplot(data=melt(Species_Diff, id=('Date'), variable.name='Group', value.name='N'))+
  geom_line(aes(x=Date,y=N,colour=Group))+
  labs(title = 'Holding Species Difference',
       x ='Date',
       y = 'Number')

##盈亏来源分析








####  output results   ####
# Path_BacktestResult <- paste0('BacktestResult/', Signal_Series, '/', Signal_Name, "/", Sys.Date(), "_", format(Sys.time(), "%H%M%S"))
# dir.create(Path_BacktestResult, recursive = T)
# 
# BackTest_Info <- paste0("Signal Series: ", Signal_Series, "\n",
#                         "Signal: ", Signal_Name, "\n",
#                         "Report Generation Time: ", Sys.time(), "\n",
#                         "Start Date: ", StartDate_Backtest, "\n",
#                         "End Date: ", EndDate_Backtest, "\n",
#                         "Rebalance Interval: ", Interval_Backtest, "\n"
# )

# write(BackTest_Info, file = paste0(Path_BacktestResult, paste0("/BackTest_Info_",Interval_Backtest,".txt")))
# 
# write_csv(IR, path = paste0(Path_BacktestResult, '/IR.csv'))
# 
# write_csv(Total_Asset, path = paste0(Path_BacktestResult, '/Total_Asset.csv'))
# 
# write_excel_csv(Total_Positions, path = paste0(Path_BacktestResult, '/Total_Positions.csv'))
# 
# #收益曲线
# png(filename = paste0(Path_BacktestResult,
#                       '/Graph_Total_Asset.png'),
#     width = 700, height = 400)
# print(Graph_Total_Asset)
# dev.off()








