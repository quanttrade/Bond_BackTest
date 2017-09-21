#### 国债期货相关 ####
#### load libraries ####
library(dplyr)
library(lubridate)
library(readr)
library(DBI)
library(rJava)
library(RJDBC)
library(zoo)
library(ggplot2)
library(reshape2)
library(RCurl)
library(stringr)
library(WindR)

#### set path ####
jdbc_path <- "C:/Users/shuorui.zhang/Documents/R_WORKING_DIR/jdbc driver/sqljdbc_6.0/enu/jre8/sqljdbc42.jar"
load_path <- "D:/DATA_NEW/"
signal_path <- "D:/BackTest_Framework/SignalPool_Bonds/"

#### DB connection ####
drv <- JDBC("com.microsoft.sqlserver.jdbc.SQLServerDriver", jdbc_path, identifier.quote = "`")    
conn <- dbConnect(drv,"jdbc:sqlserver://192.168.66.12",databaseName="ZSR","shuorui.zhang","Xyinv1704")

#### 国债期货合约信息 ####
Fut_Contract <- dbGetQuery(conn, "SELECT ContractCode,t.EffectiveDate,t.LastTradingDate,t.DeliveryDate,t.LastDeliveryDate 
                           FROM jydb.dbo.Fut_ContractMain t
                           WHERE t.OptionCode IN (501,502) AND t.LastTradingDate IS NOT NULL 
                           AND IfReal=1
                           ORDER BY ContractCode") %>%
  mutate(EffectiveDate = as.Date(EffectiveDate), 
         LastTradingDate = as.Date(LastTradingDate),
         DeliveryDate = as.Date(DeliveryDate),
         LastDeliveryDate = as.Date(LastDeliveryDate)) %>%
  mutate(ContractType=substr(ContractCode,1,nchar(ContractCode)-4),
         ContractType = ifelse(ContractType == 'T', 'TT', ContractType))

## 国债期货行情
FutContractQuote <- dbGetQuery(conn, "SELECT t.TradingDay,t.ContractCode,PrevClosePrice,ClosePrice,
                           TurnoverVolume,TurnoverValue,OpenInterest
                           FROM jydb.dbo.Fut_TradingQuote t
                           LEFT JOIN jydb.dbo.Fut_ContractMain t1
                           ON t.ContractInnerCode=t1.ContractInnerCode
                           WHERE
                           t1.OptionCode IN (501,502) AND t1.LastTradingDate IS NOT NULL 
                           AND IfReal=1
                           ") %>%
  mutate(TradingDay=as.Date(TradingDay))
  

## 构造国债期货主力合约
# 判断指标:交易量*持仓量(TotalVolume=TurnoverVolume*OpenInterest)
FutMainContr <- FutContractQuote %>%
  mutate(ContractType=substr(ContractCode,1,nchar(ContractCode)-4),
         ContractType = ifelse(ContractType == 'T', 'TT', ContractType),
         TotalVolume=TurnoverVolume*OpenInterest) %>% 
  group_by(ContractType,TradingDay) %>%  
  mutate(Rank = rank(-TotalVolume,na.last=TRUE,ties.method="first")) %>% 
  arrange(ContractType,TradingDay,Rank) %>% ungroup() %>%
  filter(Rank==1) %>% 
  select(-TotalVolume,-Rank) %>% 
  arrange(ContractType,TradingDay) %>%
  #主力合约更换品种时的收盘价对齐
  #复权计算
  group_by(ContractType) %>% 
  mutate(PreClose_temp=lag(ClosePrice),Ratio=ifelse(is.na(PreClose_temp/PrevClosePrice),1,PreClose_temp/PrevClosePrice))%>%
  mutate(AdjustFactor=cumprod(Ratio),AdjClosePrice=ClosePrice*AdjustFactor) %>%
  ungroup() %>% 
  arrange(ContractType,TradingDay) %>%
  mutate(DailyReturn = ClosePrice/PrevClosePrice -1,
         DailyReturn = ifelse(is.na(DailyReturn),0,DailyReturn)) %>%
  select(TradingDay,ContractCode,PrevClosePrice,ClosePrice,ContractType,DailyReturn)

# 至此，关于品种FutBondType的主力合约MainFutContr构造完毕
# 检查主力合约汇总各品种分布均匀度
# distribution of main contract
# FutMainContr %>% group_by(ContractType,ContractCode) %>% summarise(count=n()) %>% ungroup() %>% View

#### 中金所网站下载可交割券 ####
ConvFactors_Orig <- NULL
for (i in 1:nrow(Fut_Contract)){
  Contract_ <- Fut_Contract$ContractCode[i]
  
  url <- paste0('http://www.cffex.com.cn/sj/jgsj/jgqsj/',Contract_,'/',Contract_,'.csv')
  Delivery_Bond_ <- getURL(url,.encoding='GB2312')
  
  if(str_detect(Delivery_Bond_,'error')==TRUE){
    Curr_ConvFactors <- NULL
    } else {
      Delivery_Bond_ <- iconv(Delivery_Bond_,"gb2312","UTF-8") #编码转化
      
      Curr_ConvFactors <- read_csv(Delivery_Bond_) %>% 
        mutate(FutCode = Contract_) 
    }
  
  ConvFactors_Orig <- rbind(ConvFactors_Orig,Curr_ConvFactors)
}

colnames(ConvFactors_Orig)<-c('SecuName','SecuCode_IB','SecuCode_SH','SecuCode_SZ','MaturityDate','Coupon','ConFactor','ContractCode')

ConvFactors <- ConvFactors_Orig %>% 
  mutate(SecuCode_SZ = as.character(SecuCode_SZ),
         MaturityDate = ymd(MaturityDate)) %>%
  select(ContractCode,everything())

# write_excel_csv(ConvFactors, 'ConvFactors_CFFEX.csv')

#展开成所有的券，即统一债券不同交易所分列多条记录
ConvFactors_All <- melt(ConvFactors, id = c('ContractCode','SecuName','MaturityDate','Coupon','ConFactor')) %>%
  rename(SecuCode=value,SecuMarket=variable) %>%
  mutate(SecuMarket=apply(data.frame(SecuMarket), MARGIN=1,FUN=function(x){switch(x,
                           SecuCode_IB = '89',
                           SecuCode_SH = '83',
                           SecuCode_SZ = '90',
                           NA
                           )})) %>% 
  left_join(Fut_Contract, by = 'ContractCode') %>%
  select(ContractCode,EffectiveDate,LastTradingDate,DeliveryDate,LastDeliveryDate,
         SecuCode,SecuMarket,SecuName,Coupon,MaturityDate,ConFactor)

dbWriteTable(conn, "ZSR.DBO.Bond_BT_ConvFactors_All", ConvFactors_All, overwrite=TRUE)

dbRemoveTable(conn, "zsr.dbo.Bond_BT_UniqueDeliveryBond")

UniqueDeliveryBond <- distinct(ConvFactors_All[c('SecuCode','SecuMarket')],SecuCode,SecuMarket)

dbWriteTable(conn, "[ZSR].[dbo].[Bond_BT_UniqueDeliveryBond]", UniqueDeliveryBond, overwrite = TRUE)

DeliveryBondQuote <- dbGetQuery(conn, "SELECT t.EndDate,t.ValueCleanPrice AS CleanPrice, t.ValueFullPrice AS FullPrice
           ,t.VPADuration AS Duration,t.AccruInterest
           ,t2.SecuCode,t2.SecuMarket
           FROM jybond.dbo.Bond_CSIValuation t
           LEFT JOIN jybond.dbo.bond_code t1
           ON t.InnerCode=t1.InnerCode
           INNER JOIN zsr.dbo.bond_bt_uniquedeliverybond t2
           ON t1.SecuCode=t2.secucode AND t1.SecuMarket=t2.secumarket") %>%
  mutate(EndDate=as.Date(EndDate))

#### WIND取最廉券 ####
# Bond_BasicInfo有非常多的数据缺失（缺少很多券），没办法计算，直接取wind计算出的CTD
# 只取了银行间的债券
CTD_List_Orig <- NULL
ErrorList <- NULL
for (i in 1:nrow(Fut_Contract)){
  print(i)
  w_wsd_data<-w.wsd(paste0(Fut_Contract$ContractCode[i],'.CFE'),"tbf_CTD",
                    Fut_Contract$EffectiveDate[i],min(Fut_Contract$LastTradingDate[i],Sys.Date()),
                    "exchangeType=NIB") #只取了银行间的债券
  if(w_wsd_data$ErrorCode == 0){
    w_wsd_data_new <- w_wsd_data$Data %>% mutate(ContractCode = Fut_Contract$ContractCode[i])
    CTD_List_Orig <- rbind(CTD_List_Orig,w_wsd_data_new)
  } else {
    #统计出错的合约
    print(Fut_Contract$ContractCode[i])
    print(w_wsd_data$ErrorCode)
    ErrorList <- rbind(ErrorList, Fut_Contract$ContractCode[i])
  }
}

CTD_List <- CTD_List_Orig %>% 
  filter(!is.na(TBF_CTD),TBF_CTD!='NaN') %>% 
  mutate(SecuMarket = substr(TBF_CTD,nchar(TBF_CTD)-1,nchar(TBF_CTD))) %>% 
  mutate(SecuMarket = apply(data.frame(SecuMarket), MARGIN=1,
                            FUN=function(x){switch(x,
                                                   'IB' = '89',
                                                   'SH' = '83',
                                                   'SZ' = '90',
                                                   NA)})) %>% 
  mutate(TBF_CTD = substr(TBF_CTD, 1,nchar(TBF_CTD)-3)) %>%
  select(ContractCode,TradingDay=DATETIME,SecuCode=TBF_CTD, SecuMarket) 
  
# 保存数据，反复使用
write_csv(CTD_List, paste0('CTD_List_',Sys.Date(),'.csv'))

#### 取可交割券久期 ####
Fut_Duration <- CTD_List %>% 
  left_join(DeliveryBondQuote, by = c('SecuCode','SecuMarket','TradingDay'='EndDate')) %>% 
  filter(!is.na(Duration)) %>%
  select(ContractCode,TradingDay,SecuCode,SecuMarket,Duration) %>% 
  data.frame 

#### 得到最终的行情数据 ####
FutMainQuote_temp <- FutMainContr %>% left_join(Fut_Duration, by = c('ContractCode','TradingDay')) %>%
  filter(TradingDay>='2015-04-22',TradingDay<='2017-05-19') #JYBOND中CSI估值数据区间

#检查缺失，WIND中计算IRR时对当天交易量有最小要求，要不低于100万元才会计算IRR
FutMainQuote_temp %>% filter(is.na(Duration)) 

#若有缺失，用上一期的DURATION进行填补
FutMainQuote <- FutMainQuote_temp %>% 
  group_by(ContractType) %>% 
  arrange(ContractType, TradingDay) %>% 
  mutate(Duration = na.locf(Duration)) %>%
  ungroup

write_csv(FutMainQuote, 'FutMainQuote.csv')























