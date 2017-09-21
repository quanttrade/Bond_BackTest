#国债期货相关
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

#国债期货合约
Fut_Contract <- dbGetQuery(conn, "SELECT ContractCode,t.EffectiveDate,t.LastTradingDate,t.DeliveryDate,t.LastDeliveryDate 
                           FROM jydb.dbo.Fut_ContractMain t
                           WHERE t.OptionCode IN (501,502) AND t.LastTradingDate IS NOT NULL 
                           ORDER BY ContractCode") %>%
  mutate(EffectiveDate = as.Date(EffectiveDate), 
         LastTradingDate = as.Date(LastTradingDate),
         DeliveryDate = as.Date(DeliveryDate),
         LastDeliveryDate = as.Date(LastDeliveryDate)
  )

#国债期货行情
FutContract_Quote <- dbGetQuery(conn, "SELECT t.TradingDay,t.ContractCode,t.SettlePrice FROM jydb.dbo.Fut_TradingQuote t
                                LEFT JOIN jydb.dbo.Fut_ContractMain t1
                                ON t.ContractInnerCode=t1.ContractInnerCode
                                WHERE
                                t1.OptionCode IN (501,502) AND t1.LastTradingDate IS NOT NULL 
                                ") %>%
  mutate(TradingDay=as.Date(TradingDay))

#### 从DATAYES取国债期货可交割券及转换因子 ####
# httpheader=c("Authorization" = "Bearer 8f0891ab14acb11cb86cb7c0b859a6be060d47fa53d9a181f24720b8468022fa");
# 
# F_GetData <- function(url){
#   http <- paste("https://api.wmcloud.com/data/v1",url,sep = "")
#   return (getURL(http, httpheader=httpheader, ssl.verifypeer = FALSE,.encoding="utf8"))
# }
# 
# ConvFactors <- NULL
# for(i in seq_len(nrow(Fut_Contract))) {
#   url <- paste0("/api/future/getFutuConvf.csv?field=&secID=&ticker=", 
#                 Fut_Contract$ContractCode[i])
#   result <- F_GetData(url)
#   
#   if(nchar(result)>200){ #除去一些错误情况
#     Curr_ConvFactors <- read_csv(result)
#   } else {Curr_ConvFactors <- NULL}
#   
#   ConvFactors <- rbind(ConvFactors,Curr_ConvFactors)
# }
# 
# #调整中文编码
# Encoding(ConvFactors$secShortName)<-'GB2312'
# Encoding(ConvFactors$bondFullName)<-'GB2312'
# 
# ConvFactors <- ConvFactors %>% data.frame()
# write_excel_csv(ConvFactors, 'ConvFactors.csv')


## 中金所网站下载可交割券
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

ConvFactors <- ConvFactors_Orig %>% mutate(SecuCode_SZ = as.character(SecuCode_SZ),
                                           MaturityDate = ymd(MaturityDate)) %>%
  select(ContractCode,everything())

# write_excel_csv(ConvFactors, 'ConvFactors_CFFEX.csv')

ConvFactors_All <- melt(ConvFactors, id = c('ContractCode','SecuName','MaturityDate','Coupon','ConFactor')) %>%
  rename(SecuCode=value,SecuMarket=variable) %>%
  mutate(SecuMarket=apply(data.frame(SecuMarket), MARGIN=1,FUN=function(x){switch(x,
                                                                                  SecuCode_IB = '89',
                                                                                  SecuCode_SH = '83',
                                                                                  SecuCode_SZ = '90'
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

Fut_All <- FutContract_Quote %>% left_join(ConvFactors_All, by = 'ContractCode') %>% 
  inner_join(DeliveryBondQuote, by =c('SecuCode','SecuMarket','TradingDay'='EndDate'))
# Bond_BasicInfo有非常多的数据缺失，没办法计算



