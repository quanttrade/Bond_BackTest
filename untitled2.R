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
      Real_Position[[cursor]]<- Prev_Position_ %>%
        mutate(Date=TradeDate_,SellDate=SellDate_) %>% 
        left_join(BondQuote_Cut_[c('InnerCode','TradingDay','ReturnDaily')], by = c('InnerCode','SellDate'='TradingDay')) %>%
        select(Date,SellDate,InnerCode,SecuCode,SecuAbbr,Weight,
               Asset_BfTrade = Asset_AfTrade,
               Asset_AtTrade = Asset_HoldTmr,
               Return_Buy    = Return_Sell,
               Return_Sell   = ReturnDaily) %>%
        mutate(Trade_Orders  = 0,
               Actual_Buying = 0,
               Asset_AfTrade = Asset_AtTrade,
               Asset_HoldTmr = Asset_AfTrade*(1+Return_Sell),
               Target_Asset  = (sum(Asset_AtTrade, na.rm=T)+Cash[cursor-1])*Trade_Ratio*Weight
        )
      
      Cash[cursor] <- Cash[cursor-1]
      
    } else{#如果后一期持仓权重和前一期不同，则进行调仓
      #先计算调仓时的总资产，然后按照总资金按权重进行分配，得到目标持仓
      #最后根据目标持仓和原来持仓的差额进行先卖后买
      
      
      Sell_Side<- Position_Matrix %>% left_join(Prev_Position_[c('InnerCode','Asset_AfTrade','Asset_HoldTmr')], by = 'InnerCode') %>%
        rename(Asset_BfTrade = Asset_AfTrade, Asset_AtTrade = Asset_HoldTmr) %>% 
        mutate(Asset_BfTrade = ifelse(is.na(Asset_BfTrade),0,Asset_BfTrade),
               Asset_AtTrade = ifelse(is.na(Asset_AtTrade),0,Asset_AtTrade)) %>%
        mutate(Date=TradeDate_,SellDate=SellDate_) %>%
        mutate(Target_Asset = Weight*(sum(Asset_AtTrade, na.rm =T)+Cash[cursor-1])*Trade_Ratio) %>% #调仓时的目标持仓市值
        mutate(Trade_Orders = Target_Asset - Asset_AtTrade) %>% #实际需要交易的差额形成的交易单
        mutate(LongShort = ifelse(Trade_Orders>0,1,ifelse(Trade_Orders<0,-1,0))) %>%
        left_join(BondQuote_Cut_, by = c('InnerCode','Date'='TradingDay')) %>% #关联调仓日行情
        mutate(TurnoverValue=ifelse(is.na(TurnoverValue),0,TurnoverValue),
               Sell_Ceiling = TurnoverValue*Sell_Proportion,
               Buy_Ceiling = TurnoverValue*Buy_Proportion) %>%
        mutate(Actual_Selling = ifelse(Sell_Ceiling>=abs(Trade_Orders),-Trade_Orders,Sell_Ceiling)*ifelse(LongShort==-1,1,0)) %>%#实际卖出额
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
      
      
      Real_Position[[cursor]] <- Sell_Side  
      
      Cash[cursor] <- sum(Prev_Position_$Asset_HoldTmr, na.rm =T) + Cash[cursor-1] - 
        sum(Real_Position[[cursor]]$Asset_AfTrade) #调仓日剩余现金
      
    }
  }
}