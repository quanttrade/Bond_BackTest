test<- function(){
  Cash <- NULL
  Real_Position <- NULL
  for(cursor in 1:nrow(RebalanceDate_Backtest)){
    print(cursor)
    TradeDate_ <- RebalanceDate_Backtest$BuyDate[cursor]
    # SellDate_ <- RebalanceDate_Backtest$SellDate[cursor] #不是必需
    
    BondQuote_Cut_ <- BondQuote_Cut_All %>% filter(TradingDay ==TradeDate_) 
    #裁剪行情数据，加速回测,若包含TradeDate、SellDate两天行情，关联时需要关联TradingDay
    
    if(cursor ==1){#第一期单独处理
      Real_Position[[cursor]] <- Signal_Total %>% filter(Date==TradeDate_) %>% 
        left_join(BondQuote_Cut_, by = c('InnerCode','Date'='TradingDay')) %>% #关联上当日行情计算购买量
        mutate(Target_Asset  = Weight*Cash_Initial*Trade_Ratio, #目标买入金额
               Asset_BfTrade = 0, #盘前资产
               Asset_AtTrade = 0, #换仓时资产
               Trade_Orders  = Target_Asset, #目标交易单
               TurnoverValue = ifelse(is.na(TurnoverValue),0,TurnoverValue),
               Buy_Ceiling   = TurnoverValue*Buy_Proportion, #买入金额上限
               Actual_Buying = ifelse(Buy_Ceiling>Target_Asset,Target_Asset,Buy_Ceiling), #实际能买入的金额
               Asset_AfTrade = Actual_Buying #盘后资产
        ) %>% 
        select(Date,InnerCode,Weight,Target_Asset,Asset_BfTrade,Asset_AtTrade,Trade_Orders,
               Asset_AfTrade,everything()) 
      
      Cash[cursor] <- Cash_Initial - sum(Real_Position[[cursor]]$Actual_Buying) #购买债券后的剩余资金
      
    } else {#第二期开始
      
      Cash[cursor] <- Cash[cursor-1] #初始化本期现金
      
      Target_Position_ <- Signal_Total %>% filter(Date==TradeDate_) #目标持仓
      
      Prev_Position_ <- Real_Position[[cursor-1]] %>%
        filter(Asset_AfTrade>0) #去掉前一期中已经卖出的债券
      
      #前后两期持仓权重
      Position_Matrix <- rbind(distinct(Prev_Position_,InnerCode),distinct(Target_Position_,InnerCode)) %>% 
        distinct(InnerCode) %>% #前后两期所有债券
        #关联上前一期的持仓情况，仅需上一期的权重和资产
        filter(InnerCode != 'AS0000') %>% #'AS0000'单独简单处理，最后再合并回来，否则后面都要考虑到AS0000，要做很多ifelse的处理
        left_join(Prev_Position_[c('InnerCode','Weight','Asset_AfTrade')], by = 'InnerCode') %>% 
        rename(Weight_LT = Weight, 
               Asset_BfTrade = Asset_AfTrade) %>%
        left_join(Target_Position_[c('InnerCode','Weight')], by = 'InnerCode') %>%  #关联上当前一期的持仓权重
        mutate(Weight_LT=ifelse(is.na(Weight_LT),0,Weight_LT), 
               Weight=ifelse(is.na(Weight),0,Weight),
               Asset_BfTrade = ifelse(is.na(Asset_BfTrade),0,Asset_BfTrade)) %>% #NA->0
        mutate(Date = TradeDate_) %>%
        left_join(BondQuote_Cut_, by = c('InnerCode', 'Date' = 'TradingDay')) %>%
        mutate(IfDelisted    = ifelse(is.na(SecuCode),1,0), #是否退市，若当天关联不到，代表上一期退市
               Sell_Ceiling  = TurnoverValue*Sell_Proportion,
               Buy_Ceiling   = TurnoverValue*Buy_Proportion,
               Asset_AtTrade = Asset_BfTrade*(1+ReturnDaily)
        )
      
      #单独处理AS0000，最后合并回去
      AS0000 <- data.frame(InnerCode='AS0000',
                           Date = TradeDate_,
                           Asset_AtTrade = 0,
                           Asset_AfTrade = 0,
                           stringsAsFactors = F
      )
      
      ##首先处理退市债券
      Delisted_List <- NULL
      if(Position_Matrix %>% filter(is.na(SecuCode)) %>% nrow()>0){#检查是否有退市债券，有的话进行相应处理
        Delisted_List <- Position_Matrix %>% filter(is.na(SecuCode)) %>% 
          mutate(
            Asset_AtTrade = Asset_BfTrade,
            Asset_AfTrade = 0,
            Target_Asset  = 0,
            Actual_Selling = Asset_BfTrade
          )
        
        Cash[cursor] <- Cash[cursor] + sum(Delisted_List$Asset_BfTrade) #债券T日退市，然后在T+1日获得的资金
        
        Position_Matrix <- Position_Matrix %>% filter(!is.na(SecuCode)) #去除掉退市债券
      }
      
      #计算当期交易时的总资产
      Curr_Asset_Total <- sum(Position_Matrix$Asset_AtTrade, na.rm = T) + Cash[cursor]
      
      ##每天先检查是否有不在目标持仓中的债券，若有则先处理（卖出）这些债券，然后再根据目标持仓是否有变动进行Rebalance交易
      Daily_DealList <- Position_Matrix %>% filter(Weight < 0.0000001) %>% #处理不在目标持仓中的债券
        mutate(Target_Asset = 0,
               Trade_Orders   = -Asset_AtTrade,
               LongShort = -1,
               Actual_Selling = ifelse(Sell_Ceiling>=-Trade_Orders,-Trade_Orders,Sell_Ceiling),
               Asset_AfTrade = Asset_AtTrade - Actual_Selling
        )
      
      Cash[cursor] <- Cash[cursor] + sum(Daily_DealList$Actual_Selling) #每日查仓，卖出非目标持仓的债券后的现金
      
      Position_Matrix <- Position_Matrix %>% filter(Weight >= 0.0000001)
      
      ## 处理完退市、不在目标持仓中的债券后，对目标持仓进行调仓
      if(sum(Position_Matrix$Weight_LT!=Position_Matrix$Weight)==0){
        #如果后一期持仓权重和前一期相同，则不调仓，只对已有持仓的价值进行重新评估
        Real_Position[[cursor]]<- Position_Matrix %>%
          mutate(Trade_Orders  = 0,
                 Actual_Buying = 0,
                 Asset_AfTrade = Asset_AtTrade,
                 Target_Asset  = Asset_AtTrade) %>% 
          bind_rows(Daily_DealList, Delisted_List, AS0000) #合并上不在目标持仓中的券以及AS0000
        
      } else{#如果后一期持仓权重和前一期不同，则进行调仓
        #先计算调仓时的总资产，然后按照总资金按权重进行分配，得到目标持仓
        #最后根据目标持仓和原来持仓的差额进行先卖后买
        
        Real_Position[[cursor]]<- Position_Matrix %>% 
          mutate(Target_Asset = Weight*Curr_Asset_Total*Trade_Ratio) %>% #调仓时的目标持仓市值
          mutate(Trade_Orders = Target_Asset - Asset_AtTrade) %>% #实际需要交易的差额形成的交易单
          mutate(LongShort = ifelse(Trade_Orders>0,1,ifelse(Trade_Orders<0,-1,0))) %>% 
          mutate(Actual_Selling = ifelse(Sell_Ceiling>=abs(Trade_Orders),-Trade_Orders,Sell_Ceiling)*ifelse(LongShort==-1,1,0)) %>%#实际卖出额
          mutate(Cash_Remain = Curr_Asset_Total*(1-Trade_Ratio), 
                 Cash_Usable = Cash[cursor] + sum(Actual_Selling) - Cash_Remain) %>% #卖出后可用的资金总额
          mutate(Cash_Usable = ifelse(Cash_Usable>=0,Cash_Usable,0)) %>%
          mutate(AnyLong = sum(LongShort>0),
                 Buy_Weight = ifelse(AnyLong>0,ifelse(Trade_Orders>0,Trade_Orders,0)/sum(Trade_Orders[Trade_Orders>0]),0),
                 Cash_AlloToBuy = Cash_Usable * Buy_Weight) %>%
          mutate(Actual_Buying = ifelse(Buy_Ceiling>Cash_AlloToBuy, Cash_AlloToBuy, Buy_Ceiling)) %>%
          mutate(Asset_AfTrade=Asset_AtTrade-Actual_Selling+Actual_Buying) %>% 
          bind_rows(Daily_DealList, Delisted_List, AS0000)
        
        Cash[cursor] <- Curr_Asset_Total - sum(Real_Position[[cursor]]$Asset_AfTrade, na.rm = T) #调仓日剩余现金
        
      }
    }
  }
}









