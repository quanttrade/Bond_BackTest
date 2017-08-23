#functions
func_RebalanceDate <- function(startdate, enddate, interval = 'day', lagdays = 0){
  BaseDate_ <- QT_TradingDayNew_DB %>% filter(SecuMarket == '83') %>%
    filter(TradingDate >= startdate, TradingDate <= enddate) %>%
    filter(IfTradingDay == 1)

  if(interval == 'day'){
    RebalanceDate_ <- BaseDate_ %>%
      arrange(TradingDate) %>%
      mutate(IfRebalance = lag(IfTradingDay, lagdays)) %>%
      na.omit() %>%
      filter(IfRebalance == 1) %>%
      mutate(BuyDate = lag(TradingDate),
             SellDate = TradingDate) %>%
      na.omit() %>%
      select(BuyDate, SellDate)
  }
  
  return(RebalanceDate_)
}