"""
Simple Moving Average (SMA) Strategy

This strategy uses a 200-day simple moving average as a trend indicator:
- BUY when price crosses ABOVE the 200-day SMA
- SELL when price crosses BELOW the 200-day SMA
"""

from pyalgotrade import strategy
from pyalgotrade.technical import ma
from pyalgotrade.technical import cross


class SMAStrategy(strategy.BacktestingStrategy):
    """
    200-Day Simple Moving Average Crossover Strategy

    This is a trend-following strategy that:
    1. Buys when the closing price crosses above the 200-day SMA
    2. Sells when the closing price crosses below the 200-day SMA
    """

    def __init__(self, feed, instrument, sma_period=200):
        """
        Initialize the SMA strategy.

        Args:
            feed: Data feed with historical prices
            instrument (str): Ticker symbol to trade
            sma_period (int): Period for the SMA calculation (default: 200)
        """
        super(SMAStrategy, self).__init__(feed)

        self.__instrument = instrument
        self.__position = None
        self.__sma_period = sma_period

        # We'll use adjusted close values instead of close
        self.__prices = feed[instrument].getPriceDataSeries()
        self.__close = feed[instrument].getCloseDataSeries()

        # Calculate the Simple Moving Average
        self.__sma = ma.SMA(self.__close, sma_period)

        # Track entry and exit points for analysis
        self.__entry_price = None
        self.__entry_date = None

    def getSMA(self):
        """Return the SMA indicator."""
        return self.__sma

    def onEnterOk(self, position):
        """Called when the order to enter a position was filled."""
        exec_info = position.getEntryOrder().getExecutionInfo()
        self.__entry_price = exec_info.getPrice()
        self.__entry_date = exec_info.getDateTime()

        self.info(f"BUY executed at ${exec_info.getPrice():.2f}")

    def onEnterCanceled(self, position):
        """Called when the order to enter a position was canceled."""
        self.__position = None
        self.info("Entry order canceled")

    def onExitOk(self, position):
        """Called when the order to exit a position was filled."""
        exec_info = position.getExitOrder().getExecutionInfo()
        exit_price = exec_info.getPrice()

        # Calculate profit/loss
        if self.__entry_price:
            profit_loss = exit_price - self.__entry_price
            pct_return = (profit_loss / self.__entry_price) * 100

            self.info(
                f"SELL executed at ${exit_price:.2f} | "
                f"P/L: ${profit_loss:.2f} ({pct_return:+.2f}%)"
            )

        self.__position = None
        self.__entry_price = None

    def onExitCanceled(self, position):
        """Called when the order to exit a position was canceled."""
        # If the exit was canceled, re-submit the exit order
        self.__position.exitMarket()
        self.info("Exit order canceled - resubmitting")

    def onBars(self, bars):
        """
        Called for each bar in the data feed.
        This is where the strategy logic is implemented.
        """
        # Wait for enough bars to calculate the SMA
        if self.__sma[-1] is None:
            return

        bar = bars[self.__instrument]
        current_price = bar.getClose()
        sma_value = self.__sma[-1]

        # If we don't have a position, check for entry signal
        if self.__position is None:
            # Buy signal: price crosses above SMA
            if current_price > sma_value and cross.cross_above(self.__close, self.__sma) > 0:
                # Calculate shares to buy (use all available cash)
                cash = self.getBroker().getCash()
                shares = int(cash / current_price)

                if shares > 0:
                    self.info(
                        f"BUY SIGNAL - Price: ${current_price:.2f} > SMA: ${sma_value:.2f} | "
                        f"Buying {shares} shares"
                    )
                    self.__position = self.enterLong(self.__instrument, shares, True)

        # If we have a position, check for exit signal
        elif not self.__position.exitActive():
            # Sell signal: price crosses below SMA
            if current_price < sma_value and cross.cross_below(self.__close, self.__sma) > 0:
                self.info(
                    f"SELL SIGNAL - Price: ${current_price:.2f} < SMA: ${sma_value:.2f}"
                )
                self.__position.exitMarket()


class SMACrossoverStrategy(strategy.BacktestingStrategy):
    """
    Dual SMA Crossover Strategy

    This strategy uses two moving averages:
    - Short-term SMA (fast)
    - Long-term SMA (slow)

    Buy when fast SMA crosses above slow SMA
    Sell when fast SMA crosses below slow SMA
    """

    def __init__(self, feed, instrument, short_period=50, long_period=200):
        """
        Initialize the dual SMA crossover strategy.

        Args:
            feed: Data feed with historical prices
            instrument (str): Ticker symbol to trade
            short_period (int): Period for the fast SMA (default: 50)
            long_period (int): Period for the slow SMA (default: 200)
        """
        super(SMACrossoverStrategy, self).__init__(feed)

        self.__instrument = instrument
        self.__position = None

        # Calculate both SMAs
        self.__close = feed[instrument].getCloseDataSeries()
        self.__short_sma = ma.SMA(self.__close, short_period)
        self.__long_sma = ma.SMA(self.__close, long_period)

        self.__entry_price = None

    def getShortSMA(self):
        """Return the short-term SMA indicator."""
        return self.__short_sma

    def getLongSMA(self):
        """Return the long-term SMA indicator."""
        return self.__long_sma

    def onEnterOk(self, position):
        """Called when the order to enter a position was filled."""
        exec_info = position.getEntryOrder().getExecutionInfo()
        self.__entry_price = exec_info.getPrice()
        self.info(f"BUY executed at ${exec_info.getPrice():.2f}")

    def onEnterCanceled(self, position):
        """Called when the order to enter a position was canceled."""
        self.__position = None

    def onExitOk(self, position):
        """Called when the order to exit a position was filled."""
        exec_info = position.getExitOrder().getExecutionInfo()
        exit_price = exec_info.getPrice()

        if self.__entry_price:
            profit_loss = exit_price - self.__entry_price
            pct_return = (profit_loss / self.__entry_price) * 100

            self.info(
                f"SELL executed at ${exit_price:.2f} | "
                f"P/L: ${profit_loss:.2f} ({pct_return:+.2f}%)"
            )

        self.__position = None
        self.__entry_price = None

    def onExitCanceled(self, position):
        """Called when the order to exit a position was canceled."""
        self.__position.exitMarket()

    def onBars(self, bars):
        """Process each bar and execute strategy logic."""
        # Wait for both SMAs to be ready
        if self.__short_sma[-1] is None or self.__long_sma[-1] is None:
            return

        bar = bars[self.__instrument]
        current_price = bar.getClose()
        short_sma_value = self.__short_sma[-1]
        long_sma_value = self.__long_sma[-1]

        # Check for entry signal
        if self.__position is None:
            # Buy when short SMA crosses above long SMA
            if cross.cross_above(self.__short_sma, self.__long_sma) > 0:
                cash = self.getBroker().getCash()
                shares = int(cash / current_price)

                if shares > 0:
                    self.info(
                        f"BUY SIGNAL - Short SMA: ${short_sma_value:.2f} "
                        f"crossed above Long SMA: ${long_sma_value:.2f}"
                    )
                    self.__position = self.enterLong(self.__instrument, shares, True)

        # Check for exit signal
        elif not self.__position.exitActive():
            # Sell when short SMA crosses below long SMA
            if cross.cross_below(self.__short_sma, self.__long_sma) > 0:
                self.info(
                    f"SELL SIGNAL - Short SMA: ${short_sma_value:.2f} "
                    f"crossed below Long SMA: ${long_sma_value:.2f}"
                )
                self.__position.exitMarket()
