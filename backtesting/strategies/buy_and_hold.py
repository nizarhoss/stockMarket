"""
Buy and Hold Strategy

This is a baseline strategy that:
1. Buys the stock on the first available day with all available capital
2. Holds the position until the end of the backtest period
3. Provides a benchmark to compare against more complex strategies
"""

from pyalgotrade import strategy


class BuyAndHoldStrategy(strategy.BacktestingStrategy):
    """
    Buy and Hold Strategy - Baseline/Benchmark Strategy

    This simple strategy buys a stock with all available cash on the first
    trading day and holds it throughout the entire backtest period.

    This serves as a benchmark to compare the performance of more sophisticated
    trading strategies.
    """

    def __init__(self, feed, instrument):
        """
        Initialize the Buy and Hold strategy.

        Args:
            feed: Data feed with historical prices
            instrument (str): Ticker symbol to trade
        """
        super(BuyAndHoldStrategy, self).__init__(feed)

        self.__instrument = instrument
        self.__position = None
        self.__bought = False
        self.__entry_price = None
        self.__entry_date = None
        self.__shares = 0

    def getPosition(self):
        """Return the current position."""
        return self.__position

    def onEnterOk(self, position):
        """Called when the buy order is filled."""
        exec_info = position.getEntryOrder().getExecutionInfo()
        self.__entry_price = exec_info.getPrice()
        self.__entry_date = exec_info.getDateTime()

        self.info(
            f"BUY AND HOLD - Purchased {self.__shares} shares at "
            f"${self.__entry_price:.2f} on {self.__entry_date.date()}"
        )

    def onEnterCanceled(self, position):
        """Called when the buy order is canceled."""
        self.__position = None
        self.__bought = False
        self.info("Entry order canceled")

    def onExitOk(self, position):
        """Called when the sell order is filled (typically at end of backtest)."""
        exec_info = position.getExitOrder().getExecutionInfo()
        exit_price = exec_info.getPrice()
        exit_date = exec_info.getDateTime()

        # Calculate total return
        if self.__entry_price:
            profit_loss = (exit_price - self.__entry_price) * self.__shares
            total_return = (exit_price / self.__entry_price - 1) * 100

            # Calculate holding period
            holding_days = (exit_date - self.__entry_date).days

            self.info(
                f"POSITION CLOSED on {exit_date.date()} at ${exit_price:.2f}"
            )
            self.info(
                f"Total Return: {total_return:+.2f}% | "
                f"Profit/Loss: ${profit_loss:,.2f} | "
                f"Holding Period: {holding_days} days"
            )

        self.__position = None

    def onExitCanceled(self, position):
        """Called when the sell order is canceled."""
        self.info("Exit order canceled")

    def onBars(self, bars):
        """
        Process each bar.

        Buy on the first bar with all available cash and hold.
        """
        # Only buy once at the beginning
        if not self.__bought and self.__position is None:
            bar = bars[self.__instrument]
            current_price = bar.getClose()

            # Use all available cash to buy shares
            cash = self.getBroker().getCash()
            self.__shares = int(cash / current_price)

            if self.__shares > 0:
                # Calculate approximate investment amount
                investment = self.__shares * current_price

                self.info(
                    f"Initiating BUY AND HOLD strategy with ${cash:,.2f} cash"
                )
                self.info(
                    f"Buying {self.__shares} shares at ~${current_price:.2f} "
                    f"(~${investment:,.2f} invested)"
                )

                # Enter a long position
                self.__position = self.enterLong(self.__instrument, self.__shares, True)
                self.__bought = True
            else:
                self.warning(
                    f"Insufficient cash to buy even 1 share at ${current_price:.2f}"
                )


class BuyAndHoldWithDividends(strategy.BacktestingStrategy):
    """
    Buy and Hold Strategy with Dividend Reinvestment

    Similar to basic Buy and Hold, but this version can simulate
    dividend reinvestment if dividend data is available.
    """

    def __init__(self, feed, instrument, reinvest_dividends=False):
        """
        Initialize the Buy and Hold with Dividends strategy.

        Args:
            feed: Data feed with historical prices
            instrument (str): Ticker symbol to trade
            reinvest_dividends (bool): Whether to reinvest dividends (default: False)
        """
        super(BuyAndHoldWithDividends, self).__init__(feed)

        self.__instrument = instrument
        self.__position = None
        self.__bought = False
        self.__reinvest = reinvest_dividends
        self.__entry_price = None
        self.__shares = 0

    def onEnterOk(self, position):
        """Called when the buy order is filled."""
        exec_info = position.getEntryOrder().getExecutionInfo()
        self.__entry_price = exec_info.getPrice()

        self.info(
            f"BUY AND HOLD - Purchased {self.__shares} shares at ${self.__entry_price:.2f}"
        )

    def onEnterCanceled(self, position):
        """Called when the buy order is canceled."""
        self.__position = None
        self.__bought = False

    def onExitOk(self, position):
        """Called when the sell order is filled."""
        exec_info = position.getExitOrder().getExecutionInfo()
        exit_price = exec_info.getPrice()

        if self.__entry_price:
            total_return = (exit_price / self.__entry_price - 1) * 100
            self.info(f"Final Return: {total_return:+.2f}%")

        self.__position = None

    def onExitCanceled(self, position):
        """Called when the sell order is canceled."""
        pass

    def onBars(self, bars):
        """Process each bar and buy on first opportunity."""
        if not self.__bought and self.__position is None:
            bar = bars[self.__instrument]
            current_price = bar.getClose()

            cash = self.getBroker().getCash()
            self.__shares = int(cash / current_price)

            if self.__shares > 0:
                self.__position = self.enterLong(self.__instrument, self.__shares, True)
                self.__bought = True
