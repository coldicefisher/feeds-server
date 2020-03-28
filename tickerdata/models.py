from django.db import models
import datetime
# Create your models here.


class Tick(models.Model):
    symbol = models.CharField(max_length=10)
    ticker_datetime = models.DateTimeField()
    volume = models.FloatField()
    high = models.FloatField()
    low = models.FloatField()
    open = models.FloatField()
    close = models.FloatField()
    frequency = models.BigIntegerField()
    source = models.CharField(max_length=30)
    #yahoo fields
    dividends = models.FloatField(null=True,blank=True)
    stock_splits = models.FloatField(null=True,blank=True)
    adjusted_close = models.FloatField(null=True,blank=True)
    average = models.FloatField(null=True,blank=True)
    #iex fields stocks
    notional = models.FloatField(null=True,blank=True)
    number_of_trades = models.FloatField(null=True,blank=True)
    change_over_time = models.FloatField(null=True,blank=True)
    #iex fields - market
    market_open = models.FloatField(null=True,blank=True)
    market_high = models.FloatField(null=True,blank=True)
    market_low = models.FloatField(null=True,blank=True)
    market_close = models.FloatField(null=True,blank=True)
    market_volume = models.FloatField(null=True,blank=True)
    market_average = models.FloatField(null=True,blank=True)
    market_notional = models.FloatField(null=True,blank=True)
    market_number_of_trades = models.FloatField(null=True,blank=True)
    market_change_over_time = models.FloatField(null=True,blank=True)
    
    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['symbol','ticker_datetime'], name='one_bar_per_symbol')
        ]
    def __str__(self):
        return 'Symbol: ' + self.symbol + ' Tick:  ' + self.ticker_datetime.strftime('%m-%d-%Y %H:%M:%S.%f') # pylint: disable=no-member
    

class Symbol(models.Model):
    symbol = models.CharField(max_length=10,unique=True)
    last_pull = models.DateTimeField(blank=True,null=True)
    alphavantage_status = models.IntegerField(default=0,null=True)
    yahoo_status = models.IntegerField(default=0,null=True)
    sp500_member = models.BooleanField(default=False,null=True)
    spdr_member = models.BooleanField(default=False,null=True)
    iex_status = models.IntegerField(default=0,null=True)

