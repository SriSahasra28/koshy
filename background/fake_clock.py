# Time management file for simulation during paper trading on historical data
import datetime
class Clock:
    def __init__(self, current_date):
        #print('Fake Clock Initialized')
        print(current_date, type(current_date))
        self.current_datetime = datetime.datetime(current_date.year, current_date.month, current_date.day,9,25,1)
        #self.current_datetime = datetime.datetime(current_date.year, current_date.month, current_date.day,14,28,40)
    
    def addOneSecond(self):
        self.current_datetime = self.current_datetime + datetime.timedelta(seconds=1)
        new_time = datetime.time(self.current_datetime.hour, self.current_datetime.minute, self.current_datetime.second)
        return new_time

    def addSeconds(self, secs):
        self.current_datetime = self.current_datetime + datetime.timedelta(seconds=secs)
        new_time = datetime.time(self.current_datetime.hour, self.current_datetime.minute, self.current_datetime.second)
        return new_time
    
    def addTenSecond(self):
        self.current_datetime = self.current_datetime + datetime.timedelta(seconds=10)
        new_time = datetime.time(self.current_datetime.hour, self.current_datetime.minute, self.current_datetime.second)
        return new_time
    
    def addOneMinute(self):
        self.current_datetime = self.current_datetime + datetime.timedelta(minutes=1)
        new_time = datetime.time(self.current_datetime.hour, self.current_datetime.minute, self.current_datetime.second)
        #print('new_time: ' + str(new_time))
        return new_time
    
    def addFiveMinute(self):
        self.current_datetime = self.current_datetime + datetime.timedelta(minutes=5)
        if self.current_datetime.hour == 23 and self.current_datetime.minute > 30:
            self.addOneDay()
        new_time = datetime.time(self.current_datetime.hour, self.current_datetime.minute, self.current_datetime.second)
        #print(f"{new_time=}")
        return new_time
    
    def addFifteenMinute(self):
        self.current_datetime = self.current_datetime + datetime.timedelta(minutes=15)
        if self.current_datetime.hour == 23 and self.current_datetime.minute > 30:
            self.addOneDay()
        new_time = datetime.time(self.current_datetime.hour, self.current_datetime.minute, self.current_datetime.second)
        return new_time
    
    def addOneDay(self):
        self.current_datetime = self.current_datetime + datetime.timedelta(days=1)
        self.current_datetime = datetime.datetime(self.current_datetime.year, self.current_datetime.month, self.current_datetime.day,9,0,0)
    
    def getCurrentDateTime(self):
        return self.current_datetime
    
    def getCurrentDate(self):
        cur_date = datetime.date(self.current_datetime.year, self.current_datetime.month, self.current_datetime.day)
        return cur_date
    
    def getCurrentTime(self):
        cur_time = datetime.time(self.current_datetime.hour, self.current_datetime.minute, self.current_datetime.second)
        return cur_time
    
    def setCurrentDateTime(self, current_date):
        self.current_datetime = datetime.datetime(current_date.year, current_date.month, current_date.day,9,0,0)
    
    def setCurrentDateTime2(self, current_date, hour, mins):
        self.current_datetime = datetime.datetime(current_date.year, current_date.month, current_date.day, hour, mins,0)
        
        
        