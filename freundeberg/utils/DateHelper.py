from datetime import date, timedelta

class DateHelper():        # define parent class
    def __init__(self, year,month,day):
        self.year = int(year)
        self.month = int(month)
        self.day = int(day)

    def __is_tomorrow_holiday(self):
        #needs to be implemented
        return False

    def week_of_year(self):
        return date(self.year,self.month, self.day).isocalendar()[1]
  
    def is_tomorrow_off(self):
        flag = False
        if self.__is_tomorrow_holiday():
            print "holiday_print"
            flag = True
        #add one day
        week_day2 = date(self.year,self.month, self.day) + timedelta(days=1)
        tomorrow = week_day2.weekday()
        #0 monday , 6 Sunday
        if tomorrow == 5 or tomorrow == 6:
            flag = True
        
        return flag

    def __was_yesterday_holiday(self):
        #need to be implemented
        return False    
    
    def was_yesterday_off(self):
        if self.__was_yesterday_holiday():
            return False
        week_day2 = date(self.year,self.month, self.day) - timedelta(days=1)
        yesterday = week_day2.weekday()
        #0 monday , 6 Sunday
        if yesterday == 5 or yesterday == 6:
            return True
        return False
    
    def last_x_days(self,x):
        days = date(self.year,self.month, self.day)
        last_days = []
        for i in xrange(1,x+1):
            daysdiff = days - timedelta(days=i)
            last_days.append(daysdiff.isoformat())
        return last_days