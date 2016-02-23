'''
Created on 23/02/2016

@author: jorge
'''


import unittest 
from freundeberg.utils.DateHelper import DateHelper

class TestDateHelper(unittest.TestCase):
    
    def test_is_tomorrow_free(self):
        a = DateHelper(2016,2,23)
        self.assertEqual(a.is_tomorrow_off(),False, "Testing a tuesday")
    
    def test_was_yesterday_free(self):
        a = DateHelper(2016,2,23)
        self.assertEqual(a.was_yesterday_off(),False, "Testing a tuesday 2016,02,23")
        
    def test_is_tomorrow_free_true(self):
        a = DateHelper(2016,2,26)
        self.assertEqual(a.is_tomorrow_off(),True, "Testing a Friday ")
    
    def test_was_yesterday_free_true(self):
        a = DateHelper(2016,2,28)
        self.assertEqual(a.was_yesterday_off(),True, "Testing a Sunday")

    def test_is_tomorrow_free_true2(self):
        a = DateHelper(2016,2,21)
        self.assertEqual(a.is_tomorrow_off(),False, "Testing a Monday ")
    
    def test_was_yesterday_free_true2(self):
        a = DateHelper(2016,2,21)
        self.assertEqual(a.was_yesterday_off(),True, "Testing a Monday")