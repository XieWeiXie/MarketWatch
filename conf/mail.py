#!/usr/bin/env python
# encoding: utf-8

"""
@author: paul.xie
@software: PyCharm
@time: 2017/1/16 13:44
"""
import yagmail

class SendEmail(object):
    """
    send email to paul.xie
    """
    def __init__(self, subject=None, message=None):
        self.From_add = 'ops@chinascope.com'
        self.Password= 'p@ssw0rd'
        self.To_add = "paul.xie@chinascope.com"
        self.Host = "smtp.qiye.163.com"
        self.Port = 0
        self.subject = subject if subject else "抓取任务报告：MarketWatch"
        self.message = message

        pass

    @property
    def sendmail(self):
        yag = yagmail.SMTP(
            user=self.From_add,
            password=self.Password,
            host=self.Host,
            port=self.Port
        )
        yag.send(to=self.To_add, subject=self.subject, contents=self.message)
        pass


if __name__ == "__main__":
    A = SendEmail(subject=None, message="抓取任务详情")
    A.sendmail
