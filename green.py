import datetime
import os
import random

# from heavy import special_commit


def modify():
    with open('zero.md', 'w+') as f:
        f.write(str(random.random()))

def commit():
    # os.system('git add zero.md')
    os.system('git commit -a -m test_github_streak > /dev/null 2>&1')


def set_sys_time(year, month, day):
    os.system('date {m}{d}1627{y}'.format(y=year, m=month, d=day))


def trick_commit(year, month, day):
    set_sys_time(year, month, day)
    for j in range(random.randint(0, 5)):
        modify()
        commit()


def daily_commit(start_date, end_date):
    for i in range((end_date - start_date).days + 1):
        cur_date = start_date + datetime.timedelta(days=i)
        trick_commit(cur_date.year, '%02d' % cur_date.month, '%02d' % cur_date.day)


if __name__ == '__main__':
    daily_commit(datetime.date(2018, 8, 8), datetime.date(2018, 12, 24))
