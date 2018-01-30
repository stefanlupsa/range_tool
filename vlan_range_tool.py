#!/usr/bin/python3
import argparse
import sqlite3
import re
import time
import sys

start = 50
end = 4000
delta = 50

conn = sqlite3.connect("data.db")
c = conn.cursor()
c.execute('''
    CREATE TABLE IF NOT EXISTS intervals (
        start_key integer PRIMARY KEY,
        date_added integer
    )
''')
conn.commit()

parser = argparse.ArgumentParser(
    description="Call without arguments to reserve an interval.",
    epilog="Interval format: \d+:\d+."
)
group = parser.add_mutually_exclusive_group()
group.add_argument("-r", "--reset", help="reset used intervals list", action="store_true")
group.add_argument("-f", "--free", help="free interval", metavar="interval")
parser.add_argument("-c", "--count", help="number of used intervals", action="store_true")
parser.add_argument("-s", "--show", help="show used intervals", action="store_true")


def close(message, exit_code=0):
    conn.close()
    file = None
    if exit_code:
        file = sys.stderr

    print(message, file=file)
    exit(exit_code)


class Interval:
    @staticmethod
    def get_interval(idx):
        return Interval.get_start(idx), Interval.get_end(idx)

    @staticmethod
    def get_start(idx):
        return start + idx * delta

    @staticmethod
    def get_end(idx):
        return start + (idx + 1) * delta - 1

    @staticmethod
    def format(idx):
        result = Interval.get_interval(idx)
        return "{}:{}".format(result[0], result[1])

    @staticmethod
    def parse_interval_string(string):
        result = re.match(r"^([+-]?\d+):([+-]?\d+)$", str(string))
        if result is None:
            return None
        return int(result.group(1)), int(result.group(2))

    @staticmethod
    def is_valid_interval(interval):
        if interval[0] < start or interval[1] > end:
            return False
        return True

    @staticmethod
    def get_index(interval):
        idx_start = (interval[0] - start) / delta
        idx_end = (interval[1] - start + 1) / delta
        if not(idx_start.is_integer() and idx_start == idx_end - 1):
            return False
        return int(idx_start)


args = parser.parse_args()
if args.reset:
    c.execute("delete from intervals")
    conn.commit()

if args.free:
    interval = Interval.parse_interval_string(args.free)
    if not interval or not Interval.is_valid_interval(interval):
        close("incorrect interval")
    index = Interval.get_index(interval)
    if index is False:
        close("incorrect interval")

    c.execute("delete from intervals where start_key = ?", (index,))
    conn.commit()

if args.count:
    c.execute("select count(*) from intervals")
    print(c.fetchone()[0])

if args.show:
    c.execute("select * from intervals")
    rows = c.fetchall()
    for row in rows:
        print(Interval.format(row[0]))

if not(args.count or args.free or args.reset or args.show):
    sql = '''
        select min(result_key) from (
            select min(a.start_key) - 1 as result_key 
            from intervals a 
            left join intervals b on a.start_key = b.start_key + 1
            where a.start_key > 0 and b.start_key is null
            union
            select max(a.start_key) + 1 as result_key from intervals a
        )
    '''
    c.execute(sql)
    index = c.fetchone()[0]

    if index is None:
        index = 0
    elif Interval.get_end(index) > end:
        close("all intervals are currently in use", 1)

    try:
        c.execute("insert into intervals values (?,?)", [index, time.time()])
        conn.commit()
        print(Interval.format(index))
    except:
        close("concurrency error", 1)

