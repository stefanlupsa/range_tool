#!/usr/bin/python3
import argparse
import configparser
import mysql.connector
import re
import time
import sys

config = configparser.ConfigParser()
config.read('vlan_range.conf')

db_name = config['db']['database']
db_user = config['db']['user']
db_pass = config['db']['password']
db_host = config['db']['host']

range_start = int(config['range']['start'])
range_end = int(config['range']['end'])
range_delta = int(config['range']['delta'])

conn = mysql.connector.connect(host=db_host, user=db_user,
                               password=db_pass)
c = conn.cursor()
c.execute('use %s' % (db_name))
c.execute('''
    CREATE TABLE IF NOT EXISTS intervals (
        start INTEGER PRIMARY KEY,
        end INTEGER NOT NULL,
        reserved BOOLEAN DEFAULT FALSE,
        updated_at DATETIME,
        instance_uuid CHAR(36)
    )
''')
conn.commit()

parser = argparse.ArgumentParser(
    description="Call without arguments to reserve an interval.",
    epilog="Interval format: \d+:\d+."
)


group = parser.add_mutually_exclusive_group()
group.add_argument("-r", "--reset", help="reset intervals list and creates "
                   "fresh entries in database", action="store_true")
group.add_argument("-f", "--free", help="free interval, requires interval",
                   action="store_true")
group.add_argument("-u", "--update-uuid", help="update interval with instance "
                   "uuid, requires interval",
                   dest="instance_uuid")
parser.add_argument("-i" "--interval", help="interval",
                    dest="interval")
parser.add_argument("-c", "--count", help="number of used intervals",
                    action="store_true")
parser.add_argument("-l", "--list", help="list used intervals",
                    action="store_true")
parser.add_argument("-s", "--show", help="show all intervals",
                    action="store_true")


def close(message, exit_code=0):
    conn.close()
    file = None
    if exit_code:
        file = sys.stderr

    print(message, file=file)
    exit(exit_code)


class Interval:
    @staticmethod
    def parse_interval_string(string):
        result = re.match(r"^([+-]?\d+):([+-]?\d+)$", str(string))
        if result is None:
            return None
        return int(result.group(1)), int(result.group(2))

    @staticmethod
    def is_valid_interval(interval):
        if (interval[0] < range_start or interval[1] > range_end or
                (interval[0] + range_delta-1) != interval[1]):
            return False
        return True

    @staticmethod
    def format_row(row):
        fmt = "%d:%d reserved: %r updated: %s instance: %s" % (
            int(row[0]), int(row[1]), bool(row[2]), row[3], row[4])
        return fmt


args = parser.parse_args()
if (args.free or args.instance_uuid) and not args.interval:
    parser.error("free and update requre -i--interval argument")

if args.reset:
    c.execute("delete from intervals")
    for i in range(range_start, range_end, range_delta):
        c.execute("insert into intervals values "
                  "(%d,%d,%s,FROM_UNIXTIME(%d),%s)"
                  % (i, i+range_delta-1, "FALSE", int(time.time()), "NULL"))
    conn.commit()

if args.free:
    interval = Interval.parse_interval_string(args.interval)
    if not interval or not Interval.is_valid_interval(interval):
        close("incorrect interval")
    c.execute("update intervals set "
              "reserved=FALSE, instance_uuid=NULL, "
              "updated_at=FROM_UNIXTIME(%d) where start=%d"
              % (int(time.time()), int(interval[0])))
    conn.commit()

if args.count:
    c.execute("select count(*) from intervals where reserved=TRUE")
    print(c.fetchone()[0])

if args.list:
    c.execute("select * from intervals where reserved=TRUE")
    rows = c.fetchall()
    for row in rows:
        print(Interval.format_row(row))

if args.show:
    c.execute("select * from intervals")
    rows = c.fetchall()
    for row in rows:
        print(Interval.format_row(row))

if args.instance_uuid:
    interval = Interval.parse_interval_string(args.interval)
    if not interval or not Interval.is_valid_interval(interval):
        close("incorrect interval")
    c.execute("update intervals set updated_at=FROM_UNIXTIME(%d), "
              "instance_uuid='%s' where start=%d"
              % (int(time.time()), args.instance_uuid, int(interval[0])))
    conn.commit()

# reserve interval
if len(sys.argv) == 1:
    c.execute("select * from intervals where reserved=FALSE")
    sql_res = c.fetchone()
    if sql_res is None:
        close("all intervals are currently in use", 1)
    else:
        c.fetchall()
        index = sql_res[0]
        index_end = sql_res[1]
        c.execute("update intervals set reserved=TRUE, "
                  "updated_at=FROM_UNIXTIME(%d) where start=%d"
                  % (int(time.time()), index))
        conn.commit()
        print("%d:%d" % (index, index_end))
