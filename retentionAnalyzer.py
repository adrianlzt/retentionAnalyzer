#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2017 Adrián López Tejedor <adrianlzt@gmail.com>
#
# Distributed under terms of the GNU GPLv3 license.
#
# Example:
# retentionAnalyzer.py -vv -e someEnv -i 127.0.0.1 -u USER -p PASSWORD -d mydatabase -r retention.dat
#

"""
Parsea un fichero retention.dat y envia los datos recogidos a InfluxDB
"""

from influxdb import InfluxDBClient
import pynag.Parsers
import time
import argparse
import sys
import logging

FORMAT = "[%(levelname)s %(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(level=logging.ERROR, format=FORMAT)
logger = logging.getLogger(__name__)


class RetentionDatAnalyzer(object):

    def __init__(self, args):
        logger.debug("Inicializando cliente influx (host=%s, user=%s, pass=%s, database=%s, env=%s)",
                args.influx_host, args.influx_user, args.influx_pass, args.influx_db, args.influx_env)
        self.influx = InfluxDBClient(host=args.influx_host,username=args.influx_user,password=args.influx_pass,database=args.influx_db)

        logger.debug("Inicializando parser de icinga con el fichero: %s", args.retention_file)
        self.retention = pynag.Parsers.retention(args.retention_file)
        self.retention.parse()

        self.influx_env = args.influx_env
        self.noop = args.noop

    def run(self):
        logger.info("Parseando metricas de host")
        self.parse_and_send_host_metrics()
        logger.info("Parseando metricas de service")
        self.parse_and_send_srv_metrics()

    def send_points(self, data):
        logger.debug("Enviando %s puntos a influx", len(data))
        if self.noop:
            return
        self.influx.write_points(data, time_precision='s', tags = {"env": self.influx_env})

    def parse_and_send_host_metrics(self):
        #
        # HOST
        # 'current_down_notification_number': '0',
        # 'last_state_change': '1486238733',
        # 'last_check': '1486240369',
        # 'current_notification_number': '0',
        # 'next_check': '1486241109',
        # 'check_execution_time': '0.000',
        # 'check_latency': '0.941',
        # 'last_time_up': '1486240809',
        # 'current_state': '0',
        # 'last_time_down': '1486238515',
        # 'host_name': 'SomeFrontend',
        # 'percent_state_change': '0.00',
        # 'is_flapping': '0',
        # 'state_history': '0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0'

        for m in ["last_state_change","last_time_up","last_time_down","last_check","next_check"]:
            data = []
            for host in self.retention.data.get("host"):
                if host.get(m) == "0":
                    continue

                d = { "measurement": "host_"+m,
                        "tags": {
                            "host": host.get("host_name"),
                            "current_state": host.get("current_state"),
                            "is_flapping": host.get("is_flapping"),
                            "state_history": host.get("state_history")
                        },
                        "time": int(host.get(m)),
                        "fields": {
                            "current_down_notification_number": int(host.get("current_down_notification_number")),
                            "current_notification_number": int(host.get("current_notification_number")),
                            "check_execution_time": float(host.get("check_execution_time")),
                            "check_latency": float(host.get("check_latency")),
                            "percent_state_change": float(host.get("percent_state_change"))
                        }
                    }

                data.append(d)

            logger.info("Enviando puntos de host, metrica %s", m)
            self.send_points(data)


    def parse_and_send_srv_metrics(self):
        # SERVICE
        # host_name=SomeBackend
        # service_description=calltrace
        # check_execution_time=0.000
        # check_latency=0.042
        # current_state=0
        # last_state_change=1486240010
        # last_time_ok=1486240370
        # last_time_warning=0
        # last_time_unknown=0
        # last_time_critical=1486239991
        # last_check=1486240370
        # next_check=1486240809
        # current_notification_number=0
        # current_warning_notification_number=0
        # current_critical_notification_number=0
        # current_unknown_notification_number=0
        # is_flapping=0
        # percent_state_change=11.05
        # state_history=0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0

        for m in ["last_state_change","last_time_ok","last_time_warning","last_time_unknown","last_time_critical","last_check","next_check"]:
            data = []
            for srv in self.retention.data.get("service"):
                if srv.get(m) == "0":
                    continue

                d = { "measurement": "srv_"+m,
                        "tags": {
                            "host": srv.get("host_name"),
                            "service_description": srv.get("service_description"),
                            "current_state": srv.get("current_state"),
                            "is_flapping": srv.get("is_flapping"),
                            "state_history": srv.get("state_history")
                        },
                        "time": int(srv.get(m)),
                        "fields": {
                            "current_warning_notification_number": int(srv.get("current_warning_notification_number")),
                            "current_critical_notification_number": int(srv.get("current_critical_notification_number")),
                            "current_unknown_notification_number": int(srv.get("current_unknown_notification_number")),
                            "current_notification_number": int(srv.get("current_notification_number")),
                            "check_execution_time": float(srv.get("check_execution_time")),
                            "check_latency": float(srv.get("check_latency")),
                            "percent_state_change": float(srv.get("percent_state_change"))
                        }
                    }

                data.append(d)

            logger.info("Enviando puntos de service, metrica %s", m)
            self.send_points(data)


def parse_args(argv):
    p = argparse.ArgumentParser(description='Parse retention.dat file and send to Influx.')
    p.add_argument('-v', '--verbose', dest='verbose', action='count', default=0,
                   help='verbose output. specify twice for debug-level output.')
    p.add_argument('-n', '--noop', dest='noop', action='store_true',
                   default=False, help="No enviar las metricas a influx")
    p.add_argument("-i", "--host", action="store", dest="influx_host",
                   help="Host de influxdb", default="127.0.0.1")
    p.add_argument("-u", "--user", action="store", dest="influx_user",
                   help="User de influxdb", default=None)
    p.add_argument("-p", "--pass", action="store", dest="influx_pass",
                   help="Password de influxdb", default=None)
    p.add_argument("-d", "--database", action="store", dest="influx_db",
                   help="Database de influxdb", default="icinga")
    p.add_argument("-e", "--env", action="store", dest="influx_env",
                   help="Database de influxdb", default="test")
    p.add_argument("-r", "--retention", action="store", dest="retention_file",
                   help="Fichero retention.dat", default="retention.dat")

    args = p.parse_args(argv)

    return args

if __name__ == "__main__":
    args = parse_args(sys.argv[1:])
    if args.verbose > 1:
        logger.setLevel(logging.DEBUG)
    elif args.verbose > 0:
        logger.setLevel(logging.INFO)
    rda = RetentionDatAnalyzer(args)
    rda.run()

