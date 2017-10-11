#!/usr/bin/env python

from os import remove
import argparse
from sys import exit

from proxy_push import ManagedProxyPush, loadjson

configfile = 'input_file.json'

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("experiment")
    parser.add_argument("-c", "--config", type=str,
            help="Alternate config file", default=configfile)
    args = parser.parse_args()

    myjson = loadjson(args.config)

    if args.experiment in myjson:
        p = ManagedProxyPush(myjson=myjson)
        try:
            assert p.process_experiment(args.experiment)
        except AssertionError:
            print "Proxy push for {0} failed.  Please check the logfile "\
            "log_{0}.".format(args.experiment)
            exit(1)
        else:
            try:
                remove('log_{0}'.format(args.experiment))
            except OSError:
                print "File log_{0} was never generated".format(args.experiment)
            else:
                print "Proxy successfully pushed for {0}.".format(args.experiment)
    else:
        print "Experiment {0} is not configured in the config file {1}".format(args.experiment, args.config)
        exit(1)


if __name__ == '__main__':
    main()
    exit(0)
