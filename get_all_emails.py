from argparse import ArgumentParser
import yaml

INFILE = 'managedProxies.yml'

parser = ArgumentParser()

parser.add_argument('-i', '--infile', type=str, default=INFILE)
parser.add_argument('-d', '--delimiter', type=str, default=', ')
parser.add_argument('-e', '--experiment', nargs="*")
args = parser.parse_args()


def filter_experiments(expt):
    if args.experiment is None:
        return True
    else:
        return expt in args.experiment

with open(args.infile, 'r') as f:
    data = yaml.load(f.read(), Loader=yaml.BaseLoader)

s = set()

email_lists = (expt_config['emails']
               for expt_name, expt_config in data['experiments'].items()
               if filter_experiments(expt_name))


for email_list in email_lists:
    for email in email_list:
        s.add(email)

print(args.delimiter.join(s))
