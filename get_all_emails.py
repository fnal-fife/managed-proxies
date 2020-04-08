from argparse import ArgumentParser
import yaml

INFILE = 'managedProxies.yml'

parser = ArgumentParser()

parser.add_argument('-i', '--infile', type=str, default=INFILE)
parser.add_argument('-d', '--delimiter', type=str, default=', ')
args = parser.parse_args()

with open(args.infile, 'r') as f:
    data = yaml.load(f.read(), Loader=yaml.BaseLoader)

s = set()

email_lists = (expt_config['emails']
               for expt_config in data['experiments'].values())

for email_list in email_lists:
    for email in email_list:
        s.add(email)

print(args.delimiter.join(s))
