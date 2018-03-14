#!/usr/bin/python
"""Run ./store_certkeys_in_myproxy for all accounts configured in 
proxy_push.yml except for those under the p-* entries"""

import subprocess

CONFIG = 'proxy_push.yml'

def run_store_certkeys(account):
    """Run ./store_certkeys_in_myproxy <account>.  Return True if success, 
    False if not"""
    print account
    try:
        cmd_list = ["./store_certkeys_in_myproxy", account]
        subprocess.check_call(["./store_certkeys_in_myproxy", account])
    except subprocess.CalledProcessError as e:
        print '{0} failed with error {1}'.format(" ".join(cmd_list), e)
        return False
    return True


def main():
    import yaml

    with open(CONFIG, 'r') as f:
        config = yaml.load(f.read())

    """
    Example:
    experiments:
        minerva:
            dir: /opt
            emails: [perdue@fnal.gov, jyhan@fnal.gov]
            nodes: [minervagpvm01, minervagpvm02, minervagpvm03, minervagpvm04, 
            minervagpvm05, minervagpvm06, minervagpvm07]
            accounts:
                minervacal: Calibration
                minervapro: Production
                minervadat: Data
    """
    
    expt_dicts = (value
                  for key, value in config['experiments'].iteritems()
                  if "p-" not in key
                 )
    accounts_dict = (d['accounts'] for d in expt_dicts)
    accounts = (acct_name
                for d in accounts_dict
                for acct_name in d.iterkeys()
               )


    results = {}
    for account in accounts:
        results[account] = run_store_certkeys(account)
    
    for account, success in results.iteritems():
        if not success:
            print ("Storing of cert and key in myproxy for {0} failed.  Try to "
                   "run \"./store_certkeys_in_myproxy {0}\" and inspect any error "
                   "messages.").format(account)        


if __name__ == '__main__':
    main()
    exit(0)
