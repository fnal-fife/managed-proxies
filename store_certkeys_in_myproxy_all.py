#!/usr/bin/python
"""Run ./store_certkeys_in_myproxy for all accounts configured in 
proxy_push.yml except for those under the p-* entries"""

import subprocess

CONFIG = 'proxy_push.yml'
STORE_KEYS_SCRIPT = './store_certkeys_in_myproxy.sh'

def run_store_certkeys(account):
    """Run ./store_certkeys_in_myproxy <account>.  Return True if success, 
    False if not"""
    print account
    try:
        cmd_list = [STORE_KEYS_SCRIPT, account]
        subprocess.check_call(cmd_list)
    except subprocess.CalledProcessError as e:
        print '{0} failed with error {1}'.format(" ".join(cmd_list), e)
        return False
    return True


def main():
    import yaml
    from datetime import datetime

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
                  if "p-" not in key      # exclude p-larreco, p-larsoft, etc.
                 )
    accounts_dict = (d['accounts'] for d in expt_dicts)
    accounts = (acct_name
                for d in accounts_dict
                for acct_name in d.iterkeys()
               )


    results = {}
    for account in accounts:
        results[account] = run_store_certkeys(account)
    
    all_succeeded = True
    for account, success in results.iteritems():
        if not success:
            print ("Storing of cert and key in myproxy for {0} failed.  Try to "
                   "run \"./store_certkeys_in_myproxy {0}\" and inspect any error "
                   "messages.").format(account)        
            all_succeeded = False

    if all_succeeded:
        print "All certs and keys were stored in myproxy and myproxy-int successfully."
        print datetime.now()
        print "-"*80



if __name__ == '__main__':
    main()
    exit(0)
