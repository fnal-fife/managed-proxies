#!/usr/bin/python

import os
import shutil

curlog = 'proxy_push.log'
archive = 'proxy_push_archive.log'

if os.path.exists(archive):
    os.unlink(archive)

if os.path.exists(curlog):
    shutil.copyfile(curlog, archive)

    with open(archive, 'r') as f:
        lines = (line for line in f if line[0] == '#')
        with open (curlog, 'w') as g:
            for line in lines:
                g.write(line)





