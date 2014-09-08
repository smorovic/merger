#!/usr/bin/env python

import distutils.core
import distutils.util

platform = distutils.util.get_platform()


distutils.core.setup(
    name='python-zlib-extras',
    version='0.1',
    description='Additional interfaces to zlib library',
    author="Srecko morovic",
    author_email='srecko.morovic@cern.ch',
    license='LGPL',
    platforms='Linux',
    url='http://www.github.com/cmsdaq/hltd',
    ext_modules=[distutils.core.Extension('zlibextras', sources=['zlibextras.c'],libraries=['z'])],
    )
