#! /usr/bin/env python

from distutils.core import setup

setup(name           = 'qsome',
    version          = '0.1.0',
    description      = 'Gruesome qsome. Like qless, but with superqueues',
    url              = 'http://github.com/seomoz/qsome-py',
    author           = 'Dan Lecocq',
    author_email     = 'dan@seomoz.org',
    packages         = ['qsome'],
    package_dir      = {'qsome': 'qsome'},
    package_data     = {'qsome': ['qsome-core/qsome.lua']},
    classifiers      = [
        'Programming Language :: Python',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP'
    ],
    requires = ['redis']
)
