#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from os import path
import re

REFERENCE_RE = re.compile(r'\[[^\[\]]+]\(([./\w_-]+)?(?:#([\w_-]+))?\)')
HEADING_RE = re.compile(r'^##+\s+([\w\s_-]+)\s*$', re.MULTILINE)


def parse_md(filename):
    with open(filename, 'r') as f:
        content = f.read()
    heading_as_text = HEADING_RE.findall(content)
    heading_as_refs = set('-'.join(t.lower().split()) for t in heading_as_text)
    references = REFERENCE_RE.findall(content)
    prefix = path.dirname(filename)
    for i, (eref, iref) in enumerate(references):
        references[i] = path.join(prefix, eref if eref else filename), iref
    return heading_as_refs, references


def check_md(filename, info):
    _, references = info[filename]
    print('CHECK ' + filename)
    for ref_path, ref_tag in references:
        if ref_path not in info or not ref_tag:
            print('  ' + ref_path)
            # Either ref_path isn't another .md file, or it is a .md file that isn't in
            # the set of files being checked. Either way, just make sure it exists.
            assert path.exists(ref_path)
        else:
            print(f'  {ref_path}#{ref_tag}')
            # Make sure the reference points at an existing heading.
            assert ref_tag in info[ref_path][0]


def main():
    parser = argparse.ArgumentParser(description='Check markdown file references.')
    parser.add_argument('input', type=str, nargs='+', help='Names of markdown files to check')
    args = parser.parse_args()
    info = {fname: parse_md(fname) for fname in args.input}
    for filename in info:
        check_md(filename, info)


if __name__ == '__main__':
    main()
