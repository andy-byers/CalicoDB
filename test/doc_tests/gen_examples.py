#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re

# Regular expression for extracting the language type and text from a single code block in
# a .md file. This seems to be the format that code must be in to display properly.
CODE_BLOCK_RE = re.compile(r'```(\S+)\n([\s\S]*?)\n```')


def main():
    parser = argparse.ArgumentParser(description='Extract C++ example code from a markdown file.')
    parser.add_argument('input', type=str, help='Name of a markdown file to process')
    parser.add_argument('output', type=str, help='Name of the generated source file')
    args = parser.parse_args()

    with open(args.input, 'r') as f:
        code = CODE_BLOCK_RE.findall(f.read())
    # Paste each C++ block from doc.md into a main() function in order.
    cpp = '\n'.join(y for x, y in code if x.lower() == 'c++')
    cpp = f'''
#include <cstdio>
#include "calicodb/db.h"
#include "calicodb/cursor.h"
int main() {{{cpp} return 0;}}
'''
    with open(args.output, 'w') as f:
        f.write(cpp)


if __name__ == '__main__':
    main()
