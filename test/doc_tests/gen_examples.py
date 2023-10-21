#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re

# Regular expression for extracting the language type and text from a single code block in
# a .md file. This seems to be the format that code must be in to display properly.
CODE_BLOCK_RE = re.compile(r'```(\S+)\n([\s\S]*?)\n```')

SOURCE_LAYOUT = '''
#include <iostream>
{}
int main() {{
{} 
    return 0;
}}
'''


def main():
    parser = argparse.ArgumentParser(description='Extract and format C++ example code from a markdown file.')
    parser.add_argument('input', help='Name of a markdown file containing examples to process')
    parser.add_argument('output', help='Name of the generated source file')
    args = parser.parse_args()

    with open(args.input, 'r') as f:
        blocks = CODE_BLOCK_RE.findall(f.read())

    groups = ['', '']
    blocks = [y for x, y in blocks if x.lower() == 'c++']
    for i, block in enumerate(blocks):
        for line in block.split('\n'):
            is_source = not line.startswith('#')
            groups[is_source] += f'{" " * is_source * 4}{line}\n'

    with open(args.output, 'w') as f:
        f.write(SOURCE_LAYOUT.format(*groups))


if __name__ == '__main__':
    main()
