
import glob
import pandas as pd
import os
import argparse

pwd = os.getcwd()

parser = argparse.ArgumentParser(description="This script merge several two columns files into one.")
requiredNamed = parser.add_argument_group('required named arguments')
requiredNamed.add_argument("-i", "--input", type=str, help="The same suffix of input files (e.g., txt).", required=True)
requiredNamed.add_argument("-o", "--output", type=str, help="The concatenated file.", required=True)
args = parser.parse_args()

file_list = []
# my_list = glob.glob('*gene2anno2depth.summary.txt')
my_list = glob.glob('*'+"{0}".format(args.input))

for name in my_list:
    new_name = name.split(".{0}".format(args.input))[0]
    file = pd.read_table(name, sep='\t', header=None, names=['Gene', new_name], usecols=[0, 1], index_col=0)
    file_list.append(file)

output = file_list[0]
for file in file_list[1:]:
    output = pd.concat([output, file], axis=1, sort=True, names=['Gene'])
output.to_csv("{0}".format(args.output), sep='\t')
