#!/usr/bin/env python
import sys
import math
import re
from optparse import OptionParser

floatcheck = re.compile("^[-+]?(?:\b?[0-9]+(?:\.[0-9]*)?|\.[0-9]+\b)(?:[eE][-+]?[0-9]+\b?)?$")

def output_strlist(*args):
    """ takes a list of lists or single items and returns a list of strings """
    outlist = []
    for i in args:
        if type(i) is not list and type(i) is not tuple:
            outlist.append(str(i))
        else:
            for j in i:
                outlist.append(str(j))
    return outlist

def get_slice_fields(rawfields):
    """ takes a list of feild specifiers, e.g. 1:3,7,10
    and returns a list of explicit indicies used to slice a given line of data

    counting starts at 1, negative values can be used to pull from the tail of
    a list, but be warned, something like 1:-1 is not a valid range, nor is 
    something such as 5:1
    """

    splitfields = rawfields.split(",")
    strictfields = []

    for field in splitfields:
        if ":" in field:
            (first, second) = field.split(":")
            try:
                first = int(first)
                second = int(second)
                if first >= second:
                    print >>sys.stderr, "your slice between %s and %s won't produce results" % (first,second)
                    sys.exit(1)
                if first >= 0:
                    first -= 1
                if second < 0:
                    second += 1
                for i in range(first, second):
                    strictfields.append(i)
            except:
                raise
        else:
            try:
                first = int(field)
                if first > 0:
                    strictfields.append(first-1)
                else:
                    strictfields.append(first)
            except:
                raise
    return strictfields

def get_slice(inputline, slices, delim):
    """ takes a list of input and a list of slices as returned by
    get_slice_fields and returns a list sublist of input based on
    the specified slices
    """

    #print >> sys.stderr, "get_slice(%s, %s, %s)" % (input, slices, delim)
    #print >> sys.stderr, "get_slice... input.split(%s) = %s" % (delim, inputline.split(delim))
    output = []
    for slice_ in slices:
        try:
            if opts.debug:
                print >> sys.stderr, "inputline.split(%s) = %s" % (delim, inputline.split(delim))
            output.append(inputline.split(delim)[slice_])
        except IndexError:
            print >> sys.stderr, "unindexable line, inputline = [%s], slices = [%s], delim = [%s], output=[%s]" % (inputline, slices, delim,output)
            raise IndexError

    #print >> sys.stderr, "get_slice... return %s" % (output)
    if len(output) == 1:
        return output[0]
    elif len(output) < 0:
        return None
    else:
        if opts.debug:
            print >> sys.stderr, "output=[%s]" % (output)
        return output

def aggregate(existing, new, opts):
    existing.append(new)
    return

def redux(agg,opts):
    agg = [ x for x in agg if floatcheck.match(x) ]
    if opts.action == "add":
        if opts.debug:
            print >> sys.stderr, "redux(%s, %s), add" % (agg, opts)
        if opts.int:
                if len(agg) == 1:
                    return int(agg[0])
                return reduce(lambda x, y: int(float(x) + float(y)), agg) 
        else:
                if len(agg) == 1:
                    return float(agg[0])
                return reduce(lambda x, y: float(x) + float(y), agg) 
    if opts.action == "mean":
        if len(agg) == 1:
            return float(agg[0])
        #print >> sys.stderr, "mean reduce type = %s, agg = %s" % (type(reduce(lambda x, y: float(x) + float(y), agg)), agg)
        return reduce(lambda x, y: float(x) + float(y), agg) / float(len(agg))
    if opts.action == "concat":
        return agg
    if opts.action == "median":
        agg.sort()
        idx = len(agg)/2.0
        intidx = len(agg) / 2
        if idx == intidx:
            return (float(agg[int(idx)-1]) + float(agg[int(idx)]))/2.0
        else:
            return agg[int(idx)]
    if opts.action == "stats":
        return "stats not implemented yet, sorry"
    if opts.action == "count":
        return len(agg)


def engage(opts, args):
    keyslices = get_slice_fields(opts.key)
    valueslices = get_slice_fields(opts.value)

    if len(valueslices) > 1 and opts.action != "concat":
        print >> sys.stderr,"You cannot do math on multiple fields, sorry..."
        sys.exit(-2)

    # figure out where the data is coming from
    if len(args) == 1:
        infile = sys.stdin
    elif len(args) > 2:
        print >> sys.stderr, "right now we're only processing one file at a time, sorry..."
    else:
        try:
            infile = file(args[1])
        except IOError:
            print >> sys.stderr, "error opening input file %s" % (args[0])
            if opts.debug:
                raise

    # figure out where it's going:
    if opts.out_file is sys.stdout:
        outfile = sys.stdout
    else:
        try:
            outfile = file(opts.outfile,'w')
        except:
            print >> sys.stderr, "error opening output file %s" % (opts.out_file)
            if opts.debug:
                raise
    if opts.header:
        infile.readline()

    if opts.sorted:
        key = None
        value = []
        for line in infile.readlines():
            line = line.strip()
            try:
                newkey = get_slice(line, keyslices, opts.in_delim)
            except IndexError:
                continue
            if opts.actions != "count":
                try:
                    newvalue = get_slice(line, valueslices, opts.in_delim)
                except IndexError:
                    continue
            else:
                newvalue = None

            if newkey == -1 or newvalue == -1:
                print >> sys.stderr, "XX continuing"
                continue

            if key != newkey and key is not None:
                outputvalue = redux(value, opts)
                outputline = opts.out_delim.join(output_strlist(key , outputvalue)) + "\n"
                outfile.write(outputline)
                key = newkey
                value = [newvalue]
            else:
                value.append(newvalue)
                key = newkey
        outputvalue = redux(value,opts)
        outputline = opts.out_delim.join(output_strlist(key, outputvalue)) + "\n"
        outfile.write(outputline)
    else:
        keys = {}
        for line in infile.readlines():
            line = line.strip()
            try:
                newkey = get_slice(line, keyslices, opts.in_delim)
            except IndexError:
                continue
            if type(newkey) is not str:
                newkey = tuple(newkey)
            if opts.action != "count":
                try:
                    newvalue = get_slice(line, valueslices, opts.in_delim)
                    #print >> sys.stderr, "newkey: %s, newvalue: %s" % (newkey, newvalue)
                except IndexError:
                    continue
            else:
                newvalue = "1"
            if newkey:
                keys.setdefault(newkey, [])
            else:
                continue
            aggregate(keys[newkey], newvalue, opts)
        sortedkeys = keys.keys()
        sortedkeys.sort()
        for key in sortedkeys:
            outfile.write(opts.out_delim.join(output_strlist(key, redux(keys[key], opts))) + "\n")
    outfile.close()

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-a", "--action", default="concat", action="store", choices=["concat", "add", "mean", "median", "stats", "count"], help="method used to combine/aggregate the data, options beyond concat assume a single field is being aggregated and that said field can be parse as a digit")
    parser.add_option("-d", "--debug", default=False, action="store_true", help="enable debug output")
    parser.add_option("-F", "--in-delim", default=None, action="store", help="delimiter to split input data on, defaults to whitespace")
    parser.add_option("-f", "--out-delim", default="\t", action="store", help="output delimiter to use, defaults to a TAB")
    parser.add_option("-i", "--int", default="False", action="store_true", help="integer output, only applicable with action of 'add'")
    parser.add_option("-k", "--key", default="1", action="store", help="column(s) to use as the key when combining/aggregating results, valid notation includes ranges (e.g. 1:4, -5:-4), list (e.g. 1,4,5), or combinations thereof")
    parser.add_option("-o", "--out-file", default=sys.stdout, help="file to write the aggregated results to", action="store")
    parser.add_option("-s", "--sorted", default=False, action="store_true", help="indicate that the input records are pre-sorted (faster, uses far less memory")
    parser.add_option("-v", "--value", default="2", action="store", help="column(s) to use as the value when combining/aggregating results, valid notation is also that of --key")
    parser.add_option("-H", "--header", default=False, action="store_true", help="first line of input is a row of headers, skip first row if flag used")

    opts, args = parser.parse_args(sys.argv)
    if opts.debug:
        print >> sys.stderr, "opts=", opts

    engage(opts, args)
